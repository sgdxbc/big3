use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use hdrhistogram::{
    Histogram,
    serialization::{Serializer, V2Serializer},
};
use log::{debug, error};
use quinn::{Connection, Endpoint, TransportConfig};
use rand::{Rng, rng, seq::IteratorRandom as _};
use tokio::{
    select,
    sync::mpsc::{
        Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
    },
    task::JoinSet,
    time::{interval, sleep},
};
use tokio_util::sync::CancellationToken;

use crate::{
    cert::client_config,
    client::{Client, ClientContext},
    schema,
    types::{ClientId, NodeIndex, Reply, Request},
    workload::{InvokeId, ShardIndex, Workload, WorkloadContext},
};

use super::{RequestContext, RequestId, ResponseContext};

struct ClientWorkerChannels {
    tx_invoke_response: UnboundedSender<(RequestId, Vec<u8>)>,
    rx_invoke_response: UnboundedReceiver<(RequestId, Vec<u8>)>,
}

impl ClientWorkerChannels {
    fn new() -> Self {
        let (tx_invoke_response, rx_invoke_response) = unbounded_channel();
        Self {
            tx_invoke_response,
            rx_invoke_response,
        }
    }

    fn invoke_contexts(&self, clients: Vec<ClientHandle>) -> Vec<RequestContext<Vec<u8>, Vec<u8>>> {
        clients
            .into_iter()
            .map(|client_handle| {
                RequestContext::new(client_handle.tx_invoke, self.tx_invoke_response.clone())
            })
            .collect()
    }
}

pub struct WorkloadTask {
    channels: ClientWorkerChannels,
    state: Workload<ClientWorkerTaskContext>,
}

impl WorkloadTask {
    fn new(channels: ClientWorkerChannels, state: Workload<ClientWorkerTaskContext>) -> Self {
        Self { channels, state }
    }

    fn load(
        client_worker_channels: ClientWorkerChannels,
        clients: Vec<ClientHandle>,
        scrape_state: Arc<Mutex<ClientScrapeState>>,
        schema: &schema::ClientTask,
        num_concurrent: u32,
    ) -> anyhow::Result<Self> {
        let context = ClientWorkerTaskContext::new(client_worker_channels.invoke_contexts(clients));
        let state = Workload::new(
            context,
            &schema.workload_config.app,
            num_concurrent,
            scrape_state,
        );
        Ok(Self::new(client_worker_channels, state))
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move {
            stop.run_until_cancelled(self.run_inner()).await;
            // self.state.log_metrics();
        })
        .await?;
        Ok(())
    }

    const TICK_INTERVAL: Duration = Duration::from_millis(10);

    async fn run_inner(&mut self) {
        let duration = rng().random_range(Duration::ZERO..Self::TICK_INTERVAL);
        sleep(duration).await;
        self.state.start();
        let mut ticker = interval(Self::TICK_INTERVAL);
        loop {
            select! {
                _ = ticker.tick() => {
                    // self.state.on_tick();
                }
                Some((seq, res)) = self.channels.rx_invoke_response.recv() => {
                    self.state.on_invoke_response(seq, res);
                }
            }
        }
    }
}

struct ClientWorkerTaskContext {
    invokes: Vec<RequestContext<Vec<u8>, Vec<u8>>>,
}

impl ClientWorkerTaskContext {
    fn new(invokes: Vec<RequestContext<Vec<u8>, Vec<u8>>>) -> Self {
        Self { invokes }
    }
}

impl WorkloadContext for ClientWorkerTaskContext {
    fn invoke(&mut self, shard: ShardIndex, command: Vec<u8>) -> InvokeId {
        self.invokes[shard as usize].request(command)
    }
}

struct ClientChannels {
    tx_invoke: UnboundedSender<(Vec<u8>, ResponseContext<Vec<u8>>)>,
    rx_invoke: UnboundedReceiver<(Vec<u8>, ResponseContext<Vec<u8>>)>,

    tx_incoming_message: Sender<Reply>,
    rx_incoming_message: Receiver<Reply>,
}

#[derive(Clone)]
struct ClientHandle {
    tx_invoke: UnboundedSender<(Vec<u8>, ResponseContext<Vec<u8>>)>,
    tx_incoming_message: Sender<Reply>,
}

impl ClientChannels {
    fn new() -> Self {
        let (tx_invoke, rx_invoke) = unbounded_channel();
        let (tx_incoming_message, rx_incoming_message) = channel(100);
        Self {
            tx_invoke,
            rx_invoke,
            tx_incoming_message,
            rx_incoming_message,
        }
    }

    fn handle(&self) -> ClientHandle {
        ClientHandle {
            tx_invoke: self.tx_invoke.clone(),
            tx_incoming_message: self.tx_incoming_message.clone(),
        }
    }
}

impl ClientHandle {
    async fn incoming_message(&self, reply: Reply) -> anyhow::Result<()> {
        self.tx_incoming_message.send(reply).await?;
        Ok(())
    }
}

pub struct ClientTask {
    channels: ClientChannels,
    state: Client<ClientTaskContext>,
}

impl ClientTask {
    fn new(channels: ClientChannels, state: Client<ClientTaskContext>) -> Self {
        Self { channels, state }
    }

    async fn load(
        client_channels: ClientChannels,
        network_connect: NetworkConnectHandle,
        schema: &schema::ClientTask,
        client_id: ClientId,
    ) -> anyhow::Result<Self> {
        let client_context = ClientTaskContext { network_connect };
        let state = Client::new(client_context, schema.config.clone(), client_id);
        Ok(Self::new(client_channels, state))
    }

    async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move {
            stop.run_until_cancelled(self.run_inner()).await;
            self.state.log_metrics();
        })
        .await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        loop {
            select! {
                Some((command, tx_response)) = self.channels.rx_invoke.recv() => {
                    self.state.invoke(command, tx_response);
                }
                Some(reply) = self.channels.rx_incoming_message.recv() => {
                    self.state.on_message(reply);
                }
            }
        }
    }
}

struct ClientTaskContext {
    network_connect: NetworkConnectHandle,
}

impl ClientContext for ClientTaskContext {
    fn send(&mut self, request: Request) {
        let bytes = bincode::encode_to_vec(&request, bincode::config::standard()).unwrap();
        let _ = self.network_connect.send(bytes.into());
    }
}

pub struct NetworkConnectTask {
    txs_outgoing_message: HashMap<NodeIndex, UnboundedSender<Bytes>>,
    join_set: JoinSet<anyhow::Result<()>>,
}

struct NetworkConnectHandle {
    txs_outgoing_message: HashMap<NodeIndex, UnboundedSender<Bytes>>,
}

impl NetworkConnectHandle {
    fn send(&self, bytes: Bytes) -> anyhow::Result<()> {
        self.txs_outgoing_message
            .values()
            .choose(&mut rand::rng())
            .unwrap()
            .send(bytes)?;
        Ok(())
    }
}

impl NetworkConnectTask {
    async fn load(
        endpoint: Endpoint,
        client: ClientHandle,
        schema: &schema::ClientTask,
        shard_index: ShardIndex,
        client_id: ClientId,
    ) -> anyhow::Result<Self> {
        let mut txs_outgoing_message = HashMap::new();
        let mut join_set = JoinSet::new();

        for (i, &ip) in schema.ips[shard_index as usize].iter().enumerate() {
            let conn = endpoint
                .connect((ip, 5000).into(), "server.example")?
                .await?;
            conn.open_uni()
                .await?
                .write_all(&client_id.to_le_bytes())
                .await?;
            join_set.spawn(Self::run_connection_incoming(conn.clone(), client.clone()));
            let (tx_outgoing, rx_outgoing) = unbounded_channel();
            join_set.spawn(Self::run_connection_outgoing(conn.clone(), rx_outgoing));
            txs_outgoing_message.insert(i as NodeIndex, tx_outgoing);
        }
        Ok(Self {
            txs_outgoing_message,
            join_set,
        })
    }

    fn handle(&self) -> NetworkConnectHandle {
        NetworkConnectHandle {
            txs_outgoing_message: self.txs_outgoing_message.clone(),
        }
    }

    async fn run_connection_incoming(conn: Connection, client: ClientHandle) -> anyhow::Result<()> {
        loop {
            let mut recv = conn.accept_uni().await?;
            let bytes = recv.read_to_end(usize::MAX).await?;
            let message = bincode::decode_from_slice(&bytes, bincode::config::standard())
                .unwrap()
                .0;
            let _ = client.incoming_message(message).await;
        }
    }

    async fn run_connection_outgoing(
        conn: Connection,
        mut tx_outgoing_message: UnboundedReceiver<Bytes>,
    ) -> anyhow::Result<()> {
        while let Some(bytes) = tx_outgoing_message.recv().await {
            let mut send = conn.open_uni().await?;
            send.write_all(&bytes).await?;
        }
        Ok(())
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await })
            .await?
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(res) = self.join_set.join_next().await {
            if let Err(err) = res.unwrap() {
                error!("NetworkConnectTask error: {}", err);
            }
        }
        Ok(())
    }
}

pub struct ClientNodeTask {
    scrape_state: Arc<Mutex<ClientScrapeState>>,
    connected_clients: Vec<ConnectedClientTask>,
    workloads: Vec<WorkloadTask>,
}

struct ConnectedClientTask {
    network_connect: NetworkConnectTask,
    client: ClientTask,
}

impl ClientNodeTask {
    pub async fn load(schema: schema::ClientTask) -> anyhow::Result<Self> {
        debug!("loading client node task");
        let scrape_state = Arc::new(Mutex::new(ClientScrapeState::now()));

        let mut transport_config = TransportConfig::default();
        transport_config.keep_alive_interval(Duration::from_secs(10).into());
        let mut config = client_config();
        config.transport_config(transport_config.into());

        let mut connected_clients = Vec::new();
        let mut workloads = Vec::new();
        let num_group = 4;
        for group_index in 0..num_group {
            let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
            endpoint.set_default_client_config(config.clone());

            let mut client_handles = Vec::new();
            for shard in 0..schema.ips.len() {
                let client_channels = ClientChannels::new();

                let client_id = rand::random();

                let network_connect = NetworkConnectTask::load(
                    endpoint.clone(),
                    client_channels.handle(),
                    &schema,
                    shard as _,
                    client_id,
                )
                .await?;
                debug!("[{:08x}] network connect loaded", client_id);
                let client = ClientTask::load(
                    client_channels,
                    network_connect.handle(),
                    &schema,
                    client_id,
                )
                .await?;
                debug!("[{:08x}] client loaded", client_id);

                client_handles.push(client.channels.handle());
                connected_clients.push(ConnectedClientTask {
                    network_connect,
                    client,
                });
            }

            let num_concurrent = schema.workload_config.num_concurrent / num_group
                + (group_index < schema.workload_config.num_concurrent % num_group) as u64;
            let scrape_state = scrape_state.clone();

            let client_worker_channels = ClientWorkerChannels::new();
            let client_worker = WorkloadTask::load(
                client_worker_channels,
                client_handles.clone(),
                scrape_state.clone(),
                &schema,
                num_concurrent as u32,
            )?;
            workloads.push(client_worker);
        }
        debug!("client node task loaded");

        Ok(Self {
            scrape_state,
            connected_clients,
            workloads,
        })
    }

    pub fn scrape_state(&self) -> Arc<Mutex<ClientScrapeState>> {
        self.scrape_state.clone()
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        let mut tasks = JoinSet::new();
        // TODO remove double layer spawn
        for ConnectedClientTask {
            network_connect,
            client,
        } in self.connected_clients
        {
            tasks.spawn(network_connect.run(stop.clone()));
            tasks.spawn(client.run(stop.clone()));
        }
        for workload in self.workloads {
            tasks.spawn(workload.run(stop.clone()));
        }
        while let Some(res) = tasks.join_next().await {
            res??;
        }
        Ok(())
    }
}

pub struct ClientScrapeState {
    start: Instant,
    pub latency_histogram: Histogram<u64>,
}

impl ClientScrapeState {
    pub fn now() -> Self {
        Self {
            start: Instant::now(),
            latency_histogram: Histogram::new(3).unwrap(),
        }
    }

    pub fn scrape(&mut self) -> schema::Scrape {
        let interval = self.start.elapsed();
        self.start = Instant::now();
        let mut serializer = V2Serializer::new();
        let mut latency_histogram = Vec::new();
        serializer
            .serialize(&self.latency_histogram, &mut latency_histogram)
            .unwrap();
        self.latency_histogram.reset();
        schema::Scrape {
            interval,
            latency_histogram,
        }
    }
}
