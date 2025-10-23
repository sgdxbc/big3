use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use log::{debug, error};
use quinn::{Connection, Endpoint};
use rand::{Rng, rng};
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel},
        oneshot,
    },
    task::JoinSet,
    time::{interval, sleep},
};
use tokio_util::sync::CancellationToken;

use crate::{
    cert::client_config,
    client::{Client, ClientContext, ClientWorker, ClientWorkerContext, InvokeId, Records},
    schema,
    types::{ClientId, NodeIndex, Reply, Request},
};

struct ClientWorkerChannels {
    tx_invoke_response: Sender<(InvokeId, Vec<u8>)>,
    rx_invoke_response: Receiver<(InvokeId, Vec<u8>)>,
}

#[derive(Clone)]
struct ClientWorkerHandle {
    tx_invoke_response: Sender<(InvokeId, Vec<u8>)>,
}

impl ClientWorkerChannels {
    fn new() -> Self {
        let (tx_invoke_response, rx_invoke_response) = channel(100);
        Self {
            tx_invoke_response,
            rx_invoke_response,
        }
    }

    fn handle(&self) -> ClientWorkerHandle {
        ClientWorkerHandle {
            tx_invoke_response: self.tx_invoke_response.clone(),
        }
    }
}

impl ClientWorkerHandle {
    async fn invoke_response(&self, seq: InvokeId, res: Vec<u8>) -> anyhow::Result<()> {
        self.tx_invoke_response.send((seq, res)).await?;
        Ok(())
    }
}

pub struct ClientWorkerTask {
    channels: ClientWorkerChannels,
    state: ClientWorker<ClientWorkerTaskContext>,
}

impl ClientWorkerTask {
    fn new(channels: ClientWorkerChannels, state: ClientWorker<ClientWorkerTaskContext>) -> Self {
        Self { channels, state }
    }

    fn load(
        client_worker_channels: ClientWorkerChannels,
        client: ClientHandle,
        schema: &schema::ClientTask,
    ) -> anyhow::Result<Self> {
        debug!("loading client worker task");
        let context = ClientWorkerTaskContext::new(client_worker_channels.handle(), client);
        debug!("client worker context created");
        let state = ClientWorker::new(context, schema.worker_config.clone());
        debug!("client worker state created");
        Ok(Self::new(client_worker_channels, state))
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await }).await?;
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
                    self.state.on_tick();
                }
                Some((seq, res)) = self.channels.rx_invoke_response.recv() => {
                    self.state.on_invoke_response(seq, res);
                }
            }
        }
    }
}

struct ClientWorkerTaskContext {
    client: ClientHandle,
    client_worker: ClientWorkerHandle,
    invoke_id: InvokeId,
}

impl ClientWorkerTaskContext {
    fn new(client_worker: ClientWorkerHandle, client: ClientHandle) -> Self {
        Self {
            client_worker,
            client,
            invoke_id: 0,
        }
    }
}

impl ClientWorkerContext for ClientWorkerTaskContext {
    fn invoke(&mut self, command: Vec<u8>) -> InvokeId {
        self.invoke_id += 1;
        let invoke_id = self.invoke_id;
        let client = self.client.clone();
        let client_worker = self.client_worker.clone();
        tokio::spawn(async move {
            let res = client.invoke(command).await?;
            client_worker.invoke_response(invoke_id, res).await
        });
        self.invoke_id
    }
}

struct ClientChannels {
    tx_invoke: UnboundedSender<(Vec<u8>, oneshot::Sender<Vec<u8>>)>,
    rx_invoke: UnboundedReceiver<(Vec<u8>, oneshot::Sender<Vec<u8>>)>,

    tx_incoming_message: Sender<Reply>,
    rx_incoming_message: Receiver<Reply>,
}

#[derive(Clone)]
struct ClientHandle {
    tx_invoke: UnboundedSender<(Vec<u8>, oneshot::Sender<Vec<u8>>)>,
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
    async fn invoke(&self, command: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let (tx_response, rx_response) = oneshot::channel();
        self.tx_invoke.send((command, tx_response))?;
        let res = rx_response.await?;
        Ok(res)
    }

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
    fn send(&mut self, to: NodeIndex, request: Request) {
        let bytes = bincode::encode_to_vec(&request, bincode::config::standard()).unwrap();
        let _ = self.network_connect.send(to, bytes.into());
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
    fn send(&self, to: NodeIndex, bytes: Bytes) -> anyhow::Result<()> {
        self.txs_outgoing_message[&to].send(bytes)?;
        Ok(())
    }
}

impl NetworkConnectTask {
    async fn load(
        client: ClientHandle,
        schema: &schema::ClientTask,
        client_id: ClientId,
    ) -> anyhow::Result<Self> {
        let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
        endpoint.set_default_client_config(client_config());
        let mut txs_outgoing_message = HashMap::new();
        let mut join_set = JoinSet::new();
        for (i, &ip) in schema.ips.iter().enumerate() {
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
    network_connect: NetworkConnectTask,
    client: ClientTask,
    client_worker: ClientWorkerTask,
}

impl ClientNodeTask {
    pub async fn load(schema: schema::ClientTask) -> anyhow::Result<Self> {
        debug!("loading client node task");
        let client_channels = ClientChannels::new();
        let client_worker_channels = ClientWorkerChannels::new();

        let client_id = rand::random();

        let network_connect =
            NetworkConnectTask::load(client_channels.handle(), &schema, client_id).await?;
        debug!("network connect loaded");
        let client = ClientTask::load(
            client_channels,
            network_connect.handle(),
            &schema,
            client_id,
        )
        .await?;
        debug!("client loaded");
        let client_worker =
            ClientWorkerTask::load(client_worker_channels, client.channels.handle(), &schema)?;
        debug!("client worker loaded");
        Ok(Self {
            network_connect,
            client,
            client_worker,
        })
    }

    pub fn scrape_state(&self) -> Arc<Mutex<Records>> {
        Arc::clone(&self.client_worker.state.records)
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::try_join!(
            self.network_connect.run(stop.clone()),
            self.client.run(stop.clone()),
            self.client_worker.run(stop.clone())
        )?;
        Ok(())
    }
}
