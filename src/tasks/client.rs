use std::{collections::HashMap, net::SocketAddr};

use quinn::{Connection, Endpoint};
use tokio::{
    select,
    sync::mpsc::{
        Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{
    cert::client_config,
    client::{Client, ClientContext, ClientWorker},
    schema::ClientTask,
    types::{ClientId, ClientSeq, NodeIndex, Reply, Request},
};

struct ClientWorkerChannels {
    tx_incoming_message: Sender<Reply>,
    rx_incoming_message: Receiver<Reply>,

    tx_finalize: UnboundedSender<(ClientSeq, Vec<u8>)>,
    rx_finalize: UnboundedReceiver<(ClientSeq, Vec<u8>)>,
}

pub struct ClientWorkerTask {
    channels: ClientWorkerChannels,
    client_worker: ClientWorker<Context>,
}

impl ClientWorkerChannels {
    fn new() -> Self {
        let (tx_incoming_message, rx_incoming_message) = channel(100);
        let (tx_finalize, rx_finalize) = unbounded_channel();
        Self {
            tx_incoming_message,
            rx_incoming_message,
            tx_finalize,
            rx_finalize,
        }
    }
}

impl ClientWorkerTask {
    fn new(channels: ClientWorkerChannels, client_worker: ClientWorker<Context>) -> Self {
        Self {
            channels,
            client_worker,
        }
    }

    pub async fn run(mut self, stop: CancellationToken) {
        stop.run_until_cancelled(self.run_inner()).await;
    }

    async fn run_inner(&mut self) {
        loop {
            select! {
                Some((seq, res)) = self.channels.rx_finalize.recv() => {
                    self.client_worker.on_finalize(seq, res);
                }
                Some(message) = self.channels.rx_incoming_message.recv() => {
                    self.client_worker.client.on_message(message);
                }
            }
        }
    }
}

struct Context {
    tx_finalize: UnboundedSender<(ClientSeq, Vec<u8>)>,
    tx_outgoing_message: UnboundedSender<(NodeIndex, Request)>,
}

impl ClientContext for Context {
    fn finalize(&mut self, seq: ClientSeq, res: Vec<u8>) {
        let _ = self.tx_finalize.send((seq, res));
    }

    fn send(&mut self, to: NodeIndex, request: Request) {
        let _ = self.tx_outgoing_message.send((to, request));
    }
}

pub struct NetworkOutgoingTask {
    tx_outgoing_message: UnboundedSender<(NodeIndex, Request)>,
    rx_outgoing_message: UnboundedReceiver<(NodeIndex, Request)>,

    connections: HashMap<NodeIndex, Connection>,
}

impl NetworkOutgoingTask {
    fn new(connections: HashMap<NodeIndex, Connection>) -> Self {
        let (tx_outgoing_message, rx_outgoing_message) = unbounded_channel();
        Self {
            tx_outgoing_message,
            rx_outgoing_message,
            connections,
        }
    }

    pub async fn load(
        client_id: ClientId,
        addrs: Vec<SocketAddr>,
        tx_incoming_message: Sender<Reply>,
    ) -> anyhow::Result<Self> {
        let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
        endpoint.set_default_client_config(client_config());
        let mut connections = HashMap::new();
        for (i, &addr) in addrs.iter().enumerate() {
            let conn = endpoint.connect(addr, "server.example")?.await?;
            conn.open_uni()
                .await?
                .write_all(&client_id.to_le_bytes())
                .await?;
            tokio::spawn(Self::handle_connection(
                conn.clone(),
                tx_incoming_message.clone(),
            ));
            connections.insert(i as _, conn);
        }
        Ok(Self::new(connections))
    }

    async fn handle_connection(
        conn: Connection,
        tx_incoming_message: Sender<Reply>,
    ) -> anyhow::Result<()> {
        loop {
            let mut recv = conn.accept_uni().await?;
            let tx_incoming_message = tx_incoming_message.clone();
            tokio::spawn(async move {
                let bytes = recv.read_to_end(usize::MAX).await?;
                let message = bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;
                let _ = tx_incoming_message.send(message).await;
                anyhow::Ok(())
            });
        }
    }

    pub async fn run(mut self, stop: CancellationToken) {
        stop.run_until_cancelled(self.run_inner()).await;
    }

    async fn run_inner(&mut self) {
        while let Some((to, message)) = self.rx_outgoing_message.recv().await {
            self.handle_outgoing_message(to, message)
        }
    }

    fn handle_outgoing_message(&mut self, id: NodeIndex, request: Request) {
        if let Some(conn) = self.connections.get(&id) {
            let conn = conn.clone();
            tokio::spawn(async move {
                let mut send = conn.open_uni().await?;
                let bytes = bincode::encode_to_vec(&request, bincode::config::standard())?;
                send.write_all(&bytes).await?;
                anyhow::Ok(())
            });
        }
    }
}

pub struct ClientNodeTask {
    network_outgoing: NetworkOutgoingTask,
    client_worker: ClientWorkerTask,
}

impl ClientNodeTask {
    pub async fn load(schema: ClientTask) -> anyhow::Result<Self> {
        let channels = ClientWorkerChannels::new();

        let client_id = rand::random();
        let network_outgoing = NetworkOutgoingTask::load(
            client_id,
            schema.addrs,
            channels.tx_incoming_message.clone(),
        )
        .await?;
        let context = Context {
            tx_finalize: channels.tx_finalize.clone(),
            tx_outgoing_message: network_outgoing.tx_outgoing_message.clone(),
        };

        let client_worker = ClientWorker::new(
            Client::new(context, schema.config, client_id),
            schema.worker_config,
        );
        let client_worker = ClientWorkerTask::new(channels, client_worker);
        Ok(Self {
            network_outgoing,
            client_worker,
        })
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::join!(
            self.network_outgoing.run(stop.clone()),
            self.client_worker.run(stop)
        );
        Ok(())
    }
}
