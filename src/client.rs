use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use log::error;
use quinn::{Connection, Endpoint};
use rand::seq::IteratorRandom as _;
use tokio::{
    select,
    sync::mpsc::{
        Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
    },
    task::JoinSet,
    time::interval,
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::{ClientId, NodeIndex, Reply, Request, ResponseContext},
    schema,
};

use self::state::{Client, ClientContext};

mod state;

pub struct ClientChannels {
    tx_invoke: UnboundedSender<(Vec<u8>, ResponseContext<Vec<u8>>)>,
    rx_invoke: UnboundedReceiver<(Vec<u8>, ResponseContext<Vec<u8>>)>,

    tx_incoming_message: Sender<Reply>,
    rx_incoming_message: Receiver<Reply>,
}

#[derive(Clone)]
pub struct ClientHandle {
    pub tx_invoke: UnboundedSender<(Vec<u8>, ResponseContext<Vec<u8>>)>,
    tx_incoming_message: Sender<Reply>,
}

impl Default for ClientChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientChannels {
    pub fn new() -> Self {
        let (tx_invoke, rx_invoke) = unbounded_channel();
        let (tx_incoming_message, rx_incoming_message) = channel(100);
        Self {
            tx_invoke,
            rx_invoke,
            tx_incoming_message,
            rx_incoming_message,
        }
    }

    pub fn handle(&self) -> ClientHandle {
        ClientHandle {
            tx_invoke: self.tx_invoke.clone(),
            tx_incoming_message: self.tx_incoming_message.clone(),
        }
    }
}

impl ClientHandle {
    pub async fn incoming_message(&self, reply: Reply) -> anyhow::Result<()> {
        self.tx_incoming_message.send(reply).await?;
        Ok(())
    }
}

pub struct ClientTask {
    pub channels: ClientChannels,
    state: Client<ClientTaskContext>,
}

impl ClientTask {
    fn new(channels: ClientChannels, state: Client<ClientTaskContext>) -> Self {
        Self { channels, state }
    }

    pub async fn load(
        client_channels: ClientChannels,
        network_connect: NetworkConnectHandle,
        schema: &schema::ClientTask,
        client_id: ClientId,
    ) -> anyhow::Result<Self> {
        let client_context = ClientTaskContext { network_connect };
        let state = Client::new(client_context, schema.config.clone(), client_id);
        Ok(Self::new(client_channels, state))
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
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

pub struct NetworkConnectTask<const BATCH: bool = false> {
    txs_outgoing_message: HashMap<NodeIndex, UnboundedSender<Bytes>>,
    join_set: JoinSet<anyhow::Result<()>>,
}

pub struct NetworkConnectHandle {
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

impl<const BATCH: bool> NetworkConnectTask<BATCH> {
    pub async fn load(
        endpoint: Endpoint,
        client: ClientHandle,
        schema: &schema::ClientTask,
        shard_index: schema::ShardIndex,
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

    pub fn handle(&self) -> NetworkConnectHandle {
        NetworkConnectHandle {
            txs_outgoing_message: self.txs_outgoing_message.clone(),
        }
    }

    async fn run_connection_incoming(conn: Connection, client: ClientHandle) -> anyhow::Result<()> {
        loop {
            let mut recv = conn.accept_uni().await?;
            let mut bytes = &*recv.read_to_end(usize::MAX).await?;
            while !bytes.is_empty() {
                let (message, len) =
                    bincode::decode_from_slice(bytes, bincode::config::standard())?;
                let _ = client.incoming_message(message).await;
                bytes = &bytes[len..];
            }
        }
    }

    async fn run_connection_outgoing(
        conn: Connection,
        mut rx_outgoing_message: UnboundedReceiver<Bytes>,
    ) -> anyhow::Result<()> {
        let mut buf;
        let mut interval = interval(Duration::from_millis(1));
        while {
            if BATCH {
                interval.tick().await;
            }
            buf = Vec::new();
            rx_outgoing_message.recv_many(&mut buf, 1_000).await > 0
        } {
            let mut send = conn.open_uni().await?;
            send.write_all_chunks(&mut buf).await?;
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
