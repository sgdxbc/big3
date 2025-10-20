use std::collections::HashMap;

use bytes::Bytes;
use quinn::{Connection, Endpoint};
use tempfile::{TempDir, tempdir};
use tokio::{
    process::Command,
    select,
    sync::{
        mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel},
        oneshot,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{
    cert::{client_config, server_config},
    consensus::{Block, Bullshark, BullsharkContext},
    execute::{Execute, ExecuteContext, FetchId},
    schema,
    storage::{Storage, StorageOp},
    tasks::PREFILL_PATH,
    types::{ClientId, NodeIndex, Reply, Request},
};

pub struct ConsensusChannels {
    tx_request: UnboundedSender<Request>,
    rx_request: UnboundedReceiver<Request>,

    tx_incoming_message: Sender<crate::consensus::message::Message>,
    rx_incoming_message: Receiver<crate::consensus::message::Message>,
}

impl ConsensusChannels {
    fn new() -> Self {
        let (tx_request, rx_request) = unbounded_channel();
        let (tx_incoming_message, rx_incoming_message) = channel(100);
        Self {
            tx_request,
            rx_request,
            tx_incoming_message,
            rx_incoming_message,
        }
    }
}

pub struct ConsensusTask {
    channels: ConsensusChannels,
    consensus: Bullshark<ConsensusTaskContext>,
}

impl ConsensusTask {
    fn new(channels: ConsensusChannels, consensus: Bullshark<ConsensusTaskContext>) -> Self {
        Self {
            channels,
            consensus,
        }
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move {
            stop.run_until_cancelled(self.run_inner()).await;
            self.consensus.log_metrics();
        })
        .await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        self.consensus.init();
        loop {
            select! {
                Some(message) = self.channels.rx_incoming_message.recv() => {
                    self.consensus.on_message(message);
                }
                Some(request) = self.channels.rx_request.recv() => {
                    self.consensus.on_request(request);
                }
            }
        }
    }
}

struct ConsensusTaskContext {
    tx_deliver: UnboundedSender<Block>,
    txs_outgoing_message: HashMap<NodeIndex, UnboundedSender<Bytes>>,
}

impl BullsharkContext for ConsensusTaskContext {
    fn output(&mut self, block: Block) {
        let _ = self.tx_deliver.send(block);
    }

    fn send(&mut self, node_index: NodeIndex, message: crate::consensus::message::Message) {
        let bytes = bincode::encode_to_vec(&message, bincode::config::standard()).unwrap();
        let _ = self.txs_outgoing_message[&node_index].send(bytes.into());
    }

    fn send_to_all(&mut self, message: crate::consensus::message::Message) {
        let bytes =
            Bytes::from(bincode::encode_to_vec(&message, bincode::config::standard()).unwrap());
        for tx in self.txs_outgoing_message.values() {
            let _ = tx.send(bytes.clone());
        }
    }
}

pub struct ExecuteTask {
    tx_request: Sender<Request>,
    rx_request: Receiver<Request>,

    tx_block: UnboundedSender<Block>,
    rx_block: UnboundedReceiver<Block>,
    execute: Execute<ExecuteTaskContext>,
}

impl ExecuteTask {
    fn new(execute: Execute<ExecuteTaskContext>) -> Self {
        let (tx_request, rx_request) = channel(100);
        let (tx_block, rx_block) = unbounded_channel();
        Self {
            tx_request,
            rx_request,
            tx_block,
            rx_block,
            execute,
        }
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await }).await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        loop {
            select! {
                Some((fetch_id, value)) = self.execute.context.rx_fetch_response.recv() => {
                    self.execute.on_fetch_response(fetch_id, value);
                }
                Some(block) = self.rx_block.recv() => {
                    self.execute.on_block(block);
                }
                Some(request) = self.rx_request.recv() => {
                    self.execute.on_request(request);
                }
            }
        }
    }
}

struct ExecuteTaskContext {
    tx_outgoing_message: UnboundedSender<(ClientId, Reply)>,
    tx_storage_op: UnboundedSender<StorageOp>,
    tx_fetch_response: Sender<(FetchId, Option<Vec<u8>>)>,
    rx_fetch_response: Receiver<(FetchId, Option<Vec<u8>>)>,
    fetch_id: FetchId,
    tx_request: UnboundedSender<Request>,
}

impl ExecuteTaskContext {
    fn new(
        tx_outgoing_message: UnboundedSender<(ClientId, Reply)>,
        tx_storage_op: UnboundedSender<StorageOp>,
        tx_request: UnboundedSender<Request>,
    ) -> Self {
        let (tx_fetch_response, rx_fetch_response) = channel(100);
        Self {
            tx_outgoing_message,
            tx_storage_op,
            tx_request,
            tx_fetch_response,
            rx_fetch_response,
            fetch_id: 0,
        }
    }
}

impl ExecuteContext for ExecuteTaskContext {
    fn send(&mut self, id: ClientId, reply: Reply) {
        let _ = self.tx_outgoing_message.send((id, reply));
    }

    fn fetch(&mut self, key: [u8; 32]) -> FetchId {
        self.fetch_id += 1;
        let fetch_id = self.fetch_id;
        let (tx_response, rx_response) = oneshot::channel();
        let _ = self.tx_storage_op.send(StorageOp::Fetch(key, tx_response));
        let tx_fetch_response = self.tx_fetch_response.clone();
        tokio::spawn(async move {
            let _ = tx_fetch_response.send((fetch_id, rx_response.await?)).await;
            anyhow::Ok(())
        });
        fetch_id
    }

    fn post(&mut self, updates: Vec<([u8; 32], Option<Vec<u8>>)>) {
        let _ = self.tx_storage_op.send(StorageOp::Post(updates));
    }

    fn submit(&mut self, request: Request) {
        let _ = self.tx_request.send(request);
    }
}

pub struct StorageTask {
    tx_storage_op: UnboundedSender<StorageOp>,
    rx_storage_op: UnboundedReceiver<StorageOp>,
    storage: Storage,
}

impl StorageTask {
    fn new(storage: Storage) -> Self {
        let (tx_storage_op, rx_storage_op) = unbounded_channel();
        Self {
            tx_storage_op,
            rx_storage_op,
            storage,
        }
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await }).await?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(op) = self.rx_storage_op.recv().await {
            self.storage.invoke(op)?;
        }
        Ok(())
    }
}

pub struct NetworkAcceptTask {
    endpoint: Endpoint,

    // execute handle
    tx_incoming_message: Sender<Request>,

    // outgoing handle
    tx_connection: Sender<(ClientId, UnboundedSender<Bytes>)>,
}

impl NetworkAcceptTask {
    pub fn new(
        endpoint: Endpoint,
        tx_incoming_message: Sender<Request>,
        tx_connection: Sender<(ClientId, UnboundedSender<Bytes>)>,
    ) -> Self {
        Self {
            endpoint,
            tx_incoming_message,
            tx_connection,
        }
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await })
            .await?
            .unwrap_or(Ok(()))?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(incoming) = self.endpoint.accept().await {
            let conn = incoming.await?;
            let mut client_id = [0; size_of::<ClientId>()];
            conn.accept_uni().await?.read_exact(&mut client_id).await?;

            let (tx_outgoing, rx_outgoing) = unbounded_channel();
            let _ = self
                .tx_connection
                .send((ClientId::from_le_bytes(client_id), tx_outgoing))
                .await;
            tokio::spawn(Self::run_connection_incoming(
                conn.clone(),
                self.tx_incoming_message.clone(),
            ));
            tokio::spawn(Self::run_connection_outgoing(conn, rx_outgoing));
        }
        Ok(())
    }

    async fn run_connection_incoming(
        conn: Connection,
        tx_incoming_message: Sender<Request>,
    ) -> anyhow::Result<()> {
        loop {
            let mut recv = conn.accept_uni().await?;
            let bytes = recv.read_to_end(usize::MAX).await?;
            let message = bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;
            let _ = tx_incoming_message.send(message).await;
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
}

pub struct NetworkOutgoingTask {
    tx_connection: Sender<(ClientId, UnboundedSender<Bytes>)>,
    rx_connection: Receiver<(ClientId, UnboundedSender<Bytes>)>,

    tx_outgoing_message: UnboundedSender<(ClientId, Reply)>,
    rx_outgoing_message: UnboundedReceiver<(ClientId, Reply)>,

    connections: HashMap<ClientId, UnboundedSender<Bytes>>,
}

impl NetworkOutgoingTask {
    fn new() -> Self {
        let (tx_connection, rx_connection) = channel(100);
        let (tx_outgoing_message, rx_outgoing_message) = unbounded_channel();
        Self {
            tx_connection,
            rx_connection,
            tx_outgoing_message,
            rx_outgoing_message,
            connections: Default::default(),
        }
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await })
            .await?
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            select! {
                Some((id, conn)) = self.rx_connection.recv() => {
                    self.handle_connection(id, conn);
                }
                Some((id, reply)) = self.rx_outgoing_message.recv() => {
                    self.handle_outgoing_message(id, reply)?;
                }
            }
        }
    }

    fn handle_connection(&mut self, id: ClientId, conn: UnboundedSender<Bytes>) {
        self.connections.insert(id, conn);
    }

    fn handle_outgoing_message(&mut self, id: ClientId, reply: Reply) -> anyhow::Result<()> {
        let bytes = bincode::encode_to_vec(&reply, bincode::config::standard())?;
        let _ = self.connections[&id].send(bytes.into());
        anyhow::Ok(())
    }
}

pub struct NetworkConnectTask {
    txs_outgoing_message: HashMap<NodeIndex, UnboundedSender<Bytes>>,
}

impl NetworkConnectTask {
    pub async fn load(
        schema: &schema::ReplicaTask,
        tx_incoming_message: Sender<crate::consensus::message::Message>,
    ) -> anyhow::Result<Self> {
        let mut endpoint = Endpoint::server(
            server_config(),
            (schema.ips[schema.node_index as usize], 6000).into(),
        )?;
        endpoint.set_default_client_config(client_config());

        let connect = async {
            let mut txs = HashMap::new();
            for (i, &ip) in schema.ips[..schema.node_index as usize].iter().enumerate() {
                let conn = endpoint
                    .connect((ip, 6000).into(), "server.example")?
                    .await?;
                conn.open_uni()
                    .await?
                    .write_all(&schema.node_index.to_le_bytes())
                    .await?;
                let (tx_outgoing, rx_outgoing) = unbounded_channel();
                tokio::spawn(Self::run_connection_incoming(
                    conn.clone(),
                    tx_incoming_message.clone(),
                ));
                tokio::spawn(Self::run_connection_outgoing(conn.clone(), rx_outgoing));
                txs.insert(i as NodeIndex, tx_outgoing);
            }
            anyhow::Ok(txs)
        };
        let accept = async {
            let mut txs = HashMap::new();
            while txs.len() < (schema.ips.len() - schema.node_index as usize - 1) {
                let conn = endpoint.accept().await.unwrap().await?;
                let mut client_id = [0; size_of::<NodeIndex>()];
                conn.accept_uni().await?.read_exact(&mut client_id).await?;
                let client_index = NodeIndex::from_le_bytes(client_id);
                let (tx_outgoing, rx_outgoing) = unbounded_channel();
                tokio::spawn(Self::run_connection_incoming(
                    conn.clone(),
                    tx_incoming_message.clone(),
                ));
                tokio::spawn(Self::run_connection_outgoing(conn.clone(), rx_outgoing));
                txs.insert(client_index, tx_outgoing);
            }
            anyhow::Ok(txs)
        };
        let (txs_lower, txs_higher) = tokio::try_join!(connect, accept)?;

        let mut txs_outgoing_message = txs_lower;
        txs_outgoing_message.extend(txs_higher);
        Ok(Self {
            txs_outgoing_message,
        })
    }

    async fn run_connection_incoming(
        conn: Connection,
        tx_incoming_message: Sender<crate::consensus::message::Message>,
    ) -> anyhow::Result<()> {
        loop {
            let mut recv = conn.accept_uni().await?;
            let bytes = recv.read_to_end(usize::MAX).await?;
            let message = bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;
            let _ = tx_incoming_message.send(message).await;
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
        Ok(())
    }
}

pub struct ReplicaNodeTask {
    network_outgoing: NetworkOutgoingTask,
    network_accept: NetworkAcceptTask,
    network_connect: NetworkConnectTask,
    consensus: ConsensusTask,
    execute: ExecuteTask,
    storage: StorageTask,
    _temp_dir: TempDir,
}

impl ReplicaNodeTask {
    pub async fn load(schema: schema::ReplicaTask) -> anyhow::Result<Self> {
        let consensus_channels = ConsensusChannels::new();

        let network_outgoing = NetworkOutgoingTask::new();

        let temp_dir = tempdir()?;
        let status = Command::new("cp")
            .arg("-rT")
            .arg(PREFILL_PATH)
            .arg(temp_dir.path())
            .status()
            .await?;
        anyhow::ensure!(status.success(), "failed to copy prefill data");
        let storage = StorageTask::new(Storage::new(temp_dir.path())?);

        let execute_context = ExecuteTaskContext::new(
            network_outgoing.tx_outgoing_message.clone(),
            storage.tx_storage_op.clone(),
            consensus_channels.tx_request.clone(),
        );
        let execute = ExecuteTask::new(Execute::new(execute_context, schema.node_index));

        let network_connect =
            NetworkConnectTask::load(&schema, consensus_channels.tx_incoming_message.clone())
                .await?;

        let consensus_context = ConsensusTaskContext {
            tx_deliver: execute.tx_block.clone(),
            txs_outgoing_message: network_connect.txs_outgoing_message.clone(),
        };
        let consensus = ConsensusTask::new(
            consensus_channels,
            Bullshark::new(
                consensus_context,
                (&schema.config).into(),
                schema.node_index,
            ),
        );

        let endpoint = {
            let mut server_config = server_config();
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.max_concurrent_uni_streams(1000u32.into());
            server_config.transport_config(transport_config.into());
            Endpoint::server(server_config, ([0, 0, 0, 0], 5000).into())?
        };
        let network_accept = NetworkAcceptTask::new(
            endpoint,
            execute.tx_request.clone(),
            network_outgoing.tx_connection.clone(),
        );
        Ok(Self {
            network_outgoing,
            network_accept,
            network_connect,
            execute,
            storage,
            consensus,
            _temp_dir: temp_dir,
        })
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::try_join!(
            self.network_outgoing.run(stop.clone()),
            self.network_accept.run(stop.clone()),
            self.network_connect.run(stop.clone()),
            self.execute.run(stop.clone()),
            self.consensus.run(stop.clone()),
            self.storage.run(stop.clone()),
        )?;
        Ok(())
    }
}
