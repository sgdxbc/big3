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
    consensus::{Block, Bullshark, BullsharkContext, OutputId},
    execute::{Execute, ExecuteContext, FetchId},
    schema,
    storage::{Storage, StorageOp},
    tasks::PREFILL_PATH,
    types::{ClientId, NodeIndex, Reply, Request},
};

pub struct ConsensusChannels {
    tx_request: Sender<Request>,
    rx_request: Receiver<Request>,

    tx_incoming_message: Sender<crate::consensus::message::Message>,
    rx_incoming_message: Receiver<crate::consensus::message::Message>,

    tx_output_response: Sender<OutputId>,
    rx_output_response: Receiver<OutputId>,
}

#[derive(Clone)]
struct ConsensusHandle {
    tx_request: Sender<Request>,
    tx_incoming_message: Sender<crate::consensus::message::Message>,
    tx_output_response: Sender<OutputId>,
}

impl ConsensusChannels {
    fn new() -> Self {
        let (tx_request, rx_request) = channel(100);
        let (tx_incoming_message, rx_incoming_message) = channel(100);
        let (tx_output_response, rx_output_response) = channel(100);
        Self {
            tx_request,
            rx_request,
            tx_incoming_message,
            rx_incoming_message,
            tx_output_response,
            rx_output_response,
        }
    }

    fn handle(&self) -> ConsensusHandle {
        ConsensusHandle {
            tx_request: self.tx_request.clone(),
            tx_incoming_message: self.tx_incoming_message.clone(),
            tx_output_response: self.tx_output_response.clone(),
        }
    }
}

impl ConsensusHandle {
    async fn incoming_message(
        &self,
        message: crate::consensus::message::Message,
    ) -> anyhow::Result<()> {
        self.tx_incoming_message.send(message).await?;
        anyhow::Ok(())
    }

    async fn submit_request(&self, request: Request) -> anyhow::Result<()> {
        self.tx_request.send(request).await?;
        anyhow::Ok(())
    }

    async fn output_response(&self, output_id: OutputId) -> anyhow::Result<()> {
        self.tx_output_response.send(output_id).await?;
        anyhow::Ok(())
    }
}

pub struct ConsensusTask {
    channels: ConsensusChannels,
    state: Bullshark<ConsensusTaskContext>,
}

impl ConsensusTask {
    fn new(channels: ConsensusChannels, state: Bullshark<ConsensusTaskContext>) -> Self {
        Self { channels, state }
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
        self.state.start();
        loop {
            select! {
                Some(message) = self.channels.rx_incoming_message.recv() => {
                    self.state.on_message(message);
                }
                Some(request) = self.channels.rx_request.recv() => {
                    self.state.on_request(request);
                }
                Some(output_id) = self.channels.rx_output_response.recv() => {
                    // self.state.on_output_response(output_id);
                }
            }
        }
    }
}

struct ConsensusTaskContext {
    consensus: ConsensusHandle,
    execute: ExecuteHandle,
    network_connect: NetworkConnectHandle,
    output_id: OutputId,
}

impl ConsensusTaskContext {
    fn new(
        consensus: ConsensusHandle,
        execute: ExecuteHandle,
        network_connect: NetworkConnectHandle,
    ) -> Self {
        Self {
            consensus,
            execute,
            network_connect,
            output_id: 0,
        }
    }
}

impl BullsharkContext for ConsensusTaskContext {
    fn output(&mut self, block: Block) -> OutputId {
        self.output_id += 1;
        let output_id = self.output_id;
        let execute = self.execute.clone();
        let consensus = self.consensus.clone();
        tokio::spawn(async move {
            execute.execute(block).await?;
            consensus.output_response(output_id).await?;
            anyhow::Ok(())
        });
        output_id
    }

    fn send(&mut self, node_index: NodeIndex, message: crate::consensus::message::Message) {
        self.network_connect.send(node_index, message);
    }

    fn send_to_all(&mut self, message: crate::consensus::message::Message) {
        self.network_connect.send_to_all(message);
    }
}

struct ExecuteChannels {
    tx_block: Sender<(Block, oneshot::Sender<()>)>,
    rx_block: Receiver<(Block, oneshot::Sender<()>)>,

    tx_fetch_response: Sender<(FetchId, Vec<Option<Vec<u8>>>)>,
    rx_fetch_response: Receiver<(FetchId, Vec<Option<Vec<u8>>>)>,
}

#[derive(Clone)]
struct ExecuteHandle {
    tx_block: Sender<(Block, oneshot::Sender<()>)>,
    tx_fetch_response: Sender<(FetchId, Vec<Option<Vec<u8>>>)>,
}

impl ExecuteChannels {
    fn new() -> Self {
        let (tx_block, rx_block) = channel(100);
        let (tx_fetch_response, rx_fetch_response) = channel(100);
        Self {
            tx_block,
            rx_block,
            tx_fetch_response,
            rx_fetch_response,
        }
    }

    fn handle(&self) -> ExecuteHandle {
        ExecuteHandle {
            tx_block: self.tx_block.clone(),
            tx_fetch_response: self.tx_fetch_response.clone(),
        }
    }
}

impl ExecuteHandle {
    async fn execute(&self, block: Block) -> anyhow::Result<()> {
        let (tx_response, rx_response) = oneshot::channel();
        self.tx_block.send((block, tx_response)).await?;
        rx_response.await?;
        anyhow::Ok(())
    }

    async fn fetch_response(
        &self,
        fetch_id: FetchId,
        values: Vec<Option<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        self.tx_fetch_response.send((fetch_id, values)).await?;
        anyhow::Ok(())
    }
}

pub struct ExecuteTask {
    channels: ExecuteChannels,
    state: Execute<ExecuteTaskContext>,
}

impl ExecuteTask {
    fn new(channels: ExecuteChannels, state: Execute<ExecuteTaskContext>) -> Self {
        Self { channels, state }
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
                Some((fetch_id, values)) = self.channels.rx_fetch_response.recv() => {
                    self.state.on_fetch_response(fetch_id, values);
                }
                Some((block, tx_response)) = self.channels.rx_block.recv() => {
                    self.state.on_block(block, tx_response);
                }
            }
        }
    }
}

struct ExecuteTaskContext {
    execute: ExecuteHandle,
    storage: StorageHandle,
    network_outgoing: NetworkOutgoingHandle,
    fetch_id: FetchId,
}

impl ExecuteTaskContext {
    fn new(
        execute: ExecuteHandle,
        storage: StorageHandle,
        network_outgoing: NetworkOutgoingHandle,
    ) -> Self {
        Self {
            execute,
            storage,
            network_outgoing,
            fetch_id: 0,
        }
    }
}

impl ExecuteContext for ExecuteTaskContext {
    fn send(&mut self, id: ClientId, reply: Reply) {
        let _ = self.network_outgoing.send_message(id, reply);
    }

    fn fetch(&mut self, keys: Vec<[u8; 32]>) -> FetchId {
        self.fetch_id += 1;
        let fetch_id = self.fetch_id;
        let execute = self.execute.clone();
        let storage = self.storage.clone();
        tokio::spawn(async move {
            let response = storage.fetch(keys).await?;
            execute.fetch_response(fetch_id, response).await?;
            anyhow::Ok(())
        });
        fetch_id
    }

    fn post(&mut self, updates: Vec<([u8; 32], Option<Vec<u8>>)>) {
        let _ = self.storage.post(updates);
    }
}

struct StorageChannels {
    tx_storage_op: UnboundedSender<StorageOp>,
    rx_storage_op: UnboundedReceiver<StorageOp>,
}

#[derive(Clone)]
struct StorageHandle {
    tx_storage_op: UnboundedSender<StorageOp>,
}

impl StorageChannels {
    fn new() -> Self {
        let (tx_storage_op, rx_storage_op) = unbounded_channel();
        Self {
            tx_storage_op,
            rx_storage_op,
        }
    }

    fn handle(&self) -> StorageHandle {
        StorageHandle {
            tx_storage_op: self.tx_storage_op.clone(),
        }
    }
}

impl StorageHandle {
    async fn fetch(&self, keys: Vec<[u8; 32]>) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let (tx_response, rx_response) = oneshot::channel();
        self.tx_storage_op
            .send(StorageOp::Fetch(keys, tx_response))?;
        let res = rx_response.await?;
        anyhow::Ok(res)
    }

    fn post(&self, updates: Vec<([u8; 32], Option<Vec<u8>>)>) -> anyhow::Result<()> {
        self.tx_storage_op.send(StorageOp::Post(updates))?;
        anyhow::Ok(())
    }
}

pub struct StorageTask {
    channels: StorageChannels,
    state: Storage,
}

impl StorageTask {
    fn new(channels: StorageChannels, state: Storage) -> Self {
        Self { channels, state }
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await }).await?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(op) = self.channels.rx_storage_op.recv().await {
            self.state.invoke(op)?;
        }
        Ok(())
    }
}

pub struct NetworkAcceptTask {
    endpoint: Endpoint,
    consensus: ConsensusHandle,
    network_outgoing: NetworkOutgoingHandle,
}

impl NetworkAcceptTask {
    fn new(
        endpoint: Endpoint,
        consensus: ConsensusHandle,
        network_outgoing: NetworkOutgoingHandle,
    ) -> Self {
        Self {
            endpoint,
            consensus,
            network_outgoing,
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
                .network_outgoing
                .new_connection(ClientId::from_le_bytes(client_id), tx_outgoing)
                .await;
            tokio::spawn(Self::run_connection_incoming(
                conn.clone(),
                self.consensus.clone(),
            ));
            tokio::spawn(Self::run_connection_outgoing(conn, rx_outgoing));
        }
        Ok(())
    }

    async fn run_connection_incoming(
        conn: Connection,
        consensus: ConsensusHandle,
    ) -> anyhow::Result<()> {
        loop {
            let mut recv = conn.accept_uni().await?;
            let bytes = recv.read_to_end(usize::MAX).await?;
            let message = bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;
            let _ = consensus.submit_request(message).await;
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

struct NetworkOutgoingChannels {
    tx_connection: Sender<(ClientId, UnboundedSender<Bytes>)>,
    rx_connection: Receiver<(ClientId, UnboundedSender<Bytes>)>,

    tx_outgoing_message: UnboundedSender<(ClientId, Reply)>,
    rx_outgoing_message: UnboundedReceiver<(ClientId, Reply)>,
}

struct NetworkOutgoingHandle {
    tx_connection: Sender<(ClientId, UnboundedSender<Bytes>)>,
    tx_outgoing_message: UnboundedSender<(ClientId, Reply)>,
}

impl NetworkOutgoingChannels {
    fn new() -> Self {
        let (tx_connection, rx_connection) = channel(100);
        let (tx_outgoing_message, rx_outgoing_message) = unbounded_channel();
        Self {
            tx_connection,
            rx_connection,
            tx_outgoing_message,
            rx_outgoing_message,
        }
    }

    fn handle(&self) -> NetworkOutgoingHandle {
        NetworkOutgoingHandle {
            tx_connection: self.tx_connection.clone(),
            tx_outgoing_message: self.tx_outgoing_message.clone(),
        }
    }
}

impl NetworkOutgoingHandle {
    async fn new_connection(
        &self,
        id: ClientId,
        conn: UnboundedSender<Bytes>,
    ) -> anyhow::Result<()> {
        self.tx_connection.send((id, conn)).await?;
        anyhow::Ok(())
    }

    fn send_message(&self, id: ClientId, reply: Reply) -> anyhow::Result<()> {
        self.tx_outgoing_message.send((id, reply))?;
        anyhow::Ok(())
    }
}

pub struct NetworkOutgoingTask {
    channels: NetworkOutgoingChannels,
    connections: HashMap<ClientId, UnboundedSender<Bytes>>,
}

impl NetworkOutgoingTask {
    fn new(channels: NetworkOutgoingChannels) -> Self {
        Self {
            channels,
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
                Some((id, conn)) = self.channels.rx_connection.recv() => {
                    self.handle_connection(id, conn);
                }
                Some((id, reply)) = self.channels.rx_outgoing_message.recv() => {
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

pub struct NetworkConnectHandle {
    txs_outgoing_message: HashMap<NodeIndex, UnboundedSender<Bytes>>,
}

impl NetworkConnectHandle {
    fn send(&self, node_index: NodeIndex, message: crate::consensus::message::Message) {
        let bytes = bincode::encode_to_vec(&message, bincode::config::standard()).unwrap();
        let _ = self.txs_outgoing_message[&node_index].send(bytes.into());
    }

    fn send_to_all(&self, message: crate::consensus::message::Message) {
        let bytes =
            Bytes::from(bincode::encode_to_vec(&message, bincode::config::standard()).unwrap());
        for tx in self.txs_outgoing_message.values() {
            let _ = tx.send(bytes.clone());
        }
    }
}

impl NetworkConnectTask {
    async fn load(
        schema: &schema::ReplicaTask,
        consensus: ConsensusHandle,
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
                    consensus.clone(),
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
                    consensus.clone(),
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

    fn handle(&self) -> NetworkConnectHandle {
        NetworkConnectHandle {
            txs_outgoing_message: self.txs_outgoing_message.clone(),
        }
    }

    async fn run_connection_incoming(
        conn: Connection,
        consensus: ConsensusHandle,
    ) -> anyhow::Result<()> {
        loop {
            let mut recv = conn.accept_uni().await?;
            let bytes = recv.read_to_end(usize::MAX).await?;
            let message = bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;
            let _ = consensus.incoming_message(message).await;
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
    network_accept: NetworkAcceptTask,
    network_outgoing: NetworkOutgoingTask,
    network_connect: NetworkConnectTask,
    consensus: ConsensusTask,
    execute: ExecuteTask,
    storage: StorageTask,
    _temp_dir: TempDir,
}

impl ReplicaNodeTask {
    pub async fn load(schema: schema::ReplicaTask) -> anyhow::Result<Self> {
        let network_outgoing_channels = NetworkOutgoingChannels::new();
        let consensus_channels = ConsensusChannels::new();
        let execute_channels = ExecuteChannels::new();
        let storage_channels = StorageChannels::new();

        let network_accept = NetworkAcceptTask::new(
            Endpoint::server(server_config(), ([0, 0, 0, 0], 5000).into())?,
            consensus_channels.handle(),
            network_outgoing_channels.handle(),
        );
        let network_outgoing = NetworkOutgoingTask::new(network_outgoing_channels);
        let network_connect =
            NetworkConnectTask::load(&schema, consensus_channels.handle()).await?;
        let consensus_context = ConsensusTaskContext::new(
            consensus_channels.handle(),
            execute_channels.handle(),
            network_connect.handle(),
        );
        let consensus = ConsensusTask::new(
            consensus_channels,
            Bullshark::new(
                consensus_context,
                (&schema.config).into(),
                schema.node_index,
            ),
        );
        let execute_context = ExecuteTaskContext::new(
            execute_channels.handle(),
            storage_channels.handle(),
            network_outgoing.channels.handle(),
        );
        let execute = ExecuteTask::new(
            execute_channels,
            Execute::new(execute_context, schema.node_index),
        );
        let temp_dir = tempdir()?;
        let status = Command::new("cp")
            .arg("-rT")
            .arg(PREFILL_PATH)
            .arg(temp_dir.path())
            .status()
            .await?;
        anyhow::ensure!(status.success(), "failed to copy prefill data");
        let storage = StorageTask::new(storage_channels, Storage::new(temp_dir.path())?);
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
