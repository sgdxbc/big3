use std::time::Instant;

use rocksdb::DB;
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
    consensus::{Block, Bullshark, BullsharkContext, OutputId},
    execute::{Execute, ExecuteContext, FetchId},
    schema,
    storage::{PlainStorage, StorageOp},
    tasks::PREFILL_PATH,
    types::{ClientId, NodeIndex, Reply, Request},
};

use super::network::{
    interconnect::{NetworkInterconnectHandle, NetworkInterconnectTask, ReceiveHandle},
    server::{
        NetworkAcceptTask, NetworkOutgoingChannels, NetworkOutgoingHandle, NetworkOutgoingTask,
        SubmitHandle,
    },
};

pub struct ConsensusChannels {
    tx_request: Sender<(Instant, Request)>,
    rx_request: Receiver<(Instant, Request)>,

    tx_incoming_message: Sender<crate::consensus::message::Message>,
    rx_incoming_message: Receiver<crate::consensus::message::Message>,

    tx_output_response: Sender<OutputId>,
    rx_output_response: Receiver<OutputId>,
}

#[derive(Clone)]
struct ConsensusHandle {
    receive: ReceiveHandle<crate::consensus::message::Message>,
    submit: SubmitHandle,
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
            receive: ReceiveHandle::new(self.tx_incoming_message.clone()),
            submit: SubmitHandle::new(self.tx_request.clone()),
            tx_output_response: self.tx_output_response.clone(),
        }
    }
}

impl ConsensusHandle {
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

    async fn load(
        channels: ConsensusChannels,
        execute: ExecuteHandle,
        network_connect: NetworkInterconnectHandle,
        schema: &schema::ReplicaTask,
    ) -> anyhow::Result<Self> {
        let context = ConsensusTaskContext::new(channels.handle(), execute, network_connect);
        let state = Bullshark::new(context, (&schema.config).into(), schema.node_index);
        Ok(Self::new(channels, state))
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
                Some((at, request)) = self.channels.rx_request.recv() => {
                    self.state.on_request(at, request);
                }
                Some(output_id) = self.channels.rx_output_response.recv() => {
                    self.state.on_output_response(output_id);
                }
            }
        }
    }
}

struct ConsensusTaskContext {
    consensus: ConsensusHandle,
    execute: ExecuteHandle,
    network_connect: NetworkInterconnectHandle,
    output_id: OutputId,
}

impl ConsensusTaskContext {
    fn new(
        consensus: ConsensusHandle,
        execute: ExecuteHandle,
        network_connect: NetworkInterconnectHandle,
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
    fn output(&mut self, blocks: Vec<Block>) -> OutputId {
        self.output_id += 1;
        let output_id = self.output_id;
        let execute = self.execute.clone();
        let consensus = self.consensus.clone();
        tokio::spawn(async move {
            execute.execute(blocks).await?;
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
    tx_blocks: Sender<(Vec<Block>, oneshot::Sender<()>)>,
    rx_blocks: Receiver<(Vec<Block>, oneshot::Sender<()>)>,

    tx_fetch_response: Sender<(FetchId, Vec<Option<Vec<u8>>>)>,
    rx_fetch_response: Receiver<(FetchId, Vec<Option<Vec<u8>>>)>,
}

#[derive(Clone)]
struct ExecuteHandle {
    tx_block: Sender<(Vec<Block>, oneshot::Sender<()>)>,
    tx_fetch_response: Sender<(FetchId, Vec<Option<Vec<u8>>>)>,
}

impl ExecuteChannels {
    fn new() -> Self {
        let (tx_block, rx_block) = channel(100);
        let (tx_fetch_response, rx_fetch_response) = channel(100);
        Self {
            tx_blocks: tx_block,
            rx_blocks: rx_block,
            tx_fetch_response,
            rx_fetch_response,
        }
    }

    fn handle(&self) -> ExecuteHandle {
        ExecuteHandle {
            tx_block: self.tx_blocks.clone(),
            tx_fetch_response: self.tx_fetch_response.clone(),
        }
    }
}

impl ExecuteHandle {
    async fn execute(&self, blocks: Vec<Block>) -> anyhow::Result<()> {
        let (tx_response, rx_response) = oneshot::channel();
        self.tx_block.send((blocks, tx_response)).await?;
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

    async fn load(
        channels: ExecuteChannels,
        storage: StorageHandle,
        network_outgoing: NetworkOutgoingHandle,
        schema: &schema::ReplicaTask,
    ) -> anyhow::Result<Self> {
        let context = ExecuteTaskContext::new(channels.handle(), storage, network_outgoing);
        let state = Execute::new(context, schema.node_index);
        Ok(Self::new(channels, state))
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
                Some((block, tx_response)) = self.channels.rx_blocks.recv() => {
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

    fn fetch(&mut self, keys: Vec<Vec<u8>>) -> FetchId {
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

    fn post(&mut self, updates: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
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
    async fn fetch(&self, keys: Vec<Vec<u8>>) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let (tx_response, rx_response) = oneshot::channel();
        self.tx_storage_op
            .send(StorageOp::Fetch(keys, tx_response))?;
        let res = rx_response.await?;
        anyhow::Ok(res)
    }

    fn post(&self, updates: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> anyhow::Result<()> {
        self.tx_storage_op.send(StorageOp::Post(updates))?;
        anyhow::Ok(())
    }
}

pub struct StorageTask {
    channels: StorageChannels,
    state: PlainStorage,
    _temp_dir: TempDir,
}

impl StorageTask {
    fn new(channels: StorageChannels, state: PlainStorage, temp_dir: TempDir) -> Self {
        Self {
            channels,
            state,
            _temp_dir: temp_dir,
        }
    }

    async fn load(channels: StorageChannels) -> anyhow::Result<Self> {
        let temp_dir = tempdir()?;
        let status = Command::new("cp")
            .arg("-rT")
            .arg(PREFILL_PATH)
            .arg(temp_dir.path())
            .status()
            .await?;
        anyhow::ensure!(status.success(), "failed to copy prefill data");
        let db = DB::open_default(temp_dir.path())?;
        // use rocksdb::Options;
        // let mut db_opts = Options::default();
        // db_opts.create_if_missing(true);
        // // Explicitly include the "default" CF so we get a handle for it
        // let default_cf_opts = Options::default();
        // let cfs = vec![rocksdb::ColumnFamilyDescriptor::new(
        //     "default",
        //     default_cf_opts,
        // )];
        // let db = DB::open_cf_descriptors(&db_opts, temp_dir.path(), cfs)?;

        let state = PlainStorage::new(db)?;
        Ok(Self::new(channels, state, temp_dir))
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move {
            stop.run_until_cancelled(self.run_inner()).await;
            self.state.log_metrics();
        })
        .await?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(op) = self.channels.rx_storage_op.recv().await {
            self.state.invoke(op)?;
        }
        Ok(())
    }
}

pub struct ReplicaNodeTask {
    network_accept: NetworkAcceptTask,
    network_outgoing: NetworkOutgoingTask,
    network_connect: NetworkInterconnectTask,
    consensus: ConsensusTask,
    execute: ExecuteTask,
    storage: StorageTask,
}

impl ReplicaNodeTask {
    pub async fn load(schema: schema::ReplicaTask) -> anyhow::Result<Self> {
        let network_outgoing_channels = NetworkOutgoingChannels::new();
        let consensus_channels = ConsensusChannels::new();
        let execute_channels = ExecuteChannels::new();
        let storage_channels = StorageChannels::new();

        let network_accept = NetworkAcceptTask::load(
            consensus_channels.handle().submit,
            network_outgoing_channels.handle(),
        )
        .await?;
        let network_outgoing = NetworkOutgoingTask::load(network_outgoing_channels).await?;
        let network_connect =
            NetworkInterconnectTask::load(consensus_channels.handle().receive, &schema, 5001)
                .await?;
        let consensus = ConsensusTask::load(
            consensus_channels,
            execute_channels.handle(),
            network_connect.handle(),
            &schema,
        )
        .await?;
        let execute = ExecuteTask::load(
            execute_channels,
            storage_channels.handle(),
            network_outgoing.channels.handle(),
            &schema,
        )
        .await?;
        let storage = StorageTask::load(storage_channels).await?;
        Ok(Self {
            network_outgoing,
            network_accept,
            network_connect,
            execute,
            storage,
            consensus,
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
