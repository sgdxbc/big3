use std::{cmp::Reverse, collections::BinaryHeap};

use bincode::{Decode, Encode};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::{
    select,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        oneshot,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{
    consensus::Block,
    execute::{AbstractExecute, AbstractOp, Execute, ExecuteContext, FetchId},
    schema,
    storage::FetchResponse,
    types::{ClientId, ClientSeq, NodeIndex, Reply},
};

use super::{
    ResponseContext,
    network::server::NetworkOutgoingHandle,
    storage::{StorageContext, StorageHandle},
};

pub struct ExecuteSourceChannels {
    tx_blocks: UnboundedSender<Vec<Block>>,
    rx_blocks: UnboundedReceiver<Vec<Block>>,
}

pub struct ExecuteSourceHandle {
    tx_blocks: UnboundedSender<Vec<Block>>,
}

impl Default for ExecuteSourceChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecuteSourceChannels {
    pub fn new() -> Self {
        let (tx_blocks, rx_blocks) = unbounded_channel();
        Self {
            tx_blocks,
            rx_blocks,
        }
    }

    pub fn handle(&self) -> ExecuteSourceHandle {
        ExecuteSourceHandle {
            tx_blocks: self.tx_blocks.clone(),
        }
    }
}

impl ExecuteSourceHandle {
    pub fn execute(&self, blocks: Vec<Block>) {
        let _ = self.tx_blocks.send(blocks);
    }
}

pub struct ExecuteSourceTask<Op> {
    channels: ExecuteSourceChannels,
    tx_fetch: flume::Sender<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    tx_op_state: UnboundedSender<RequestState<Op>>,
}

pub struct RequestState<Op> {
    op: Op,
    read_set: FxHashMap<Vec<u8>, oneshot::Receiver<Option<Vec<u8>>>>,
    client_id: ClientId,
    client_seq: ClientSeq,
}

impl<Op> ExecuteSourceTask<Op> {
    pub fn new(
        channels: ExecuteSourceChannels,
        tx_fetch: flume::Sender<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
        tx_op_state: UnboundedSender<RequestState<Op>>,
    ) -> Self {
        Self {
            channels,
            tx_fetch,
            tx_op_state,
        }
    }
}

impl<Op: AbstractOp + Send + 'static + Decode<()>> ExecuteSourceTask<Op> {
    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move {
            stop.run_until_cancelled(self.run_inner()).await;
        })
        .await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        while let Some(blocks) = self.channels.rx_blocks.recv().await {
            for block in blocks {
                for request in block.txns {
                    let op = bincode::decode_from_slice::<Op, _>(
                        &request.command,
                        bincode::config::standard(),
                    )
                    .unwrap()
                    .0;
                    let mut read_set = FxHashMap::default();
                    for key in op.read_set() {
                        let (tx, rx) = oneshot::channel();
                        let _ = self.tx_fetch.send((key.clone(), tx));
                        read_set.insert(key, rx);
                    }
                    let op_state = RequestState {
                        op,
                        read_set,
                        client_id: request.client_id,
                        client_seq: request.client_seq,
                    };
                    let _ = self.tx_op_state.send(op_state);
                }
            }
        }
    }
}

pub struct ExecuteSchedChannels<Op> {
    tx_request_state: UnboundedSender<RequestState<Op>>,
    rx_request_state: UnboundedReceiver<RequestState<Op>>,

    tx_write_done: UnboundedSender<u64>,
    rx_write_done: UnboundedReceiver<u64>,
}

pub struct ExecuteSchedHandle<Op> {
    pub tx_request_state: UnboundedSender<RequestState<Op>>,
    pub tx_write_done: UnboundedSender<u64>,
}

impl<T> Default for ExecuteSchedChannels<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Op> ExecuteSchedChannels<Op> {
    pub fn new() -> Self {
        let (tx_request_state, rx_request_state) = unbounded_channel();
        let (tx_write_done, rx_write_done) = unbounded_channel();
        Self {
            tx_request_state,
            rx_request_state,
            tx_write_done,
            rx_write_done,
        }
    }

    pub fn handle(&self) -> ExecuteSchedHandle<Op> {
        ExecuteSchedHandle {
            tx_request_state: self.tx_request_state.clone(),
            tx_write_done: self.tx_write_done.clone(),
        }
    }
}

impl<Op> ExecuteSchedHandle<Op> {
    pub fn submit_request(&self, request: RequestState<Op>) {
        let _ = self.tx_request_state.send(request);
    }

    pub fn notify_write_done(&self, version: u64) {
        let _ = self.tx_write_done.send(version);
    }
}

pub struct ExecuteSched<E: AbstractExecute> {
    channels: ExecuteSchedChannels<E::Op>,
    tx_write: UnboundedSender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
    network_outgoing: NetworkOutgoingHandle,

    node_index: NodeIndex,

    state: E,
    recent_updates: FxHashMap<Vec<u8>, (u64, Option<Vec<u8>>)>,
    current_version: u64,
    evict_queue: BinaryHeap<Reverse<(u64, Vec<u8>)>>,
}

impl<E: AbstractExecute> ExecuteSched<E> {
    pub fn new(
        channels: ExecuteSchedChannels<E::Op>,
        tx_write: UnboundedSender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
        network_outgoing: NetworkOutgoingHandle,
        node_index: NodeIndex,
        state: E,
    ) -> Self {
        Self {
            channels,
            tx_write,
            network_outgoing,
            node_index,
            state,
            recent_updates: Default::default(),
            current_version: 0,
            evict_queue: Default::default(),
        }
    }
}

impl<E> ExecuteSched<E>
where
    E: AbstractExecute + Send + 'static,
    E::Res: Encode,
{
    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()>
    where
        E::Op: Send + 'static,
    {
        tokio::spawn(async move {
            stop.run_until_cancelled(self.run_inner()).await;
        })
        .await?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            select! {
                Some(state) = self.channels.rx_request_state.recv() => {
                    self.handle_request_state(state).await?
                }
                Some(version) = self.channels.rx_write_done.recv() => {
                    self.handle_write_done(version)
                }
            }
        }
    }

    async fn handle_request_state(
        &mut self,
        request_state: RequestState<E::Op>,
    ) -> anyhow::Result<()> {
        let mut state = FxHashMap::default();
        for (key, rx_value) in request_state.read_set {
            let value = if let Some((_, value)) = self.recent_updates.get(&key) {
                value.clone()
            } else {
                rx_value.await?
            };
            state.insert(key, value);
        }
        let (res, updates) = self.state.execute(request_state.op, state);
        let reply = Reply {
            client_seq: request_state.client_seq,
            res: bincode::encode_to_vec(&res, bincode::config::standard()).unwrap(),
            node_index: self.node_index,
        };
        let _ = self
            .network_outgoing
            .send_message(request_state.client_id, reply);

        if !updates.is_empty() {
            self.current_version += 1;
            for (key, value) in &updates {
                self.recent_updates
                    .insert(key.clone(), (self.current_version, value.clone()));
                self.evict_queue
                    .push(Reverse((self.current_version, key.clone())));
            }
            let _ = self.tx_write.send(updates);
        }
        Ok(())
    }

    fn handle_write_done(&mut self, version: u64) {
        while self
            .evict_queue
            .peek()
            .is_some_and(|&Reverse((v, _))| v <= version)
        {
            let Reverse((v, key)) = self.evict_queue.pop().unwrap();
            if self.recent_updates[&key].0 == v {
                self.recent_updates.remove(&key);
            }
        }
    }
}

pub struct ExecuteChannels {
    tx_blocks: UnboundedSender<(Vec<Block>, ResponseContext<()>)>,
    rx_blocks: UnboundedReceiver<(Vec<Block>, ResponseContext<()>)>,

    tx_fetch_response: UnboundedSender<(FetchId, FetchResponse)>,
    rx_fetch_response: UnboundedReceiver<(FetchId, FetchResponse)>,
}

#[derive(Clone)]
pub struct ExecuteHandle {
    pub tx_blocks: UnboundedSender<(Vec<Block>, ResponseContext<()>)>,
}

impl Default for ExecuteChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecuteChannels {
    pub fn new() -> Self {
        let (tx_blocks, rx_blocks) = unbounded_channel();
        let (tx_fetch_response, rx_fetch_response) = unbounded_channel();
        Self {
            tx_blocks,
            rx_blocks,
            tx_fetch_response,
            rx_fetch_response,
        }
    }

    pub fn handle(&self) -> ExecuteHandle {
        ExecuteHandle {
            tx_blocks: self.tx_blocks.clone(),
        }
    }

    fn storage_context(&self, storage: StorageHandle) -> StorageContext {
        StorageContext::new(storage, self.tx_fetch_response.clone())
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

    pub async fn load(
        channels: ExecuteChannels,
        storage: StorageHandle,
        network_outgoing: NetworkOutgoingHandle,
        schema: &schema::ReplicaTask,
    ) -> anyhow::Result<Self> {
        let context = ExecuteTaskContext::new(channels.storage_context(storage), network_outgoing);
        let state = Execute::new(context, (&schema.config).into(), schema.node_index);
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
                    self.state.on_blocks(block, tx_response);
                }
            }
        }
    }
}

struct ExecuteTaskContext {
    storage: StorageContext,
    network_outgoing: NetworkOutgoingHandle,
}

impl ExecuteTaskContext {
    fn new(storage: StorageContext, network_outgoing: NetworkOutgoingHandle) -> Self {
        Self {
            storage,
            network_outgoing,
        }
    }
}

impl ExecuteContext for ExecuteTaskContext {
    fn send(&mut self, id: ClientId, reply: Reply) {
        let _ = self.network_outgoing.send_message(id, reply);
    }

    fn fetch(&mut self, keys: FxHashSet<Vec<u8>>) -> FetchId {
        self.storage.fetch(keys)
    }

    fn post(&mut self, updates: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        self.storage.post(updates);
    }
}
