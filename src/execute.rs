use std::{collections::VecDeque, time::Instant};

use bincode::{Decode, Encode};
use log::info;
use rustc_hash::FxHashMap;
use tokio::{
    select,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        oneshot,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::{ClientId, ClientSeq, NodeIndex, Reply, Request},
    consensus::Block,
    metrics::Latency,
    network::server::NetworkOutgoingHandle,
    schema,
};

pub mod sharded_utxo;
pub mod utxo;
pub mod ycsb;

pub trait AbstractOp {
    fn read_set(&self) -> Vec<Vec<u8>>;
}

pub trait AbstractExecute {
    type Op: AbstractOp;
    type Res;

    fn execute(
        &mut self,
        op: Self::Op,
        state: &FxHashMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> (Self::Res, Vec<(Vec<u8>, Option<Vec<u8>>)>);
}

pub struct ExecuteSourceChannels {
    tx_blocks: UnboundedSender<Vec<Block>>,
    rx_blocks: UnboundedReceiver<Vec<Block>>,

    tx_post_done: UnboundedSender<u64>,
    rx_post_done: UnboundedReceiver<u64>,
}

pub struct ExecuteSourceHandle {
    pub tx_blocks: UnboundedSender<Vec<Block>>,
    pub tx_post_done: UnboundedSender<u64>,
}

impl Default for ExecuteSourceChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecuteSourceChannels {
    pub fn new() -> Self {
        let (tx_blocks, rx_blocks) = unbounded_channel();
        let (tx_post_done, rx_post_done) = unbounded_channel();
        Self {
            tx_blocks,
            rx_blocks,
            tx_post_done,
            rx_post_done,
        }
    }

    pub fn handle(&self) -> ExecuteSourceHandle {
        ExecuteSourceHandle {
            tx_blocks: self.tx_blocks.clone(),
            tx_post_done: self.tx_post_done.clone(),
        }
    }
}

impl ExecuteSourceHandle {
    pub fn execute(&self, blocks: Vec<Block>) {
        let _ = self.tx_blocks.send(blocks);
    }
}

pub struct ExecuteConfig {
    num_faulty_nodes: NodeIndex,
    node_index: NodeIndex,
    num_max_concurrent_fetches: u32,
}

impl From<&schema::ReplicaTask> for ExecuteConfig {
    fn from(config: &schema::ReplicaTask) -> Self {
        Self {
            num_faulty_nodes: config.config.num_faulty_nodes,
            node_index: config.node_index,
            num_max_concurrent_fetches: 100_000,
        }
    }
}

pub struct ExecuteSourceTask<Op> {
    pub channels: ExecuteSourceChannels,
    tx_fetch: flume::Sender<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    sched: ExecuteSchedHandle<Op>,

    config: ExecuteConfig,

    fetch_version: u64,
    post_version: u64,
    pending_requests: VecDeque<Request>,

    metrics: ExecuteSourceMetrics,
}

struct ExecuteSourceMetrics {
    fetch: Latency,
    pending_count: u64,
}

enum ExecuteSchedEvent<Op> {
    RequestState(RequestState<Op>),
    // PostDone(u64),
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
        sched: ExecuteSchedHandle<Op>,
        config: ExecuteConfig,
    ) -> Self {
        Self {
            channels,
            tx_fetch,
            sched,
            config,
            fetch_version: 0,
            post_version: 0,
            pending_requests: Default::default(),
            metrics: ExecuteSourceMetrics {
                fetch: Latency::new(),
                pending_count: 0,
            },
        }
    }

    pub fn log_metrics(&self) {
        info!(
            "ExecuteSource Metrics:\nfetch {}\npending_count {}",
            self.metrics.fetch, self.metrics.pending_count
        )
    }
}

impl<Op: AbstractOp + Send + 'static + Decode<()>> ExecuteSourceTask<Op> {
    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move {
            stop.run_until_cancelled(self.run_inner()).await;
            self.log_metrics();
        })
        .await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        loop {
            select! {
                Some(blocks) = self.channels.rx_blocks.recv() => self.handle_blocks(blocks),
                Some(version) = self.channels.rx_post_done.recv() => self.handle_post_done(version),
            }
        }
    }

    fn handle_blocks(&mut self, blocks: Vec<Block>) {
        for block in blocks {
            for request in block.txns {
                if self.fetch_version - self.post_version
                    >= self.config.num_max_concurrent_fetches as u64
                {
                    self.metrics.pending_count += 1;
                    self.pending_requests.push_back(request);
                    continue;
                }

                self.fetch(request);
            }
        }
    }

    fn fetch(&mut self, request: Request) {
        let start = Instant::now();

        self.fetch_version += 1;

        let op = bincode::decode_from_slice::<Op, _>(&request.command, bincode::config::standard())
            .unwrap()
            .0;
        let mut read_set = FxHashMap::default();
        for key in op.read_set() {
            let (tx, rx) = oneshot::channel();
            let _ = self.tx_fetch.send((key.clone(), tx));
            // let _ = tx.send(Some(vec![0; 100 - 16]));
            read_set.insert(key, rx);
        }
        let op_state = RequestState {
            op,
            read_set,
            client_id: request.client_id,
            client_seq: request.client_seq,
        };
        self.sched.emit(ExecuteSchedEvent::RequestState(op_state));

        self.metrics.fetch += start.elapsed();
    }

    fn handle_post_done(&mut self, version: u64) {
        // self.sched.emit(ExecuteSchedEvent::PostDone(version));
        self.post_version = version;

        while self.fetch_version - self.post_version < self.config.num_max_concurrent_fetches as u64
            && let Some(request) = self.pending_requests.pop_front()
        {
            self.fetch(request);
        }
    }
}

pub struct ExecuteSchedChannels<Op> {
    tx_request_state: UnboundedSender<ExecuteSchedEvent<Op>>,
    rx_request_state: UnboundedReceiver<ExecuteSchedEvent<Op>>,
}

pub struct ExecuteSchedHandle<Op> {
    tx_request_state: UnboundedSender<ExecuteSchedEvent<Op>>,
}

impl<T> Default for ExecuteSchedChannels<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Op> ExecuteSchedChannels<Op> {
    pub fn new() -> Self {
        let (tx_request_state, rx_request_state) = unbounded_channel();
        Self {
            tx_request_state,
            rx_request_state,
        }
    }

    pub fn handle(&self) -> ExecuteSchedHandle<Op> {
        ExecuteSchedHandle {
            tx_request_state: self.tx_request_state.clone(),
        }
    }
}

impl<Op> ExecuteSchedHandle<Op> {
    fn emit(&self, event: ExecuteSchedEvent<Op>) {
        let _ = self.tx_request_state.send(event);
    }
}

pub struct ExecuteSchedTask<E: AbstractExecute> {
    channels: ExecuteSchedChannels<E::Op>,
    tx_post: UnboundedSender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
    network_outgoing: NetworkOutgoingHandle,

    config: ExecuteConfig,

    state: E,
    recent_updates: FxHashMap<Vec<u8>, (u64, Option<Vec<u8>>)>,
    current_version: u64,
    evict_queue: VecDeque<(u64, Vec<u8>)>,
    reply_flag: NodeIndex,

    metrics: ExecuteSchedMetrics,
}

struct ExecuteSchedMetrics {
    handle: Latency,
    delayed_fetch: u64,
}

impl<E: AbstractExecute> ExecuteSchedTask<E> {
    pub fn new(
        channels: ExecuteSchedChannels<E::Op>,
        tx_post: UnboundedSender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
        network_outgoing: NetworkOutgoingHandle,
        config: ExecuteConfig,
        state: E,
    ) -> Self {
        let send_count = config.node_index;
        Self {
            channels,
            tx_post,
            network_outgoing,
            config,
            state,
            recent_updates: Default::default(),
            current_version: 0,
            evict_queue: Default::default(),
            reply_flag: send_count,
            metrics: ExecuteSchedMetrics {
                handle: Latency::new(),
                delayed_fetch: 0,
            },
        }
    }

    pub fn log_metrics(&self) {
        info!(
            "ExecuteSched Metrics:\nhandle {}\ndelayed_fetch {}",
            self.metrics.handle, self.metrics.delayed_fetch
        )
    }
}

impl<E> ExecuteSchedTask<E>
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
            self.log_metrics();
        })
        .await?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(event) = self.channels.rx_request_state.recv().await {
            let start = Instant::now();
            match event {
                ExecuteSchedEvent::RequestState(request_state) => {
                    self.handle_request_state(request_state).await?;
                    self.metrics.handle += start.elapsed();
                } // ExecuteSchedEvent::PostDone(version) => self.handle_post_done(version),
            }
        }
        Ok(())
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
                if rx_value.is_empty() {
                    self.metrics.delayed_fetch += 1;
                }
                rx_value.await?
                // Some(vec![0; 100 - 16])
            };
            state.insert(key, value);
        }
        let (res, updates) = self.state.execute(request_state.op, &state);
        if self.reply_flag <= self.config.num_faulty_nodes {
            let reply = Reply {
                client_seq: request_state.client_seq,
                res: bincode::encode_to_vec(&res, bincode::config::standard()).unwrap(),
                node_index: self.config.node_index,
            };
            let _ = self
                .network_outgoing
                .send_message(request_state.client_id, reply);
        }
        self.reply_flag = (self.reply_flag + 1) % (self.config.num_faulty_nodes * 2 + 1);

        // 1-1 mapping between requests and versions is necessary for skipping
        // TODO implement skipping
        // if !updates.is_empty() {
        self.current_version += 1;
        for (key, value) in &updates {
            self.recent_updates
                .insert(key.clone(), (self.current_version, value.clone()));
            self.evict_queue
                .push_back((self.current_version, key.clone()));
        }
        let _ = self.tx_post.send(updates);
        // }

        if self.current_version > self.config.num_max_concurrent_fetches as u64 {
            self.handle_post_done(
                self.current_version - self.config.num_max_concurrent_fetches as u64,
            );
        }
        Ok(())
    }

    fn handle_post_done(&mut self, version: u64) {
        while self.evict_queue.front().is_some_and(|&(v, _)| v <= version) {
            let (v, key) = self.evict_queue.pop_front().unwrap();
            if self.recent_updates[&key].0 == v {
                self.recent_updates.remove(&key);
            }
        }
    }
}

pub enum GeneralExecuteSourceTask {
    Ycsb(ExecuteSourceTask<crate::execute::ycsb::YcsbOp>),
    Utxo(ExecuteSourceTask<crate::execute::utxo::UtxoOp>),
    ShardedUtxo(ExecuteSourceTask<crate::execute::sharded_utxo::ShardedUtxoOp>),
}

impl GeneralExecuteSourceTask {
    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        match self {
            GeneralExecuteSourceTask::Ycsb(task) => task.run(stop).await,
            GeneralExecuteSourceTask::Utxo(task) => task.run(stop).await,
            GeneralExecuteSourceTask::ShardedUtxo(task) => task.run(stop).await,
        }
    }
}

pub enum GeneralExecuteSchedTask {
    Ycsb(ExecuteSchedTask<crate::execute::ycsb::YcsbExecute>),
    Utxo(ExecuteSchedTask<crate::execute::utxo::UtxoExecute>),
    ShardedUtxo(ExecuteSchedTask<crate::execute::sharded_utxo::ShardedUtxoExecute>),
}

impl GeneralExecuteSchedTask {
    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        match self {
            GeneralExecuteSchedTask::Ycsb(task) => task.run(stop).await,
            GeneralExecuteSchedTask::Utxo(task) => task.run(stop).await,
            GeneralExecuteSchedTask::ShardedUtxo(task) => task.run(stop).await,
        }
    }
}
