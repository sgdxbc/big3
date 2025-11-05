use std::{collections::VecDeque, time::Instant};

use log::info;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    consensus::Block,
    metrics::Latency,
    storage::FetchResponse,
    tasks::{RequestId, ResponseContext},
    types::{ClientId, NodeIndex, Reply},
};

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
        state: FxHashMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> (Self::Res, Vec<(Vec<u8>, Option<Vec<u8>>)>);
}

pub type FetchId = RequestId;

pub trait ExecuteContext {
    // network
    fn send(&mut self, id: ClientId, reply: Reply);
    // storage
    fn fetch(&mut self, keys: FxHashSet<Vec<u8>>) -> FetchId;
    fn post(&mut self, updates: Vec<(Vec<u8>, Option<Vec<u8>>)>);
}

pub struct ExecuteConfig {
    num_faulty_nodes: NodeIndex,
    num_concurrent_fetches: usize,
}

impl From<&crate::schema::ReplicaConfig> for ExecuteConfig {
    fn from(config: &crate::schema::ReplicaConfig) -> Self {
        Self {
            num_faulty_nodes: config.num_faulty_nodes,
            num_concurrent_fetches: 0,
        }
    }
}

pub struct Execute<C> {
    pub context: C,
    config: ExecuteConfig,
    index: NodeIndex,

    pending_blocks: VecDeque<WillFetchState>,
    fetching: VecDeque<FetchingState>,
    executed_count: u64,

    metrics: ExecuteMetrics,
}

enum BlocksExecuteState {
    Ycsb(ycsb::BlocksExecuteState),
}

impl BlocksExecuteState {
    fn prepare(blocks: &[Block]) -> (Self, FxHashSet<Vec<u8>>) {
        let (state, keys) = ycsb::BlocksExecuteState::prepare(blocks);
        (BlocksExecuteState::Ycsb(state), keys)
    }

    fn is_empty(&self) -> bool {
        match self {
            BlocksExecuteState::Ycsb(state) => state.is_empty(),
        }
    }

    fn commit(
        self,
        state: FetchResponse,
        node_index: NodeIndex,
        send: impl FnMut(ClientId, Reply),
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        match self {
            BlocksExecuteState::Ycsb(s) => s.commit(state, node_index, send),
        }
    }
}

struct FetchingState {
    execute: BlocksExecuteState,
    fetch_id: FetchId,

    start: Instant,
}

struct WillFetchState {
    execute: BlocksExecuteState,
    fetch_keys: FxHashSet<Vec<u8>>,
    context: ResponseContext<()>,
}

#[derive(Default)]
struct ExecuteMetrics {
    prepare_time: Latency,
    execute_time: Latency,
    fetch_time: Latency,
}

impl<C> Execute<C> {
    pub fn new(context: C, config: ExecuteConfig, index: NodeIndex) -> Self {
        Self {
            context,
            config,
            index,

            pending_blocks: Default::default(),
            fetching: Default::default(),
            executed_count: 0,

            metrics: Default::default(),
        }
    }

    pub fn log_metrics(&self) {
        info!(
            "\nprepare time: {}\nexecute time: {}\nfetch time: {}",
            self.metrics.prepare_time, self.metrics.execute_time, self.metrics.fetch_time,
        );
    }
}

impl<C: ExecuteContext> Execute<C> {
    pub fn on_blocks(&mut self, blocks: Vec<Block>, context: ResponseContext<()>) {
        self.prepare_blocks(blocks, context);
    }

    fn prepare_blocks(&mut self, blocks: Vec<Block>, context: ResponseContext<()>) {
        let start = Instant::now();

        let (execute, fetch_keys) = BlocksExecuteState::prepare(&blocks);
        let working = WillFetchState {
            execute,
            fetch_keys,
            context,
        };
        // besides performance optimization, this also prevents no-op fetches (and posts) to pollute
        // metrics of execution and storage
        if working.execute.is_empty() {
            working.context.respond(());
            return;
        }

        self.metrics.prepare_time += start.elapsed();

        if self.fetching.len() < self.config.num_concurrent_fetches.max(1) {
            self.fetch_for_blocks(working);
        } else {
            self.pending_blocks.push_back(working);
        }
    }

    fn fetch_for_blocks(&mut self, working: WillFetchState) {
        assert!(self.fetching.len() < self.config.num_concurrent_fetches.max(1));
        let fetch_id = self.context.fetch(working.fetch_keys);
        self.fetching.push_back(FetchingState {
            execute: working.execute,
            fetch_id,
            start: Instant::now(),
        });
        working.context.respond(());
    }

    pub fn on_fetch_response(&mut self, fetch_id: FetchId, response: FetchResponse) {
        let Some(working) = self.fetching.pop_front() else {
            unimplemented!()
        };
        assert_eq!(working.fetch_id, fetch_id);
        self.metrics.fetch_time += working.start.elapsed();

        if self.config.num_concurrent_fetches > 0
            && let Some(state) = self.pending_blocks.pop_front()
        {
            self.fetch_for_blocks(state);
        }
        self.execute_blocks(working, response);
    }

    fn execute_blocks(&mut self, working: FetchingState, response: FetchResponse) {
        let start = Instant::now();

        let send = |client_id, reply| {
            if (self.executed_count
                ..self.executed_count + (self.config.num_faulty_nodes + 1) as u64)
                .any(|i| {
                    (i % (self.config.num_faulty_nodes as u64 * 2 + 1)) as NodeIndex == self.index
                })
            {
                self.context.send(client_id, reply);
            }
            self.executed_count += 1;
        };
        let updates = working.execute.commit(response, self.index, send);
        self.context.post(updates);

        self.metrics.execute_time += start.elapsed();

        if self.config.num_concurrent_fetches == 0
            && let Some(state) = self.pending_blocks.pop_front()
        {
            self.fetch_for_blocks(state);
        }
    }
}
