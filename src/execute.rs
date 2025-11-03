use std::{collections::VecDeque, time::Instant};

use bincode::{Decode, Encode};
use log::info;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    consensus::Block,
    metrics::Latency,
    storage::FetchResponse,
    tasks::{RequestId, ResponseContext},
    types::{ClientId, ClientSeq, NodeIndex, Reply},
};

#[derive(Encode, Decode)]
pub enum Op {
    Put(String, Vec<u8>),
    Get(String),
}

#[derive(Encode, Decode)]
pub enum Res {
    Put,
    Get(Vec<u8>),
}

pub fn key(index: u64) -> String {
    format!("key-{index:012}")
}

pub const VALUE_SIZE: usize = 100 - 16;

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
}

impl From<&crate::schema::ReplicaConfig> for ExecuteConfig {
    fn from(config: &crate::schema::ReplicaConfig) -> Self {
        Self {
            num_faulty_nodes: config.num_faulty_nodes,
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

struct FetchingState {
    requests: Vec<(Op, ClientId, ClientSeq)>,
    fetch_id: FetchId,

    start: Instant,
}

struct WillFetchState {
    requests: Vec<(Op, ClientId, ClientSeq)>,
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

    const NUM_MAX_CONCURRENT_FETCHES: usize = 1;
}

impl<C: ExecuteContext> Execute<C> {
    pub fn on_blocks(&mut self, blocks: Vec<Block>, context: ResponseContext<()>) {
        self.prepare_blocks(blocks, context);
    }

    fn prepare_blocks(&mut self, blocks: Vec<Block>, context: ResponseContext<()>) {
        let start = Instant::now();

        let mut working = WillFetchState {
            requests: Default::default(),
            fetch_keys: Default::default(),
            context,
        };
        let mut fetching_keys = FxHashSet::default();
        for block in blocks {
            for request in block.txns {
                let op = bincode::decode_from_slice(&request.command, bincode::config::standard())
                    .unwrap()
                    .0;
                if let Op::Get(key) = &op {
                    fetching_keys.insert(key.as_bytes().to_vec());
                }
                working
                    .requests
                    .push((op, request.client_id, request.client_seq));
            }
        }
        // besides performance optimization, this also prevents no-op fetches (and posts) to pollute
        // metrics of execution and storage
        if working.requests.is_empty() {
            working.context.respond(());
            return;
        }

        working.fetch_keys = fetching_keys;

        self.metrics.prepare_time += start.elapsed();

        if self.fetching.len() < Self::NUM_MAX_CONCURRENT_FETCHES {
            self.fetch_for_blocks(working);
        } else {
            self.pending_blocks.push_back(working);
        }
    }

    fn fetch_for_blocks(&mut self, working: WillFetchState) {
        assert!(self.fetching.len() < Self::NUM_MAX_CONCURRENT_FETCHES);
        let fetch_id = self.context.fetch(working.fetch_keys);
        self.fetching.push_back(FetchingState {
            requests: working.requests,
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

        if let Some(state) = self.pending_blocks.pop_front() {
            self.fetch_for_blocks(state);
        }
        self.execute_blocks(working, response);
    }

    fn execute_blocks(&mut self, working: FetchingState, response: FetchResponse) {
        let start = Instant::now();

        let mut updates = FxHashMap::default();
        for (op, client_id, client_seq) in working.requests {
            let op = match op {
                Op::Put(key, value) => {
                    updates.insert(key, value.clone());
                    Res::Put
                }
                Op::Get(key) => {
                    let value = updates
                        .get(&key)
                        .or_else(|| response.get(key.as_bytes()).as_ref());
                    let Some(value) = value else {
                        panic!("key not found");
                    };
                    Res::Get(value.clone())
                    // Res::Get(vec![0; 68])
                }
            };
            let reply = Reply {
                client_seq,
                res: bincode::encode_to_vec(&op, bincode::config::standard()).unwrap(),
                node_index: self.index,
            };

            if (self.executed_count
                ..self.executed_count + (self.config.num_faulty_nodes + 1) as u64)
                .any(|i| {
                    (i % (self.config.num_faulty_nodes as u64 * 2 + 1)) as NodeIndex == self.index
                })
            {
                self.context.send(client_id, reply);
            }
            self.executed_count += 1;
        }

        self.metrics.execute_time += start.elapsed();

        self.context.post(
            updates
                .into_iter()
                .map(|(k, v)| (k.into_bytes(), Some(v)))
                .collect(),
        );
    }
}
