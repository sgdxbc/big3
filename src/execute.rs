use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use bincode::{Decode, Encode};
use log::info;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    consensus::Block,
    tasks::ResponseContext,
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

pub type FetchId = u64;

pub trait ExecuteContext {
    // network
    fn send(&mut self, id: ClientId, reply: Reply);
    // storage
    fn fetch(&mut self, keys: Vec<Vec<u8>>) -> FetchId;
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

    working: Option<WorkingState>,
    pending_blocks: VecDeque<(Vec<Block>, ResponseContext<()>)>,
    executed_count: u64,

    metrics: ExecuteMetrics,
}

struct WorkingState {
    requests: Vec<(Op, ClientId, ClientSeq)>,
    fetch_id: FetchId,
    fetching: Vec<String>,
    context: ResponseContext<()>,
}

struct ExecuteMetrics {
    work_time: Duration,
    fetch_start: Instant,
    fetch_time: Duration,
}

impl<C> Execute<C> {
    pub fn new(context: C, config: ExecuteConfig, index: NodeIndex) -> Self {
        Self {
            context,
            config,
            index,

            working: None,
            pending_blocks: Default::default(),
            executed_count: 0,

            metrics: ExecuteMetrics {
                work_time: Duration::ZERO,
                fetch_start: Instant::now(),
                fetch_time: Duration::ZERO,
            },
        }
    }
}

impl<C: ExecuteContext> Execute<C> {
    pub fn on_block(&mut self, blocks: Vec<Block>, context: ResponseContext<()>) {
        if self.working.is_some() {
            self.pending_blocks.push_back((blocks, context));
            return;
        }
        self.prepare_blocks(blocks, context);
    }

    pub fn log_metrics(&self) {
        info!(
            "execution work time: {:?}, fetch time: {:?}",
            self.metrics.work_time, self.metrics.fetch_time
        );
    }

    fn prepare_blocks(&mut self, blocks: Vec<Block>, context: ResponseContext<()>) {
        assert!(self.working.is_none());
        let start = Instant::now();

        let mut working = WorkingState {
            requests: Default::default(),
            fetch_id: 0,
            fetching: Default::default(),
            context,
        };
        let mut fetching_keys = FxHashSet::default();
        for block in blocks {
            for request in block.txns {
                let op = bincode::decode_from_slice(&request.command, bincode::config::standard())
                    .unwrap()
                    .0;
                if let Op::Get(key) = &op {
                    fetching_keys.insert(key.clone());
                }
                working
                    .requests
                    .push((op, request.client_id, request.client_seq));
            }
        }

        working.fetching = fetching_keys.into_iter().collect();
        working.fetching.sort_unstable();
        let keys = working
            .fetching
            .iter()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        working.fetch_id = self.context.fetch(keys);
        let replaced = self.working.replace(working);
        assert!(replaced.is_none());
        // self.commit_blocks(working, Default::default())

        self.metrics.work_time += start.elapsed();
        self.metrics.fetch_start = Instant::now();
    }

    pub fn on_fetch_response(&mut self, fetch_id: FetchId, values: Vec<Option<Vec<u8>>>) {
        let Some(working) = self.working.take() else {
            return;
        };
        assert_eq!(working.fetch_id, fetch_id);
        self.metrics.fetch_time += self.metrics.fetch_start.elapsed();
        self.commit_blocks(working, values);
    }

    fn commit_blocks(&mut self, working: WorkingState, values: Vec<Option<Vec<u8>>>) {
        let start = Instant::now();

        let mut state = working
            .fetching
            .into_iter()
            .zip(values)
            .collect::<FxHashMap<_, _>>();
        let mut updates = Vec::new();
        for (op, client_id, client_seq) in working.requests {
            let op = match op {
                Op::Put(key, value) => {
                    updates.push((key.as_bytes().to_vec(), Some(value.clone())));
                    state.insert(key, Some(value));
                    Res::Put
                }
                Op::Get(key) => {
                    let Some(value) = &state[&key] else {
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

            if (self.executed_count..self.executed_count + self.config.num_faulty_nodes as u64 + 1)
                .map(|i| (i % (self.config.num_faulty_nodes as u64 * 2 + 1)) as NodeIndex)
                .any(|i| i == self.index)
            {
                self.context.send(client_id, reply);
            }
            self.executed_count += 1;
        }
        self.context.post(updates);
        working.context.respond(());

        self.metrics.work_time += start.elapsed();

        if let Some((block, tx_response)) = self.pending_blocks.pop_front() {
            self.prepare_blocks(block, tx_response);
        }
    }
}
