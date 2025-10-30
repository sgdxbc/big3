use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

use bincode::{Decode, Encode};
use log::info;
use tokio::sync::oneshot;

use crate::{
    consensus::Block,
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
    pending_blocks: VecDeque<(Vec<Block>, oneshot::Sender<()>)>,
    executed_count: u64,

    metrics: ExecuteMetrics,
}

struct WorkingState {
    requests: Vec<(Op, ClientId, ClientSeq)>,
    fetch_id: FetchId,
    fetching: Vec<String>,
    tx_response: oneshot::Sender<()>,
}

struct ExecuteMetrics {
    start: Instant,
    work_time: Duration,
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
                start: Instant::now(),
                work_time: Duration::ZERO,
            },
        }
    }
}

impl<C: ExecuteContext> Execute<C> {
    pub fn on_block(&mut self, blocks: Vec<Block>, tx_response: oneshot::Sender<()>) {
        if self.working.is_some() {
            self.pending_blocks.push_back((blocks, tx_response));
            return;
        }
        self.prepare_blocks(blocks, tx_response);
    }

    pub fn log_metrics(&self) {
        info!("execution work time: {:?}", self.metrics.work_time);
    }

    fn prepare_blocks(&mut self, blocks: Vec<Block>, tx_response: oneshot::Sender<()>) {
        self.metrics.start = Instant::now();
        assert!(self.working.is_none());

        let mut working = WorkingState {
            requests: Default::default(),
            fetch_id: 0,
            fetching: Default::default(),
            tx_response,
        };
        let mut fetching_keys = HashSet::new();
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
    }

    pub fn on_fetch_response(&mut self, fetch_id: FetchId, values: Vec<Option<Vec<u8>>>) {
        let Some(working) = self.working.take() else {
            return;
        };
        assert_eq!(working.fetch_id, fetch_id);
        self.commit_blocks(working, values);
    }

    fn commit_blocks(&mut self, working: WorkingState, values: Vec<Option<Vec<u8>>>) {
        let mut state: HashMap<String, Option<Vec<u8>>> = working
            .fetching
            .into_iter()
            .zip(values)
            .collect::<HashMap<_, _>>();
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
        let _ = working.tx_response.send(());
        self.metrics.work_time += self.metrics.start.elapsed();

        if let Some((block, tx_response)) = self.pending_blocks.pop_front() {
            self.prepare_blocks(block, tx_response);
        }
    }
}
