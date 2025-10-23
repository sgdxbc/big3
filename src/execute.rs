use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

use bincode::{Decode, Encode};
use log::{info, trace};
use sha2::{Digest as _, Sha256};
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
    Get(Option<Vec<u8>>),
}

pub fn key(index: u64) -> String {
    format!("key-{index:08}")
}

pub fn storage_key(key: &str) -> [u8; 32] {
    Sha256::digest(key).into()
}

pub type FetchId = u64;

pub trait ExecuteContext {
    // network
    fn send(&mut self, id: ClientId, reply: Reply);
    // storage
    fn fetch(&mut self, keys: Vec<[u8; 32]>) -> FetchId;
    fn post(&mut self, updates: Vec<([u8; 32], Option<Vec<u8>>)>);
}

pub struct Execute<C> {
    pub context: C,
    index: NodeIndex,

    working: Option<WorkingState>,
    pending_blocks: VecDeque<(Block, oneshot::Sender<()>)>,

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
    pub fn new(context: C, index: NodeIndex) -> Self {
        Self {
            context,
            index,

            working: None,
            pending_blocks: Default::default(),
            metrics: ExecuteMetrics {
                start: Instant::now(),
                work_time: Duration::ZERO,
            },
        }
    }
}

impl<C: ExecuteContext> Execute<C> {
    pub fn on_block(&mut self, block: Block, tx_response: oneshot::Sender<()>) {
        trace!(
            "node {} executing block ({}, {}) size {}",
            self.index,
            block.round,
            block.node_index,
            block.txns.len()
        );
        if block.txns.is_empty() {
            let _ = tx_response.send(());
            return;
        }

        if self.working.is_some() {
            self.pending_blocks.push_back((block, tx_response));
            return;
        }
        self.prepare_block(block, tx_response);
    }

    pub fn log_metrics(&self) {
        info!("execution work time: {:?}", self.metrics.work_time);
    }

    fn prepare_block(&mut self, block: Block, tx_response: oneshot::Sender<()>) {
        self.metrics.start = Instant::now();
        assert!(self.working.is_none());

        let mut working = WorkingState {
            requests: Default::default(),
            fetch_id: 0,
            fetching: Default::default(),
            tx_response,
        };
        let mut fetching_keys = HashSet::new();
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

        // if fetching_keys.is_empty() {
        //     self.commit_block(working, Default::default());
        //     return;
        // }
        let mut keys = Vec::new();
        for key in fetching_keys {
            keys.push(storage_key(&key));
            working.fetching.push(key);
        }
        working.fetch_id = self.context.fetch(keys);
        let replaced = self.working.replace(working);
        assert!(replaced.is_none());
    }

    pub fn on_fetch_response(&mut self, fetch_id: FetchId, values: Vec<Option<Vec<u8>>>) {
        let Some(working) = self.working.take() else {
            return;
        };
        assert_eq!(working.fetch_id, fetch_id);
        self.commit_block(working, values);
    }

    fn commit_block(&mut self, working: WorkingState, values: Vec<Option<Vec<u8>>>) {
        let mut state: HashMap<String, Option<Vec<u8>>> = working
            .fetching
            .into_iter()
            .zip(values)
            .collect::<HashMap<_, _>>();
        let mut updates = Vec::new();
        for (op, client_id, client_seq) in working.requests {
            let op = match op {
                Op::Put(key, value) => {
                    let storage_key = storage_key(&key);
                    updates.push((storage_key, Some(value.clone())));
                    state.insert(key, Some(value));
                    Res::Put
                }
                Op::Get(key) => {
                    let value = state[&key].clone();
                    Res::Get(value)
                }
            };
            let reply = Reply {
                client_seq,
                res: bincode::encode_to_vec(&op, bincode::config::standard()).unwrap(),
                node_index: self.index,
            };
            self.context.send(client_id, reply);
        }
        self.context.post(updates);
        let _ = working.tx_response.send(());
        self.metrics.work_time += self.metrics.start.elapsed();

        if let Some((block, tx_response)) = self.pending_blocks.pop_front() {
            self.prepare_block(block, tx_response);
        }
    }
}
