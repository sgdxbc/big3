use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem::take,
};

use bincode::{Decode, Encode};
use log::trace;
use sha2::{Digest as _, Sha256};

use crate::{
    consensus::Block,
    types::{ClientId, ClientSeq, NodeIndex, Reply, Request},
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
    fn fetch(&mut self, key: [u8; 32]) -> FetchId;
    fn post(&mut self, updates: Vec<([u8; 32], Option<Vec<u8>>)>);
    // consensus
    fn submit(&mut self, request: Request);
}

pub struct Execute<C> {
    pub context: C,
    index: NodeIndex,

    requests: Vec<(Op, ClientId, ClientSeq)>,
    fetching: HashMap<FetchId, String>,
    state: HashMap<String, Option<Vec<u8>>>,
    updates: Vec<([u8; 32], Option<Vec<u8>>)>,

    pending_blocks: VecDeque<Block>,
}

impl<C> Execute<C> {
    pub fn new(context: C, index: NodeIndex) -> Self {
        Self {
            context,
            index,

            requests: Default::default(),
            fetching: Default::default(),
            state: Default::default(),
            updates: Default::default(),
            pending_blocks: Default::default(),
        }
    }

    // tune this according to the ordering latency. ordering latency should not exceed the execution
    // latency of a block * NUM_MAX_PENDING
    const NUM_MAX_PENDING: usize = 100;
}

impl<C: ExecuteContext> Execute<C> {
    pub fn on_request(&mut self, request: Request) {
        if self
            .pending_blocks
            .iter()
            .map(|block| block.txns.len())
            .sum::<usize>()
            < Self::NUM_MAX_PENDING
        {
            self.context.submit(request);
        } // otherwise discard the request that is over processing capacity
    }

    pub fn on_block(&mut self, block: Block) {
        trace!(
            "node {} executing block ({}, {}) size {}",
            self.index,
            block.round,
            block.node_index,
            block.txns.len()
        );
        if block.txns.is_empty() {
            return;
        }

        if !self.requests.is_empty() {
            self.pending_blocks.push_back(block);
            return;
        }
        self.prepare_block(block);
    }

    fn prepare_block(&mut self, block: Block) {
        assert!(self.requests.is_empty());
        let mut fetching_keys = HashSet::new();
        for request in block.txns {
            let op = bincode::decode_from_slice(&request.command, bincode::config::standard())
                .unwrap()
                .0;
            if let Op::Get(key) = &op
                && fetching_keys.insert(key.clone())
            {
                let fetch_id = self.context.fetch(storage_key(key));
                self.fetching.insert(fetch_id, key.clone());
            }
            self.requests
                .push((op, request.client_id, request.client_seq));
        }

        if self.fetching.is_empty() {
            self.commit_block()
        }
    }

    fn commit_block(&mut self) {
        assert!(self.fetching.is_empty());
        for (op, client_id, client_seq) in self.requests.drain(..) {
            let op = match op {
                Op::Put(key, value) => {
                    let storage_key = storage_key(&key);
                    self.updates.push((storage_key, Some(value.clone())));
                    self.state.insert(key, Some(value));
                    Res::Put
                }
                Op::Get(key) => {
                    let value = self.state[&key].clone();
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
        self.context.post(take(&mut self.updates));

        if let Some(block) = self.pending_blocks.pop_front() {
            self.prepare_block(block);
        }
    }

    pub fn on_fetch_response(&mut self, fetch_id: FetchId, value: Option<Vec<u8>>) {
        let Some(key) = self.fetching.remove(&fetch_id) else {
            unimplemented!()
        };
        self.state.insert(key, value);
        if self.fetching.is_empty() {
            self.commit_block()
        }
    }
}
