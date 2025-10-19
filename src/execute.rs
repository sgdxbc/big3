use std::{collections::VecDeque, mem::replace};

use bincode::{Decode, Encode};
use log::trace;
use sha2::{Digest as _, Sha256};

use crate::{
    consensus::Block,
    types::{ClientId, NodeIndex, Reply, Request},
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
    fn send(&mut self, id: ClientId, reply: Reply);

    fn fetch(&mut self, key: [u8; 32]) -> FetchId;
    fn post(&mut self, updates: Vec<([u8; 32], Option<Vec<u8>>)>);
}

pub struct Execute<C> {
    pub context: C,
    index: NodeIndex,

    requests: VecDeque<Request>,
    request_state: ExecuteRequestState,
}

enum ExecuteRequestState {
    Idle,
    Getting(FetchId, ClientId, u64),
}

impl<C> Execute<C> {
    pub fn new(context: C, index: NodeIndex) -> Self {
        Self {
            context,
            index,
            requests: Default::default(),
            request_state: ExecuteRequestState::Idle,
        }
    }
}

impl<C: ExecuteContext> Execute<C> {
    pub fn on_block(&mut self, block: Block) {
        trace!(
            "node {} executing block ({}, {}) size {}",
            self.index,
            block.round,
            block.node_index,
            block.txns.len()
        );
        // self.requests.extend(block.txns);
        // self.may_execute();
        for request in block.txns {
            let reply = Reply {
                client_seq: request.client_seq,
                res: bincode::encode_to_vec(&Res::Put, bincode::config::standard()).unwrap(),
                node_index: self.index,
            };
            self.context.send(request.client_id, reply);
        }
    }

    pub fn on_fetch_response(&mut self, fetch_id: FetchId, value: Option<Vec<u8>>) {
        let ExecuteRequestState::Getting(fetch_id2, client_id, client_seq) =
            replace(&mut self.request_state, ExecuteRequestState::Idle)
        else {
            panic!("unexpected fetch response");
        };
        assert_eq!(fetch_id, fetch_id2);
        let reply = Reply {
            client_seq,
            res: bincode::encode_to_vec(Res::Get(value), bincode::config::standard()).unwrap(),
            node_index: self.index,
        };
        self.context.send(client_id, reply);
        self.may_execute();
    }

    fn may_execute(&mut self) {
        if matches!(self.request_state, ExecuteRequestState::Getting(..)) {
            return;
        }
        while let Some(request) = self.requests.pop_front() {
            let op = bincode::decode_from_slice(&request.command, bincode::config::standard())
                .unwrap()
                .0;
            match op {
                Op::Put(key, value) => {
                    let storage_key = storage_key(&key);
                    self.context.post(vec![(storage_key, Some(value))]);
                    let reply = Reply {
                        client_seq: request.client_seq,
                        res: bincode::encode_to_vec(&Res::Put, bincode::config::standard())
                            .unwrap(),
                        node_index: self.index,
                    };
                    self.context.send(request.client_id, reply);
                }
                Op::Get(key) => {
                    let storage_key = storage_key(&key);
                    let id = self.context.fetch(storage_key);
                    self.request_state =
                        ExecuteRequestState::Getting(id, request.client_id, request.client_seq);
                    break;
                }
            }
        }
    }
}
