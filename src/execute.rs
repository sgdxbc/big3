use sha2::{Digest as _, Sha256};

use crate::types::{ClientId, NodeIndex, Reply, Request};

pub enum Op {
    Put(String, Vec<u8>),
    Get(String),
}

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

pub trait ExecuteContext {
    fn send(&mut self, id: ClientId, reply: Reply);
}

pub struct Execute<C> {
    context: C,
    index: NodeIndex,
}

impl<C> Execute<C> {
    pub fn new(context: C, index: NodeIndex) -> Self {
        Self { context, index }
    }
}

impl<C: ExecuteContext> Execute<C> {
    pub fn on_requests(&mut self, requests: Vec<Request>) {
        for request in requests {
            let reply = Reply {
                client_seq: request.client_seq,
                res: request.command, // echo
                node_index: self.index,
            };
            self.context.send(request.client_id, reply);
        }
    }
}
