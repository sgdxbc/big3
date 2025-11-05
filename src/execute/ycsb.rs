use bincode::{Decode, Encode};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    consensus::Block,
    storage::FetchResponse,
    types::{ClientId, ClientSeq, NodeIndex, Reply},
};

use super::{AbstractExecute, AbstractOp};

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

impl AbstractOp for Op {
    fn read_set(&self) -> Vec<Vec<u8>> {
        match self {
            Op::Put(_, _) => Vec::new(),
            Op::Get(key) => vec![key.as_bytes().to_vec()],
        }
    }
}

pub struct YcsbExecute;

impl AbstractExecute for YcsbExecute {
    type Op = Op;
    type Res = Res;

    fn execute(
        &mut self,
        op: Self::Op,
        state: FxHashMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> (Self::Res, Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        match op {
            Op::Put(key, value) => (Res::Put, vec![(key.as_bytes().to_vec(), Some(value))]),
            Op::Get(key) => {
                let value = state[key.as_bytes()].clone().expect("key not found");
                (Res::Get(value), Vec::new())
            }
        }
    }
}

pub struct BlocksExecuteState {
    requests: Vec<(Op, ClientId, ClientSeq)>,
}

impl BlocksExecuteState {
    pub fn prepare(blocks: &[Block]) -> (Self, FxHashSet<Vec<u8>>) {
        let mut state = BlocksExecuteState {
            requests: Vec::new(),
        };
        let mut keys = FxHashSet::default();
        for block in blocks {
            for request in &block.txns {
                let op: Op =
                    bincode::decode_from_slice(&request.command, bincode::config::standard())
                        .expect("failed to decode op")
                        .0;
                if let Op::Get(key) = &op {
                    keys.insert(key.as_bytes().to_vec());
                }
                state
                    .requests
                    .push((op, request.client_id, request.client_seq));
            }
        }
        (state, keys)
    }

    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    pub fn commit(
        self,
        state: FetchResponse,
        node_index: NodeIndex,
        mut send: impl FnMut(ClientId, Reply),
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let mut put_state = FxHashMap::default();
        for (op, client_id, client_seq) in self.requests {
            let res = match op {
                Op::Put(key, value) => {
                    put_state.insert(key, value);
                    Res::Put
                }
                Op::Get(key) => {
                    let value = put_state
                        .get(&key)
                        .or_else(|| state.get(key.as_bytes()).as_ref());
                    Res::Get(value.unwrap().clone())
                }
            };
            let reply = Reply {
                client_seq,
                res: bincode::encode_to_vec(&res, bincode::config::standard()).unwrap(),
                node_index,
            };
            send(client_id, reply);
        }
        put_state
            .into_iter()
            .map(|(k, v)| (k.into_bytes(), Some(v)))
            .collect()
    }
}

pub fn key(index: u64) -> String {
    format!("key-{index:012}")
}

pub const VALUE_SIZE: usize = 100 - 16;
