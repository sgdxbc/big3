use bincode::{Decode, Encode};
use rustc_hash::FxHashMap;

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

pub fn key(index: u64) -> String {
    format!("key-{index:012}")
}

pub const VALUE_SIZE: usize = 100 - 16;
