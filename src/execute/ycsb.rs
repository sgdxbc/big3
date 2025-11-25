use bincode::{Decode, Encode};
use hashbrown::HashMap;

use crate::schema;

use super::{AbstractExecute, AbstractOp};

#[derive(Encode, Decode)]
pub enum YcsbOp {
    Put(String, Vec<u8>),
    Get(String),
}

#[derive(Encode, Decode)]
pub enum YcsbRes {
    Put,
    Get(Vec<u8>),
}

impl AbstractOp for YcsbOp {
    fn read_set(&self) -> impl IntoIterator<Item = Vec<u8>> {
        match self {
            YcsbOp::Put(_, _) => None,
            YcsbOp::Get(key) => Some(key.as_bytes().to_vec()),
        }
    }
}

pub struct YcsbConfig {
    num_keys: u64,
    num_shards: schema::ShardIndex,
}

impl YcsbConfig {
    pub fn new(num_keys: u64, num_shards: schema::ShardIndex) -> Self {
        Self {
            num_keys,
            num_shards,
        }
    }

    pub fn shard_of(&self, index: u64) -> schema::ShardIndex {
        (((index as u128 + 1) * self.num_shards as u128) / (self.num_keys as u128 + 1)) as _
    }
}

pub struct YcsbExecute;

impl AbstractExecute for YcsbExecute {
    type Op = YcsbOp;
    type Res = YcsbRes;

    fn execute(
        &mut self,
        op: Self::Op,
        state: &HashMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> (
        Self::Res,
        impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)> + use<> + Clone,
    ) {
        match op {
            YcsbOp::Put(key, value) => (YcsbRes::Put, Some((key.as_bytes().to_vec(), Some(value)))),
            YcsbOp::Get(key) => {
                let value = state[key.as_bytes()].clone().expect("key not found");
                // let value = vec![0; 100 - 16];
                (YcsbRes::Get(value), None)
            }
        }
    }
}

pub fn key(index: u64) -> String {
    format!("key-{index:012}")
}

pub const VALUE_SIZE: usize = 100 - 16;
