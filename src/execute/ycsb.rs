use std::ops::Range;

use bincode::{Decode, Encode};
use hashbrown::HashMap;

use super::{AbstractExecute, AbstractOp};

pub const VALUE_SIZE: usize = 100;

#[derive(Encode, Decode)]
pub enum YcsbOp {
    Put(String, usize, Vec<u8>),
    Get(String),
}

#[derive(Encode, Decode)]
pub enum YcsbRes {
    Put,
    Get(Vec<Vec<u8>>),
}

pub fn key(index: u64) -> String {
    format!("key-{index:012}")
}

impl AbstractOp for YcsbOp {
    fn read_set(&self) -> impl IntoIterator<Item = Vec<u8>> {
        match self {
            YcsbOp::Put(key, _, _) | YcsbOp::Get(key) => Some(key.clone().into_bytes()),
        }
    }
}

pub struct YcsbExecute {
    key_range: Range<String>,
}

impl YcsbExecute {
    pub fn new(num_shards: u8, shard_index: u8, num_keys: u64) -> Self {
        assert!(shard_index <= num_shards);
        if shard_index == num_shards {
            return Self {
                key_range: "".to_string().."".to_string(),
            };
        }
        let keys_per_shard = num_keys / num_shards as u64;
        let start_index = shard_index as u64 * keys_per_shard;
        let end_index = if shard_index == num_shards - 1 {
            num_keys
        } else {
            start_index + keys_per_shard
        };
        Self {
            key_range: key(start_index)..key(end_index),
        }
    }
}

impl AbstractExecute for YcsbExecute {
    type Op = YcsbOp;
    type Res = YcsbRes;

    fn should_execute(&self, op: &Self::Op) -> bool {
        match op {
            YcsbOp::Put(key, _, _) | YcsbOp::Get(key) => self.key_range.contains(key),
        }
    }

    fn execute(
        &mut self,
        op: Self::Op,
        state: &HashMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> (
        Self::Res,
        impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)> + use<> + Clone,
    ) {
        match op {
            YcsbOp::Put(key, index, field_value) => {
                let mut value_bytes = state[key.as_bytes()].clone().expect("key not found");
                value_bytes[index * VALUE_SIZE..(index + 1) * VALUE_SIZE]
                    .copy_from_slice(&field_value);
                (YcsbRes::Put, Some((key.into_bytes(), Some(value_bytes))))
            }
            YcsbOp::Get(key) => {
                let value_bytes = state[key.as_bytes()].clone().expect("key not found");
                // let value = vec![0; 100 - 16];
                let values = value_bytes
                    .chunks_exact(VALUE_SIZE)
                    .map(|chunk| chunk.to_vec())
                    .collect();
                (YcsbRes::Get(values), None)
            }
        }
    }
}
