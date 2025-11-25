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

impl AbstractOp for YcsbOp {
    fn read_set(&self) -> impl IntoIterator<Item = Vec<u8>> {
        match self {
            YcsbOp::Put(key, _, _) | YcsbOp::Get(key) => Some(key.clone().into_bytes()),
        }
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

pub fn key(index: u64) -> String {
    format!("key-{index:012}")
}
