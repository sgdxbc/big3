use bincode::{Decode, Encode};
use log::warn;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{execute::utxo, schema};

use super::{
    AbstractExecute, AbstractOp,
    utxo::{OutputIndex, UtxoOp},
};

#[derive(Encode, Decode)]
pub enum ShardedUtxoOp {
    Prepare(UtxoOp),
    Commit(UtxoOp, bool),
}

#[derive(Encode, Decode)]
pub enum ShardedUtxoRes {
    Prepare(bool),
    Committed,
}

impl AbstractOp for ShardedUtxoOp {
    fn read_set(&self) -> Vec<Vec<u8>> {
        match self {
            ShardedUtxoOp::Prepare(op) => op.read_set(),
            _ => vec![],
        }
    }
}

pub fn shard_of(num_shards: schema::ShardIndex, output_index: &OutputIndex) -> schema::ShardIndex {
    let (txn_id, _) = output_index;
    let x = u64::from_be_bytes([
        txn_id[0], txn_id[1], txn_id[2], txn_id[3], txn_id[4], txn_id[5], txn_id[6], txn_id[7],
    ]);
    ((x as u128 * num_shards as u128) >> 64) as _
}

pub struct ShardedUtxoExecute {
    num_shards: schema::ShardIndex,
    shard_index: schema::ShardIndex,
    locked: FxHashSet<OutputIndex>,
}

impl ShardedUtxoExecute {
    pub fn new(num_shards: schema::ShardIndex, shard_index: schema::ShardIndex) -> Self {
        Self {
            num_shards,
            shard_index,
            locked: Default::default(),
        }
    }
}

impl AbstractExecute for ShardedUtxoExecute {
    type Op = ShardedUtxoOp;
    type Res = ShardedUtxoRes;

    fn execute(
        &mut self,
        op: Self::Op,
        state: FxHashMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> (Self::Res, Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        match op {
            ShardedUtxoOp::Prepare(op) => {
                for input in &op.inputs {
                    if shard_of(self.num_shards, input) != self.shard_index {
                        continue;
                    }
                    if !self.locked.contains(input)
                        && let Some(_value) = &state[&utxo::key(input)]
                    // TODO check signature
                    {
                    } else {
                        warn!(
                            "UTXO prepare failed: input {:?} is missing or locked",
                            input
                        );
                        return (ShardedUtxoRes::Prepare(false), vec![]);
                    }
                }
                for input in op.inputs {
                    if shard_of(self.num_shards, &input) != self.shard_index {
                        continue;
                    }
                    self.locked.insert(input);
                }
                (ShardedUtxoRes::Prepare(true), vec![])
            }
            ShardedUtxoOp::Commit(op, success) => {
                for input in &op.inputs {
                    self.locked.remove(input);
                }
                let updates = if success {
                    let mut updates = vec![];
                    for input in &op.inputs {
                        if shard_of(self.num_shards, input) != self.shard_index {
                            continue;
                        }
                        updates.push((utxo::key(input), None));
                    }
                    let txn_id = op.id();
                    for (i, output) in op.outputs.into_iter().enumerate() {
                        if shard_of(self.num_shards, &(txn_id, i as _)) != self.shard_index {
                            continue;
                        }
                        updates.push((
                            utxo::key(&(txn_id, i as _)),
                            Some([&output.0[..], &output.1.to_le_bytes()].concat()),
                        ));
                    }
                    updates
                } else {
                    vec![]
                };
                (ShardedUtxoRes::Committed, updates)
            }
        }
    }
}
