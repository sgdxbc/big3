use bincode::{Decode, Encode};
use hashbrown::{HashMap, HashSet};
use log::warn;

use crate::{execute::utxo, schema};

use super::{
    AbstractExecute, AbstractOp,
    utxo::{OutputIndex, TxnId, UtxoOp},
};

#[derive(Encode, Decode)]
pub enum ShardedUtxoOp {
    Prepare(UtxoOp),
    Commit(UtxoOp, bool),
}

#[derive(Encode, Decode)]
pub enum ShardedUtxoRes {
    Prepare(schema::ShardIndex, bool),
    Committed(schema::ShardIndex),
}

impl AbstractOp for ShardedUtxoOp {
    fn read_set(&self) -> Vec<Vec<u8>> {
        match self {
            ShardedUtxoOp::Prepare(op) => op.read_set(),
            _ => vec![],
        }
    }
}

pub fn shard_of(num_shards: schema::ShardIndex, txn_id: &TxnId) -> schema::ShardIndex {
    let x = u64::from_be_bytes([
        txn_id[0], txn_id[1], txn_id[2], txn_id[3], txn_id[4], txn_id[5], txn_id[6], txn_id[7],
    ]);
    ((x as u128 * num_shards as u128) >> 64) as _
}

pub struct ShardedUtxoExecute {
    num_shards: schema::ShardIndex,
    shard_index: schema::ShardIndex,
    locked: HashSet<OutputIndex>,
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
        state: &HashMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> (Self::Res, Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        match op {
            ShardedUtxoOp::Prepare(op) => {
                for input in &op.inputs {
                    if shard_of(self.num_shards, &input.0) != self.shard_index {
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
                        return (ShardedUtxoRes::Prepare(self.shard_index, false), vec![]);
                    }
                }
                for input in op.inputs {
                    if shard_of(self.num_shards, &input.0) != self.shard_index {
                        continue;
                    }
                    self.locked.insert(input);
                }
                (ShardedUtxoRes::Prepare(self.shard_index, true), vec![])
            }
            ShardedUtxoOp::Commit(op, success) => {
                for input in &op.inputs {
                    self.locked.remove(input);
                }
                let updates = if success {
                    let mut updates = vec![];
                    for input in &op.inputs {
                        if shard_of(self.num_shards, &input.0) != self.shard_index {
                            continue;
                        }
                        updates.push((utxo::key(input), None));
                    }
                    let txn_id = op.id();
                    if shard_of(self.num_shards, &txn_id) == self.shard_index {
                        for (i, output) in op.outputs.into_iter().enumerate() {
                            updates.push((
                                utxo::key(&(txn_id, i as _)),
                                Some([&output.0[..], &output.1.to_le_bytes()].concat()),
                            ));
                        }
                    }
                    updates
                } else {
                    vec![]
                };
                (ShardedUtxoRes::Committed(self.shard_index), updates)
            }
        }
    }
}
