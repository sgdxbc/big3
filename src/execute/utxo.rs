use bincode::{Decode, Encode};
use log::warn;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    consensus::Block,
    storage::FetchResponse,
    types::{ClientId, ClientSeq, NodeIndex, Reply},
};

use super::{AbstractExecute, AbstractOp};

pub type TxnId = [u8; 32];
pub type OutputIndex = (TxnId, u32);
pub type Output = ([u8; 32], u64);

#[derive(Encode, Decode)]
pub struct Op {
    pub inputs: Vec<OutputIndex>,
    pub outputs: Vec<Output>,
}

#[derive(Encode, Decode)]
pub enum Res {
    Ok,
    Invalid,
}

impl Op {
    pub fn txn_id(&self) -> TxnId {
        use sha2::{Digest as _, Sha256};

        let mut hasher = Sha256::new();
        for (input_txn_id, input_index) in &self.inputs {
            hasher.update(input_txn_id);
            hasher.update(input_index.to_le_bytes());
        }
        for (output_hash, output_value) in &self.outputs {
            hasher.update(output_hash);
            hasher.update(output_value.to_le_bytes());
        }
        hasher.finalize().into()
    }
}

pub fn key(output_index: &OutputIndex) -> Vec<u8> {
    [&output_index.0[..], &output_index.1.to_be_bytes()].concat()
}

impl AbstractOp for Op {
    fn read_set(&self) -> Vec<Vec<u8>> {
        self.inputs.iter().map(key).collect()
    }
}

pub struct UtxoExecute;

impl AbstractExecute for UtxoExecute {
    type Op = Op;
    type Res = Res;

    fn execute(
        &mut self,
        op: Self::Op,
        state: FxHashMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> (Self::Res, Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        for input in &op.inputs {
            // TODO check signature script
            if !state.contains_key(&key(input)) {
                warn!("invalid UTXO");
                return (Res::Invalid, Vec::new());
            }
        }
        // TODO check sum(input) >= sum(output)
        let mut updates = Vec::new();
        for input in &op.inputs {
            updates.push((key(input), None));
        }
        let txn_id = op.txn_id();
        for (i, (pub_key, amount)) in op.outputs.iter().enumerate() {
            updates.push((
                key(&(txn_id, i as u32)),
                Some([&pub_key[..], &amount.to_le_bytes()].concat()),
            ));
        }
        (Res::Ok, updates)
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
                for (txn_id, index) in &op.inputs {
                    keys.insert([&txn_id[..], &index.to_be_bytes()].concat());
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
        let mut spent = FxHashSet::default();
        let mut outputs = Vec::new();
        for (op, client_id, client_seq) in self.requests {
            let mut res = Res::Ok;
            for (txn_id, index) in &op.inputs {
                if state
                    .get(&[&txn_id[..], &index.to_be_bytes()].concat())
                    .is_none()
                    || spent.contains(&(*txn_id, *index))
                // TODO check signature script
                // TODO check sum(input) >= sum(output)
                // TODO should allow spending the newly-created outputs in the same batch, but
                // probably rare in practice
                {
                    warn!("invalid UTXO");
                    res = Res::Invalid;
                    break;
                }
            }
            if let Res::Ok = res {
                for (txn_id, index) in &op.inputs {
                    spent.insert((*txn_id, *index));
                }
                let txn_id = op.txn_id();
                for (i, (pub_key, amount)) in op.outputs.iter().enumerate() {
                    outputs.push((txn_id, i as u32, *pub_key, *amount));
                }
            }
            let reply = Reply {
                client_seq,
                res: bincode::encode_to_vec(&res, bincode::config::standard()).unwrap(),
                node_index,
            };
            send(client_id, reply);
        }

        let mut updates = spent
            .into_iter()
            .map(|(txn_id, index)| ([&txn_id[..], &index.to_be_bytes()].concat(), None))
            .collect::<Vec<_>>();
        updates.extend(outputs.into_iter().map(|(txn_id, index, pub_key, amount)| {
            (
                [&txn_id[..], &index.to_be_bytes()].concat(),
                Some([&pub_key[..], &amount.to_le_bytes()].concat()),
            )
        }));
        updates
    }
}

impl Op {
    pub fn prefilled(index: u64) -> Self {
        Self {
            inputs: Vec::new(),
            outputs: vec![([0u8; 32], index)],
        }
    }
}
