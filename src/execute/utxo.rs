use bincode::{Decode, Encode};
use log::warn;
use rustc_hash::FxHashMap;

use super::{AbstractExecute, AbstractOp};

pub type TxnId = [u8; 32];
pub type OutputIndex = (TxnId, u32);
pub type Output = ([u8; 32], u64);

#[derive(Encode, Decode, Clone)]
pub struct UtxoOp {
    pub inputs: Vec<OutputIndex>,
    pub outputs: Vec<Output>,
}

#[derive(Encode, Decode)]
pub enum UtxoRes {
    Ok,
    Invalid,
}

impl UtxoOp {
    pub fn id(&self) -> TxnId {
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

impl AbstractOp for UtxoOp {
    fn read_set(&self) -> Vec<Vec<u8>> {
        self.inputs.iter().map(key).collect()
    }
}

pub struct UtxoExecute;

impl AbstractExecute for UtxoExecute {
    type Op = UtxoOp;
    type Res = UtxoRes;

    fn execute(
        &mut self,
        op: Self::Op,
        state: FxHashMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> (Self::Res, Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        for input in &op.inputs {
            // TODO check signature script
            if !state.contains_key(&key(input)) {
                warn!("invalid UTXO");
                return (UtxoRes::Invalid, Vec::new());
            }
        }
        // TODO check sum(input) >= sum(output)
        let mut updates = Vec::new();
        for input in &op.inputs {
            updates.push((key(input), None));
        }
        let txn_id = op.id();
        for (i, (pub_key, amount)) in op.outputs.iter().enumerate() {
            updates.push((
                key(&(txn_id, i as u32)),
                Some([&pub_key[..], &amount.to_le_bytes()].concat()),
            ));
        }
        (UtxoRes::Ok, updates)
    }
}

impl UtxoOp {
    pub fn prefilled(index: u64) -> Self {
        Self {
            inputs: Vec::new(),
            outputs: vec![([0u8; 32], index)],
        }
    }
}
