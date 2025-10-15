use std::path::Path;

use rocksdb::{DB, WriteBatch};
use tokio::sync::oneshot;

pub struct Storage {
    db: DB,
}

pub enum StorageOp {
    Fetch([u8; 32], oneshot::Sender<Option<Vec<u8>>>),
    Post(Vec<([u8; 32], Option<Vec<u8>>)>),
}

impl Storage {
    pub fn new(path: &Path) -> anyhow::Result<Self> {
        let db = DB::open_default(path)?;
        Ok(Self { db })
    }

    pub fn invoke(&mut self, op: StorageOp) -> anyhow::Result<()> {
        match op {
            StorageOp::Fetch(key, tx_response) => {
                let res = self.db.get(key)?;
                let _ = tx_response.send(res);
            }
            StorageOp::Post(kvs) => {
                let mut batch = WriteBatch::new();
                for (key, value) in kvs {
                    match value {
                        Some(value) => batch.put(key, value),
                        None => batch.delete(key),
                    }
                }
                self.db.write(batch)?;
            }
        }
        Ok(())
    }
}
