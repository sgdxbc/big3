use rocksdb::{DB, ReadOptions, WriteBatch};
use tokio::sync::oneshot;

pub struct Storage {
    db: DB,
}

pub enum StorageOp {
    Fetch(Vec<[u8; 32]>, oneshot::Sender<Vec<Option<Vec<u8>>>>),
    Post(Vec<([u8; 32], Option<Vec<u8>>)>),
}

impl Storage {
    pub fn new(db: DB) -> anyhow::Result<Self> {
        Ok(Self { db })
    }

    pub fn invoke(&mut self, op: StorageOp) -> anyhow::Result<()> {
        match op {
            StorageOp::Fetch(keys, tx_response) => {
                let mut read_options = ReadOptions::default();
                read_options.set_async_io(true);
                let res = self
                    .db
                    .multi_get_opt(keys, &read_options)
                    .into_iter()
                    .collect::<Result<_, _>>()?;
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
