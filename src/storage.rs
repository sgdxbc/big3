use std::time::Instant;

use hdrhistogram::Histogram;
use log::info;
use rocksdb::{DB, WriteBatch};
use tokio::sync::oneshot;

pub enum StorageOp {
    Fetch(Vec<Vec<u8>>, oneshot::Sender<Vec<Option<Vec<u8>>>>),
    Post(Vec<(Vec<u8>, Option<Vec<u8>>)>),
}

pub struct PlainStorage {
    db: DB,
    metrics: PlainStorageMetrics,
}

struct PlainStorageMetrics {
    multi_get_size: Histogram<u64>,
    multi_get_tput: Histogram<u64>,
}

impl Default for PlainStorageMetrics {
    fn default() -> Self {
        Self {
            multi_get_size: Histogram::<u64>::new(3).unwrap(),
            multi_get_tput: Histogram::<u64>::new(3).unwrap(),
        }
    }
}

impl PlainStorage {
    pub fn new(db: DB) -> anyhow::Result<Self> {
        Ok(Self {
            db,
            metrics: Default::default(),
        })
    }

    pub fn log_metrics(&self) {
        info!(
            "PlainStorage\n\tmulti_get_size: mean {:.2} p50 {:.2} p99 {:.2}\n\tmulti_get_tput: mean {:.2} p50 {:.2} p99 {:.2}",
            self.metrics.multi_get_size.mean(),
            self.metrics.multi_get_size.value_at_percentile(50.0),
            self.metrics.multi_get_size.value_at_percentile(99.0),
            self.metrics.multi_get_tput.mean(),
            self.metrics.multi_get_tput.value_at_percentile(50.0),
            self.metrics.multi_get_tput.value_at_percentile(99.0),
        );
    }

    pub fn invoke(&mut self, op: StorageOp) -> anyhow::Result<()> {
        match op {
            StorageOp::Fetch(keys, tx_response) => {
                let res = if !keys.is_empty() {
                    let start = Instant::now();
                    let res = self
                        .db
                        .multi_get(&keys)
                        .into_iter()
                        .collect::<Result<_, _>>()?;
                    let latency = start.elapsed();
                    self.metrics.multi_get_size += keys.len() as u64;
                    self.metrics.multi_get_tput +=
                        (keys.len() as f64 / latency.as_secs_f64()) as u64;
                    res
                } else {
                    Default::default()
                };
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

pub type FetchId = u64;

pub trait BigStorageContext {
    fn fetch(&mut self, keys: Vec<[u8; 32]>) -> FetchId;
    fn post(&mut self, kvs: Vec<([u8; 32], Option<Vec<u8>>)>);

    fn send_to_all(&mut self, message: Vec<u8>);
}

// pub struct BigStorage<C> {
//     context: C,
//     //
// }
