use std::thread::available_parallelism;

use rand::{RngCore as _, SeedableRng, rngs::StdRng};
use rocksdb::{DB, Options, WriteBatch};
use tokio::fs;

use crate::{execute, schema};

use super::PREFILL_PATH;

pub struct PrefillTask;

impl PrefillTask {
    pub async fn load(schema: schema::PrefillTask) -> anyhow::Result<()> {
        let _ = fs::remove_dir_all(PREFILL_PATH).await;

        let mut options = Options::default();
        options.create_if_missing(true);
        options.prepare_for_bulk_load();
        options.increase_parallelism(available_parallelism()?.get() as _);
        options.set_max_subcompactions(available_parallelism()?.get() as _);
        let db = DB::open(&options, PREFILL_PATH)?;

        let batch_size = 10_000;
        let mut rng = StdRng::seed_from_u64(117418);

        let (tx_batch, rx_batch) = std::sync::mpsc::sync_channel(100);
        let produce = tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::new();
            let mut value = vec![0u8; 100 - 32];
            for i in 0..schema.num_keys {
                let key = execute::storage_key(&execute::key(i));
                rng.fill_bytes(&mut value);
                batch.put(key, &value);
                if i % batch_size == batch_size - 1 {
                    let _ = tx_batch.send(batch);
                    batch = WriteBatch::new();
                }
            }
            if !batch.is_empty() {
                let _ = tx_batch.send(batch);
            }
        });
        let consume = tokio::task::spawn_blocking(move || {
            for batch in rx_batch {
                db.write(batch)?;
            }
            db.compact_range::<&[u8], &[u8]>(None, None);
            anyhow::Ok(())
        });
        let produce = async {
            produce.await?;
            anyhow::Ok(())
        };
        let consume = async { consume.await? };
        tokio::try_join!(produce, consume)?;
        Ok(())
    }
}
