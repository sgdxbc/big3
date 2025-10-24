use std::{sync::Arc, thread::available_parallelism};

use rand::{RngCore as _, SeedableRng, rngs::StdRng};
use rocksdb::{DB, Options, WriteBatch, WriteOptions};
use tokio::{fs, task::JoinSet};

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
        // options.set_enable_pipelined_write(true);
        options.set_unordered_write(true);
        let db = DB::open(&options, PREFILL_PATH)?;

        let batch_size = 100_000;
        let mut rng = StdRng::seed_from_u64(117418);

        let mut i = 0;
        let mut join_set = JoinSet::new();
        let db = Arc::new(db);
        while i < schema.num_keys {
            let mut rng = StdRng::from_rng(&mut rng);
            let db = db.clone();
            join_set.spawn(async move {
                let mut batch = WriteBatch::new();
                let mut value = vec![0u8; 100 - 16];
                for j in i..(i + batch_size).min(schema.num_keys) {
                    let key = execute::key(j);
                    rng.fill_bytes(&mut value);
                    batch.put(key, &value);
                }
                let mut options = WriteOptions::default();
                options.disable_wal(true);
                db.write_opt(batch, &options)?;
                anyhow::Ok(())
            });
            i += batch_size;
        }
        while let Some(res) = join_set.join_next().await {
            res??;
        }
        db.compact_range(None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }
}
