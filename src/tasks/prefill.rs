use std::thread::available_parallelism;

use rand::{RngCore as _, SeedableRng, rngs::StdRng};
use rocksdb::{DB, Options, WriteBatch};

use crate::{execute, schema};

use super::PREFILL_PATH;

pub struct PrefillTask;

impl PrefillTask {
    pub async fn load(schema: &schema::PrefillTask) -> anyhow::Result<()> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.prepare_for_bulk_load();
        options.increase_parallelism(available_parallelism()?.get() as _);
        options.set_max_subcompactions(available_parallelism()?.get() as _);
        let db = DB::open(&options, PREFILL_PATH)?;

        let batch_size = 10_000;
        let mut rng = StdRng::seed_from_u64(117418);
        let mut batch = WriteBatch::new();
        let mut value = vec![0u8; 100 - 32];
        for i in 0..schema.num_keys {
            let key = execute::storage_key(&execute::key(i));
            rng.fill_bytes(&mut value);
            batch.put(key, &value);
            if i % batch_size == batch_size - 1 {
                db.write(batch)?;
                batch = WriteBatch::new();
            }
        }
        if !batch.is_empty() {
            db.write(batch)?;
        }
        db.compact_range::<&[u8], &[u8]>(None, None);
        Ok(())
    }
}
