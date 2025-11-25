use std::{sync::Arc, thread::available_parallelism};

use rand::{Rng as _, SeedableRng, rngs::StdRng};
use rocksdb::{DB, Options, WriteBatch, WriteOptions};
use tokio::{fs, task::JoinSet};

use crate::{
    common::PREFILL_PATH,
    execute::{utxo, ycsb},
    schema,
    storage::BigStorageConfig,
};

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
        let storage_config = BigStorageConfig::from(&schema.config);
        let storing_shards = storage_config.storing_shards(schema.node_index);
        while i < schema.num_keys {
            let mut rng = StdRng::from_rng(&mut rng);
            let db = db.clone();
            let storage_config = storage_config.clone();
            let storing_shards = storing_shards.clone();
            join_set.spawn(async move {
                let mut batch = WriteBatch::new();
                let mut value = [0u8; 1 << 10];
                for j in i..(i + batch_size).min(schema.num_keys) {
                    let (mut key, value) = match &schema.app {
                        schema::App::Ycsb => {
                            let key = ycsb::key(j);
                            rng.fill(&mut value[..ycsb::VALUE_SIZE * 10]);
                            (key.into(), &value[..ycsb::VALUE_SIZE * 10])
                        }
                        schema::App::Utxo => {
                            let txn = utxo::UtxoOp::prefilled(j);
                            let key = utxo::key(&(txn.id(), 0));
                            rng.fill(&mut value[..32 + 8]);
                            (key, &value[..32 + 8])
                        }
                    };
                    // TODO deal with sharded
                    if matches!(schema.storage, schema::Storage::Big) {
                        let shard = storage_config.shard_of_key(&key);
                        if !storing_shards.contains(&shard) {
                            continue;
                        }
                        key = [&shard.to_be_bytes()[..], &key[..]].concat();
                    }
                    batch.put(key, value);
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
