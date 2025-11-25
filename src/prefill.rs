use std::{sync::Arc, thread::available_parallelism};

use rand::{Rng as _, SeedableRng, rngs::StdRng};
use rocksdb::{DB, Options, WriteBatch, WriteOptions};
use tokio::fs;

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

        // let mut join_set = JoinSet::new();
        let db = Arc::new(db);
        match schema.app {
            schema::App::Ycsb(num_keys) => {
                let key_fn = |index| ycsb::key(index).into_bytes();
                run_prefill_storage(
                    &db,
                    num_keys,
                    key_fn,
                    ycsb::VALUE_SIZE * 10,
                    schema.storage,
                    &schema,
                )?;
            }
            schema::App::Utxo(num_outputs) => {
                let key_fn = |index| utxo::key(&(utxo::UtxoOp::prefilled(index).id(), 0));
                run_prefill_storage(&db, num_outputs, key_fn, 32 + 8, schema.storage, &schema)?;
            }
        }
        Ok(())
    }
}

fn run_prefill(
    db: &DB,
    num_keys: u64,
    key: impl Fn(u64) -> Vec<u8>,
    value_size: usize,
) -> anyhow::Result<()> {
    let batch_size = 100_000;
    let mut rng = StdRng::seed_from_u64(117418);
    let mut i = 0;
    while i < num_keys {
        let mut batch = WriteBatch::new();
        let mut value = vec![0u8; value_size];
        for j in i..(i + batch_size).min(num_keys) {
            let key = key(j);
            rng.fill(&mut value[..]);
            batch.put(key, &value[..]);
        }
        let mut options = WriteOptions::default();
        options.disable_wal(true);
        db.write_opt(batch, &options)?;
        i += batch_size;
    }
    db.compact_range(None::<&[u8]>, None::<&[u8]>);
    Ok(())
}

fn run_prefill_storage(
    db: &DB,
    num_keys: u64,
    key: impl Fn(u64) -> Vec<u8>,
    value_size: usize,
    storage: schema::Storage,
    schema: &schema::PrefillTask,
) -> anyhow::Result<()> {
    match storage {
        schema::Storage::Full => run_prefill(db, num_keys, key, value_size),
        schema::Storage::Big => {
            let storage_config = BigStorageConfig::from(&schema.config);
            let storing_shards = storage_config.storing_shards(schema.node_index);
            let wrapped_key = |k: u64| {
                let mut key = key(k);
                let shard = storage_config.shard_of_key(&key);
                if !storing_shards.contains(&shard) {
                    // return empty key to skip
                    return vec![];
                }
                key = [&shard.to_be_bytes()[..], &key[..]].concat();
                key
            };
            run_prefill(db, num_keys, wrapped_key, value_size)
        }
    }
}
