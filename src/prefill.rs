use std::thread::available_parallelism;

use rand::{Rng as _, SeedableRng, rngs::StdRng};
use ring::digest;
use rocksdb::{DB, Options, WriteBatch, WriteOptions};
use tokio::fs;

use crate::{
    common::PREFILL_PATH,
    execute::{utxo, ycsb},
    merkle::{MerkleHash, MerkleTree},
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
        let mut db = db;
        db.create_cf("merkle", &Default::default())?;
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
        db.compact_range(None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }
}

fn run_prefill(
    db: &DB,
    num_keys: u64,
    mut key_value: impl FnMut(u64) -> (Vec<u8>, Vec<u8>),
) -> anyhow::Result<()> {
    let batch_size = 100_000;
    let mut i = 0;
    while i < num_keys {
        let mut batch = WriteBatch::new();
        for j in i..(i + batch_size).min(num_keys) {
            let (key, value) = key_value(j);
            batch.put(key, &value[..]);
        }
        let mut options = WriteOptions::default();
        options.disable_wal(true);
        db.write_opt(batch, &options)?;
        i += batch_size;
    }
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
    let mut rng = StdRng::seed_from_u64(117418);
    match storage {
        schema::Storage::Full => run_prefill(db, num_keys, |i| {
            let mut value = vec![0u8; value_size];
            rng.fill(&mut value[..]);
            (key(i), value)
        }),
        schema::Storage::Big => {
            let storage_config = BigStorageConfig::from(&schema.config);
            // let storing_shards = storage_config.storing_shards(schema.node_index);
            let mut shard_hashes =
                vec![Vec::<MerkleHash>::new(); storage_config.num_shards() as usize];
            let wrapped_key_value = |k: u64| {
                let mut key = key(k);
                let shard = storage_config.shard_of_key(&key);
                key = [&shard.to_be_bytes()[..], &key[..]].concat();

                let i = shard_hashes[shard as usize].len() as u32;
                let mut value = vec![0u8; value_size + 4];
                rng.fill(&mut value[..value_size]);
                value[value_size..].copy_from_slice(&i.to_le_bytes());

                let mut hasher = digest::Context::new(&digest::SHA256);
                hasher.update(&key);
                hasher.update(&value[..value_size]);
                hasher.update(&0u32.to_le_bytes());
                shard_hashes[shard as usize].push(hasher.finish().as_ref().try_into().unwrap());

                (key, value)
            };
            run_prefill(db, num_keys, wrapped_key_value)?;
            let cf = db.cf_handle("merkle").unwrap();
            for (shard, hashes) in shard_hashes.into_iter().enumerate() {
                let tree = MerkleTree::new(hashes);
                let tree_bytes = bincode::encode_to_vec(&tree, bincode::config::standard())?;
                db.put_cf(cf, (shard as u32).to_be_bytes(), &tree_bytes[..])?;
            }
            Ok(())
        }
    }
}
