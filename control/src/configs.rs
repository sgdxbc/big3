#[allow(unused)]
mod defaults;

#[allow(unused)]
pub use defaults::*;

include!("configs/overrides.rs");

pub const fn num_nodes() -> u16 {
    NUM_FAULTY_NODES * 3 + 1
}

impl App {
    pub fn to_schema_app(&self) -> big_schema::App {
        match self {
            App::Ycsb => big_schema::App::Ycsb(NUM_KEYS),
            App::Utxo => big_schema::App::Utxo(NUM_KEYS),
        }
    }

    pub fn to_schema_workload(&self) -> big_schema::WorkloadConfig {
        match self {
            App::Ycsb => big_schema::WorkloadConfig::Ycsb(big_schema::YcsbWorkloadConfig {
                num_keys: NUM_KEYS,
                read_ratio: READ_RATIO,
            }),
            App::Utxo => big_schema::WorkloadConfig::Utxo(big_schema::UtxoWorkloadConfig {
                num_outputs: NUM_KEYS,
            }),
        }
    }
}

impl Sharding {
    pub fn num_shards(&self) -> u8 {
        match self {
            Sharding::Single => 1,
            Sharding::Multi(num_shards, _) => *num_shards,
        }
    }

    pub fn num_shard_faulty_nodes(&self) -> u16 {
        match self {
            Sharding::Single => NUM_FAULTY_NODES,
            Sharding::Multi(_, num_shard_faulty_nodes) => *num_shard_faulty_nodes,
        }
    }
}
