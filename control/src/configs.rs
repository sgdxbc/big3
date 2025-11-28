#[allow(unused)]
mod defaults;

#[allow(unused)]
pub use defaults::*;

include!("configs/overrides.rs");

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
                theta: THETA,
            }),
            App::Utxo => big_schema::WorkloadConfig::Utxo(big_schema::UtxoWorkloadConfig {
                num_outputs: NUM_KEYS,
            }),
        }
    }
}

impl Sharding {
    pub fn num_running_nodes(&self) -> u16 {
        self.num_shards as u16 * (self.num_shard_faulty_nodes * 2 + 1)
    }

    pub fn num_nodes(&self) -> u16 {
        self.num_shards as u16 * (self.num_shard_faulty_nodes * 3 + 1)
    }

    pub fn num_faulty_nodes(&self) -> u16 {
        self.num_shards as u16 * self.num_shard_faulty_nodes
    }
}
