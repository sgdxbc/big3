use std::time::Duration;

use big_schema::Storage;

pub const NUM_KEYS: u64 = 10_000_000;
pub const READ_RATIO: f64 = 0.5;
pub const NUM_FAULTY_NODES: u16 = 1;
pub const NUM_CONCURRENT: u32 = 1;

pub const THETA: f64 = 0.99;
pub const CACHE_SIZE: usize = 8_000_000;

pub enum Sharding {
    Single,
    Multi(u8, u16), // (num_shards, num_shard_faulty_nodes)
}

pub const SHARDING: Sharding = Sharding::Single;

pub const STORAGE: Storage = Storage::Full;

#[derive(Debug)]
pub enum App {
    Ycsb,
    Utxo,
}

pub const APP: App = App::Ycsb;

pub const LIVE_DURATION: Duration = Duration::from_secs(10);
pub const STRIPE_INTERVAL: Duration = Duration::from_hours(1);

include!("latency_matrix.rs");

#[derive(Debug)]
pub enum Network {
    Lan,
    Wan,
}
pub const NETWORK: Network = Network::Lan;

impl Network {
    pub fn to_latencies(self) -> Option<Vec<Vec<u32>>> {
        match self {
            Network::Lan => None,
            Network::Wan => Some(LATENCY_MATRIX.iter().map(|row| row.to_vec()).collect()),
        }
    }
}
