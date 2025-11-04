use std::time::Duration;

use big_schema::{App, Storage};

pub const NUM_KEYS: u64 = 10_000_000;
pub const READ_RATIO: f64 = 0.5;
pub const NUM_FAULTY_NODES: u16 = 1;
pub const NUM_CONCURRENT: u64 = 1;

pub const STORAGE: Storage = Storage::Full;
pub const APP: App = App::Ycsb;
pub const NUM_SHARDS: u8 = 1;
pub const LIVE_DURATION: Duration = Duration::from_secs(10);
