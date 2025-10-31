use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use rand::{Rng, rng};

use crate::{
    execute::{self, Op},
    schema,
    tasks::{RequestId, client::ClientScrapeState},
};

use self::zipfian::ScrambledZipfian;

mod zipfian;

pub type InvokeId = RequestId;
pub type ShardIndex = crate::schema::ShardIndex;

pub trait WorkloadContext {
    fn invoke(&mut self, shard: ShardIndex, command: Vec<u8>) -> InvokeId;
}

pub struct WorkloadConfig {
    num_keys: u64,
    read_ratio: f64,
    num_shards: ShardIndex,
}

impl From<&schema::WorkloadConfig> for WorkloadConfig {
    fn from(config: &schema::WorkloadConfig) -> Self {
        Self {
            num_keys: config.num_keys,
            read_ratio: config.read_ratio,
            num_shards: config.num_shards,
        }
    }
}

impl WorkloadConfig {
    fn shard_of_key(&self, index: u64) -> ShardIndex {
        (index % self.num_shards as u64) as _
    }
}

pub struct Workload<C> {
    context: C,
    config: WorkloadConfig,
    zipfian: ScrambledZipfian,
    scrape_state: Arc<Mutex<ClientScrapeState>>,

    working: Option<WorkingState>,
}

struct WorkingState {
    start: Instant,
    invoke_id: InvokeId,
}

impl<C> Workload<C> {
    pub fn new(
        context: C,
        config: WorkloadConfig,
        scrape_state: Arc<Mutex<ClientScrapeState>>,
    ) -> Self {
        let zipfian = ScrambledZipfian::new_range(0, config.num_keys - 1);
        Self {
            context,
            config,
            zipfian,
            scrape_state,
            working: None,
        }
    }
}

impl<C: WorkloadContext> Workload<C> {
    pub fn start(&mut self) {
        self.invoke();
    }

    pub fn on_invoke_response(&mut self, invoke_id: InvokeId, _res: Vec<u8>) {
        let working = self.working.as_ref().expect("no ongoing work");
        assert_eq!(working.invoke_id, invoke_id);
        let latency = working.start.elapsed();
        {
            let mut scrape_state = self.scrape_state.lock().unwrap();
            scrape_state.latency_histogram += latency.as_nanos() as u64;
        }

        self.working = None;
        self.invoke();
    }

    fn invoke(&mut self) {
        let key_index = self.zipfian.next_u64(&mut rng());
        // let key_index = rng().random_range(0..self.config.num_keys);
        let key = execute::key(key_index);
        if rng().random_bool(self.config.read_ratio) {
            let op = Op::Get(key);
            let command = bincode::encode_to_vec(&op, bincode::config::standard()).unwrap();
            let invoke_id = self
                .context
                .invoke(self.config.shard_of_key(key_index), command);
            self.working = Some(WorkingState {
                start: Instant::now(),
                invoke_id,
            });
        } else {
            todo!()
        }
    }
}
