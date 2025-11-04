use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Instant,
};

use rand::{Rng, RngCore as _, rng, seq::IteratorRandom};

use crate::{
    execute::{
        self,
        utxo::OutputIndex,
        ycsb::{Op, VALUE_SIZE},
    },
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

pub enum Workload<C> {
    Ycsb(YcsbWorkload<C>),
}

impl<C> Workload<C> {
    pub fn new(
        context: C,
        config: &schema::WorkloadConfig,
        num_concurrent: u32,
        scrape_state: Arc<Mutex<ClientScrapeState>>,
    ) -> Self {
        Self::Ycsb(YcsbWorkload::new(
            context,
            config.into(),
            num_concurrent,
            scrape_state,
        ))
    }
}

impl<C: WorkloadContext> Workload<C> {
    pub fn start(&mut self) {
        match self {
            Workload::Ycsb(w) => w.start(),
        }
    }

    pub fn on_invoke_response(&mut self, invoke_id: InvokeId, res: Vec<u8>) {
        match self {
            Workload::Ycsb(w) => w.on_invoke_response(invoke_id, res),
        }
    }
}

pub struct YcsbWorkloadConfig {
    num_keys: u64,
    read_ratio: f64,
    num_shards: ShardIndex,
}

impl From<&schema::WorkloadConfig> for YcsbWorkloadConfig {
    fn from(config: &schema::WorkloadConfig) -> Self {
        Self {
            num_keys: config.num_keys,
            read_ratio: config.read_ratio,
            num_shards: config.num_shards,
        }
    }
}

impl YcsbWorkloadConfig {
    fn shard_of_key(&self, index: u64) -> ShardIndex {
        ((index / (self.num_keys / self.num_shards as u64)) as ShardIndex).min(self.num_shards - 1)
    }
}

pub struct YcsbWorkload<C> {
    context: C,
    config: YcsbWorkloadConfig,
    num_concurrent: u32,
    zipfian: ScrambledZipfian,
    scrape_state: Arc<Mutex<ClientScrapeState>>,

    working: HashMap<InvokeId, WorkingState>,
}

struct WorkingState {
    start: Instant,
}

impl<C> YcsbWorkload<C> {
    pub fn new(
        context: C,
        config: YcsbWorkloadConfig,
        num_concurrent: u32,
        scrape_state: Arc<Mutex<ClientScrapeState>>,
    ) -> Self {
        let zipfian = ScrambledZipfian::new_range(0, config.num_keys - 1);
        Self {
            context,
            config,
            num_concurrent,
            zipfian,
            scrape_state,
            working: Default::default(),
        }
    }
}

impl<C: WorkloadContext> YcsbWorkload<C> {
    pub fn start(&mut self) {
        for _ in 0..self.num_concurrent {
            self.invoke();
        }
    }

    pub fn on_invoke_response(&mut self, invoke_id: InvokeId, _res: Vec<u8>) {
        let working = self.working.remove(&invoke_id).expect("no ongoing work");
        let latency = working.start.elapsed();
        {
            let mut scrape_state = self.scrape_state.lock().unwrap();
            scrape_state.latency_histogram += latency.as_nanos() as u64;
        }

        self.invoke();
    }

    fn invoke(&mut self) {
        let key_index = self.zipfian.next_u64(&mut rng());
        // let key_index = rng().random_range(0..self.config.num_keys);
        let key = execute::ycsb::key(key_index);
        let op = if rng().random_bool(self.config.read_ratio) {
            Op::Get(key)
        } else {
            let mut value = vec![0; VALUE_SIZE];
            rng().fill_bytes(&mut value);
            Op::Put(key, value)
        };
        let command = bincode::encode_to_vec(&op, bincode::config::standard()).unwrap();
        let invoke_id = self
            .context
            .invoke(self.config.shard_of_key(key_index), command);
        self.working.insert(
            invoke_id,
            WorkingState {
                start: Instant::now(),
            },
        );
    }
}

pub struct UtxoWorkloadConfig {
    // num_keys: u64,
}

impl From<&schema::WorkloadConfig> for UtxoWorkloadConfig {
    fn from(config: &schema::WorkloadConfig) -> Self {
        Self {
            // num_keys: config.num_keys,
        }
    }
}

pub struct UtxoWorkload<C> {
    context: C,
    config: UtxoWorkloadConfig,
    num_concurrent: u32,
    scrape_state: Arc<Mutex<ClientScrapeState>>,

    working: HashMap<InvokeId, UtxoWorkingState>,
    output_pool: HashSet<OutputIndex>,
}

struct UtxoWorkingState {
    start: Instant,
    output_indexes: Vec<OutputIndex>,
}

impl<C> UtxoWorkload<C> {
    pub fn new(
        context: C,
        config: UtxoWorkloadConfig,
        scrape_state: Arc<Mutex<ClientScrapeState>>,
        num_concurrent: u32,
        output_pool: HashSet<OutputIndex>,
    ) -> Self {
        assert!(num_concurrent as usize <= output_pool.len());
        Self {
            context,
            config,
            num_concurrent,
            scrape_state,
            output_pool,
            working: Default::default(),
        }
    }
}

impl<C: WorkloadContext> UtxoWorkload<C> {
    pub fn start(&mut self) {
        for _ in 0..self.num_concurrent {
            self.invoke();
        }
    }

    pub fn on_invoke_response(&mut self, invoke_id: InvokeId, _res: Vec<u8>) {
        let working = self.working.remove(&invoke_id).expect("no ongoing work");
        // assert_eq!(working.invoke_id, invoke_id);
        let latency = working.start.elapsed();
        {
            let mut scrape_state = self.scrape_state.lock().unwrap();
            scrape_state.latency_histogram += latency.as_nanos() as u64;
        }
        self.output_pool.extend(working.output_indexes);

        self.invoke();
    }

    fn invoke(&mut self) {
        let output_index = *self.output_pool.iter().choose(&mut rng()).unwrap();
        self.output_pool.remove(&output_index);
        let op = execute::utxo::Op {
            inputs: vec![output_index],
            outputs: vec![([0u8; 32], 0)],
        };
        let txn_id = op.txn_id();
        let command = bincode::encode_to_vec(&op, bincode::config::standard()).unwrap();
        let invoke_id = self.context.invoke(0, command);
        self.working.insert(
            invoke_id,
            UtxoWorkingState {
                start: Instant::now(),
                output_indexes: vec![(txn_id, 0)],
            },
        );
    }
}
