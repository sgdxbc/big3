use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use hdrhistogram::{
    Histogram,
    serialization::{Serializer as _, V2Serializer},
};
use log::info;
use rand::{Rng, RngCore as _, rng};

use crate::{
    common::RequestId,
    execute::{
        self,
        utxo::OutputIndex,
        ycsb::{Op, VALUE_SIZE},
    },
    schema,
};

use super::zipfian::ScrambledZipfian;

pub type InvokeId = RequestId;
pub type ShardIndex = crate::schema::ShardIndex;

pub trait WorkloadContext {
    fn invoke(&mut self, shard: ShardIndex, command: Vec<u8>) -> InvokeId;
}

pub enum Workload<C> {
    Ycsb(YcsbWorkload<C>),
    Utxo(UtxoWorkload<C>),
}

impl<C> Workload<C> {
    pub fn new(
        context: C,
        scrape_state: Arc<Mutex<ClientScrapeState>>,
        schema: &schema::WorkloadConfig,
        num_concurrent: u32,
        workload_index: u32,
    ) -> Self {
        match &schema {
            schema::WorkloadConfig::Ycsb(cfg) => Self::Ycsb(YcsbWorkload::new(
                context,
                cfg.into(),
                num_concurrent,
                scrape_state,
            )),
            schema::WorkloadConfig::Utxo(cfg) => Self::Utxo(UtxoWorkload::new(
                context,
                cfg,
                num_concurrent,
                scrape_state,
                workload_index,
            )),
        }
    }

    pub fn log_metrics(&self) {
        match self {
            Workload::Ycsb(w) => w.log_metrics(),
            Workload::Utxo(_w) => { /* TODO */ }
        }
    }
}

impl<C: WorkloadContext> Workload<C> {
    pub fn start(&mut self) {
        match self {
            Workload::Ycsb(w) => w.start(),
            Workload::Utxo(w) => w.start(),
        }
    }

    pub fn on_invoke_response(&mut self, invoke_id: InvokeId, res: Vec<u8>) {
        match self {
            Workload::Ycsb(w) => w.on_invoke_response(invoke_id, res),
            Workload::Utxo(w) => w.on_invoke_response(invoke_id, res),
        }
    }
}

pub struct YcsbWorkloadConfig {
    num_keys: u64,
    read_ratio: f64,
    num_shards: ShardIndex,
}

impl From<&schema::YcsbWorkloadConfig> for YcsbWorkloadConfig {
    fn from(config: &schema::YcsbWorkloadConfig) -> Self {
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

    fn log_metrics(&self) {
        let now = Instant::now();
        let max_latency = self.working.values().map(|state| now - state.start).max();
        info!(
            "YCSB Workload Metrics: Ongoing Requests: {}, Max Latency: {:?}",
            self.working.len(),
            max_latency
        );
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

pub struct UtxoWorkload<C> {
    context: C,
    num_concurrent: u32,
    scrape_state: Arc<Mutex<ClientScrapeState>>,

    working: HashMap<InvokeId, UtxoWorkingState>,
    output_pool: Vec<OutputIndex>,
}

struct UtxoWorkingState {
    start: Instant,
    output_indexes: Vec<OutputIndex>,
}

impl<C> UtxoWorkload<C> {
    const POOL_SIZE: u32 = 1_000_000;

    pub fn new(
        context: C,
        config: &schema::UtxoWorkloadConfig,
        num_concurrent: u32,
        scrape_state: Arc<Mutex<ClientScrapeState>>,
        pool_index: u32,
    ) -> Self {
        // ensure that fresh outputs are not reused too quickly
        assert!(num_concurrent * 2 < Self::POOL_SIZE);
        assert!((pool_index + 1) as u64 * Self::POOL_SIZE as u64 <= config.num_outputs);
        let output_pool = (pool_index * Self::POOL_SIZE..(pool_index + 1) * Self::POOL_SIZE)
            .map(|i| {
                let op = execute::utxo::Op::prefilled(i as _);
                (op.id(), 0)
            })
            .collect();
        Self {
            context,
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
        let i = rng().random_range(0..self.output_pool.len());
        let output_index = self.output_pool.swap_remove(i);
        let op = execute::utxo::Op {
            inputs: vec![output_index],
            outputs: vec![([0u8; 32], 0)],
        };
        let txn_id = op.id();
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

pub struct ClientScrapeState {
    start: Instant,
    pub latency_histogram: Histogram<u64>,
}

impl ClientScrapeState {
    pub fn now() -> Self {
        Self {
            start: Instant::now(),
            latency_histogram: Histogram::new(3).unwrap(),
        }
    }

    pub fn scrape(&mut self) -> schema::Scrape {
        let interval = self.start.elapsed();
        self.start = Instant::now();
        let mut serializer = V2Serializer::new();
        let mut latency_histogram = Vec::new();
        serializer
            .serialize(&self.latency_histogram, &mut latency_histogram)
            .unwrap();
        self.latency_histogram.reset();
        schema::Scrape {
            interval,
            latency_histogram,
        }
    }
}
