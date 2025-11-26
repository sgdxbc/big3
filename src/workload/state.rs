use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Instant,
};

use hdrhistogram::{
    Histogram,
    serialization::{Serializer as _, V2Serializer},
};
use log::{info, warn};
use rand::{Rng, RngCore as _, random_range, rng};

use crate::{
    common::RequestId,
    execute::{
        self,
        sharded_utxo::{self, ShardedUtxoOp, ShardedUtxoRes},
        utxo::{OutputIndex, TxnId, UtxoOp, UtxoRes},
        ycsb::{VALUE_SIZE, YcsbOp},
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
    ShardedUtxo(UtxoWorkload<C, ShardedUtxoWorkingState>),
}

impl<C> Workload<C> {
    pub fn new(
        context: C,
        scrape_state: Arc<Mutex<ClientScrapeState>>,
        schema: &schema::WorkloadConfig,
        num_concurrent: u32,
        num_shards: schema::ShardIndex,
        workload_index: u32,
    ) -> Self {
        match &schema {
            schema::WorkloadConfig::Ycsb(cfg) => Self::Ycsb(YcsbWorkload::new(
                context,
                cfg.clone(),
                num_concurrent,
                scrape_state,
            )),
            schema::WorkloadConfig::Utxo(cfg) if num_shards == 1 => Self::Utxo(UtxoWorkload::new(
                context,
                cfg,
                num_concurrent,
                num_shards,
                scrape_state,
                workload_index,
            )),
            schema::WorkloadConfig::Utxo(cfg) => Self::ShardedUtxo(UtxoWorkload::new(
                context,
                cfg,
                num_concurrent,
                num_shards,
                scrape_state,
                workload_index,
            )),
        }
    }

    pub fn log_metrics(&self) {
        match self {
            Workload::Ycsb(w) => w.log_metrics(),
            _ => { /* TODO */ }
        }
    }
}

impl<C: WorkloadContext> Workload<C> {
    pub fn start(&mut self) {
        match self {
            Workload::Ycsb(w) => w.start(),
            Workload::Utxo(w) => w.start(),
            Workload::ShardedUtxo(w) => w.start(),
        }
    }

    pub fn on_invoke_response(&mut self, invoke_id: InvokeId, res: Vec<u8>) {
        match self {
            Workload::Ycsb(w) => w.on_invoke_response(invoke_id, res),
            Workload::Utxo(w) => w.on_invoke_response(invoke_id, res),
            Workload::ShardedUtxo(w) => w.on_invoke_response(invoke_id, res),
        }
    }
}

pub struct YcsbWorkload<C> {
    context: C,
    config: schema::YcsbWorkloadConfig,
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
        config: schema::YcsbWorkloadConfig,
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
            YcsbOp::Get(key)
        } else {
            let field = random_range(0..10);
            let mut value = vec![0; VALUE_SIZE];
            rng().fill_bytes(&mut value);
            YcsbOp::Put(key, field, value)
        };
        let command = bincode::encode_to_vec(&op, bincode::config::standard()).unwrap();
        let invoke_id = self.context.invoke(0, command);
        self.working.insert(
            invoke_id,
            WorkingState {
                start: Instant::now(),
            },
        );
    }
}

pub struct UtxoWorkload<C, WS = UtxoWorkingState> {
    context: C,
    num_concurrent: u32,
    num_shards: schema::ShardIndex,
    scrape_state: Arc<Mutex<ClientScrapeState>>,

    working: HashMap<TxnId, WS>,
    invoke_txns: HashMap<InvokeId, TxnId>,
    output_pool: Vec<OutputIndex>,
}

pub struct UtxoWorkingState {
    start: Instant,
}

impl<C, WS> UtxoWorkload<C, WS> {
    pub fn new(
        context: C,
        config: &schema::UtxoWorkloadConfig,
        num_concurrent: u32,
        num_shards: schema::ShardIndex,
        scrape_state: Arc<Mutex<ClientScrapeState>>,
        pool_index: u32,
    ) -> Self {
        let pool_size = num_concurrent;
        assert!((pool_index + 1) as u64 * pool_size as u64 <= config.num_outputs);
        let output_pool = (pool_index * pool_size..(pool_index + 1) * pool_size)
            .map(|i| {
                let op = UtxoOp::prefilled(i as _);
                (op.id(), 0)
            })
            .collect();
        Self {
            context,
            num_concurrent,
            num_shards,
            scrape_state,
            output_pool,
            working: Default::default(),
            invoke_txns: Default::default(),
        }
    }
}

impl<C: WorkloadContext> UtxoWorkload<C, UtxoWorkingState> {
    pub fn start(&mut self) {
        for _ in 0..self.num_concurrent {
            self.invoke();
        }
    }

    pub fn on_invoke_response(&mut self, invoke_id: InvokeId, res: Vec<u8>) {
        let txn_id = self.invoke_txns.remove(&invoke_id).unwrap();
        let working = self.working.remove(&txn_id).expect("no ongoing work");
        let res = bincode::decode_from_slice(&res, bincode::config::standard())
            .expect("failed to decode response")
            .0;
        if matches!(res, UtxoRes::Ok) {
            let latency = working.start.elapsed();
            {
                let mut scrape_state = self.scrape_state.lock().unwrap();
                scrape_state.latency_histogram += latency.as_nanos() as u64;
            }
            self.output_pool.extend([(txn_id, 0)]);
        } else {
            // probably should not put the outputs back to the pool; retrying to spend them is
            // likely to fail again
        }

        self.invoke();
    }

    fn invoke(&mut self) {
        let i = rng().random_range(0..self.output_pool.len());
        let output_index = self.output_pool.swap_remove(i);
        let op = UtxoOp {
            inputs: vec![output_index],
            outputs: vec![([0u8; 32], 0)],
        };
        let txn_id = op.id();
        let command = bincode::encode_to_vec(&op, bincode::config::standard()).unwrap();
        assert_eq!(self.num_shards, 1);
        let invoke_id = self.context.invoke(0, command);
        self.working.insert(
            txn_id,
            UtxoWorkingState {
                start: Instant::now(),
            },
        );
        self.invoke_txns.insert(invoke_id, txn_id);
    }
}

pub struct ShardedUtxoWorkingState {
    start: Instant,
    op: UtxoOp,
    status: ShardedUtxoStatus,
}

enum ShardedUtxoStatus {
    Preparing(HashSet<ShardIndex>, bool),
    Committing(bool, HashSet<ShardIndex>),
}

impl<C: WorkloadContext> UtxoWorkload<C, ShardedUtxoWorkingState> {
    pub fn start(&mut self) {
        for _ in 0..self.num_concurrent {
            self.invoke();
        }
    }

    pub fn on_invoke_response(&mut self, invoke_id: InvokeId, res: Vec<u8>) {
        let txn_id = self.invoke_txns.remove(&invoke_id).unwrap();
        let working = self.working.get_mut(&txn_id).expect("no ongoing work");
        assert_eq!(txn_id, working.op.id());
        let res = bincode::decode_from_slice(&res, bincode::config::standard())
            .expect("failed to decode response")
            .0;
        match (&mut working.status, res) {
            (
                ShardedUtxoStatus::Preparing(pending_shards, success),
                ShardedUtxoRes::Prepare(shard, shard_success),
            ) => {
                pending_shards.remove(&shard);
                *success &= shard_success;
                if pending_shards.is_empty() {
                    let command = bincode::encode_to_vec(
                        ShardedUtxoOp::Commit(working.op.clone(), *success),
                        bincode::config::standard(),
                    )
                    .unwrap();
                    let mut pending_shards = HashSet::new();
                    for input in &working.op.inputs {
                        let shard = sharded_utxo::shard_of(self.num_shards, &input.0);
                        if pending_shards.insert(shard) {
                            let invoke_id = self.context.invoke(0, command.clone());
                            self.invoke_txns.insert(invoke_id, txn_id);
                        }
                    }
                    if *success {
                        let shard = sharded_utxo::shard_of(self.num_shards, &txn_id);
                        if pending_shards.insert(shard) {
                            let invoke_id = self.context.invoke(0, command.clone());
                            self.invoke_txns.insert(invoke_id, txn_id);
                        }
                    }
                    assert!(!pending_shards.is_empty());
                    working.status = ShardedUtxoStatus::Committing(*success, pending_shards);
                }
            }
            (
                ShardedUtxoStatus::Committing(success, pending_shards),
                ShardedUtxoRes::Committed(shard),
            ) => {
                pending_shards.remove(&shard);
                if pending_shards.is_empty() {
                    if *success {
                        let latency = working.start.elapsed();
                        {
                            let mut scrape_state = self.scrape_state.lock().unwrap();
                            scrape_state.latency_histogram += latency.as_nanos() as u64;
                        }
                        self.output_pool
                            .extend((0..working.op.outputs.len()).map(|i| (txn_id, i as _)));
                    } else {
                        // probably should not put the outputs back to the pool; retrying to spend them is
                        // likely to fail again
                        warn!("Transaction {:?} aborted", txn_id);
                    }

                    self.working.remove(&txn_id);
                    self.invoke();
                }
            }
            _ => unimplemented!(),
        }
    }

    fn invoke(&mut self) {
        let i = rng().random_range(0..self.output_pool.len());
        let input = self.output_pool.swap_remove(i);
        let op = UtxoOp {
            inputs: vec![input],
            outputs: vec![([0u8; 32], 0)],
        };
        let txn_id = op.id();
        let command = bincode::encode_to_vec(
            ShardedUtxoOp::Prepare(op.clone()),
            bincode::config::standard(),
        )
        .unwrap();
        let shard = sharded_utxo::shard_of(self.num_shards, &input.0);
        let invoke_id = self.context.invoke(0, command);
        self.working.insert(
            txn_id,
            ShardedUtxoWorkingState {
                start: Instant::now(),
                op,
                status: ShardedUtxoStatus::Preparing([shard].into(), true),
            },
        );
        self.invoke_txns.insert(invoke_id, txn_id);
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
