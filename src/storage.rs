use std::{
    collections::VecDeque,
    hash::{BuildHasher, BuildHasherDefault, DefaultHasher},
    time::Instant,
};

use hdrhistogram::Histogram;
use log::info;
use rand::{SeedableRng, rngs::StdRng, seq::IteratorRandom};
use rocksdb::{DB, WriteBatch};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use crate::{
    metrics::Latency,
    schema,
    tasks::{RequestId, ResponseContext},
    types::NodeIndex,
};

pub enum StorageOp {
    Fetch(HashSet<Vec<u8>>, ResponseContext<FetchResponse>),
    Post(Vec<(Vec<u8>, Option<Vec<u8>>)>),
}

pub struct PlainStorage {
    db: DB,
    metrics: PlainStorageMetrics,
}

pub enum FetchResponse {
    Plain(PlainStorageFetchResponse),
    Big(BigStorageFetchResponse),
}

impl FetchResponse {
    pub fn get(&self, key: &[u8]) -> &Option<Vec<u8>> {
        match self {
            FetchResponse::Plain(res) => res.get(key),
            FetchResponse::Big(res) => res.get(key),
        }
    }
}

pub struct PlainStorageFetchResponse(HashMap<Vec<u8>, Option<Vec<u8>>>);

impl PlainStorageFetchResponse {
    pub fn get(&self, key: &[u8]) -> &Option<Vec<u8>> {
        &self.0[key]
    }
}

struct PlainStorageMetrics {
    multi_get_size: Histogram<u64>,
    multi_get_tput: Histogram<u64>,
    fetch_time: Latency,
    post_time: Latency,
}

impl Default for PlainStorageMetrics {
    fn default() -> Self {
        Self {
            multi_get_size: Histogram::<u64>::new(3).unwrap(),
            multi_get_tput: Histogram::<u64>::new(3).unwrap(),
            fetch_time: Default::default(),
            post_time: Default::default(),
        }
    }
}

impl PlainStorage {
    pub fn new(db: DB) -> anyhow::Result<Self> {
        Ok(Self {
            db,
            metrics: Default::default(),
        })
    }

    pub fn log_metrics(&self) {
        info!(
            "PlainStorage\nmulti_get_size: mean {:.2} p50 {:.2} p99 {:.2}\nmulti_get_tput: mean {:.2} p50 {:.2} p99 {:.2}\nfetch_time: {}\npost_time: {}",
            self.metrics.multi_get_size.mean(),
            self.metrics.multi_get_size.value_at_percentile(50.0),
            self.metrics.multi_get_size.value_at_percentile(99.0),
            self.metrics.multi_get_tput.mean(),
            self.metrics.multi_get_tput.value_at_percentile(50.0),
            self.metrics.multi_get_tput.value_at_percentile(99.0),
            self.metrics.fetch_time,
            self.metrics.post_time,
        );
    }

    pub fn invoke(&mut self, op: StorageOp) -> anyhow::Result<()> {
        match op {
            StorageOp::Fetch(keys, context) => {
                let res = if !keys.is_empty() {
                    let start = Instant::now();
                    let res = self
                        .db
                        .multi_get(&keys)
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?;
                    let latency = start.elapsed();
                    self.metrics.fetch_time += latency;
                    self.metrics.multi_get_size += keys.len() as u64;
                    self.metrics.multi_get_tput +=
                        (keys.len() as f64 / latency.as_secs_f64()) as u64;
                    keys.into_iter().zip(res).collect()
                } else {
                    Default::default()
                };
                context.respond(FetchResponse::Plain(PlainStorageFetchResponse(res)));
            }
            StorageOp::Post(kvs) => {
                let mut batch = WriteBatch::new();
                for (key, value) in kvs {
                    match value {
                        Some(value) => batch.put(key, value),
                        None => batch.delete(key),
                    }
                }
                if !batch.is_empty() {
                    let start = Instant::now();
                    self.db.write(batch)?;
                    self.metrics.post_time += start.elapsed();
                }
            }
        }
        Ok(())
    }
}

pub type BackendFetchId = RequestId;

pub trait BigStorageContext {
    fn backend_fetch(&mut self, keys: HashSet<Vec<u8>>) -> BackendFetchId;
    fn backend_post(&mut self, updates: Vec<(Vec<u8>, Option<Vec<u8>>)>);

    fn send_to_all(&mut self, message: message::Message);
}

#[derive(Clone)]
pub struct BigStorageConfig {
    num_nodes: NodeIndex,
    num_faulty_nodes: NodeIndex,
    num_stripes: u32,
    num_secondary_nodes: NodeIndex,
}

impl From<&schema::ReplicaConfig> for BigStorageConfig {
    fn from(value: &schema::ReplicaConfig) -> Self {
        Self {
            num_nodes: value.num_nodes,
            num_faulty_nodes: value.num_faulty_nodes,
            num_stripes: 100,
            num_secondary_nodes: 7,
        }
    }
}

impl BigStorageConfig {
    fn num_shards(&self) -> u32 {
        self.num_stripes * (self.num_nodes - self.num_faulty_nodes) as u32
    }

    fn shard_of_key(&self, key: &[u8]) -> u32 {
        (BuildHasherDefault::<DefaultHasher>::default().hash_one(key) % self.num_shards() as u64)
            as _
    }

    fn primary_node_of_shard(&self, shard: u32) -> NodeIndex {
        (shard % (self.num_nodes - self.num_faulty_nodes) as u32) as _
    }

    fn secondary_nodes_of_shard(&self, shard: u32) -> impl Iterator<Item = NodeIndex> {
        (0..self.num_nodes - 1)
            .choose_multiple(
                &mut StdRng::seed_from_u64(shard as _),
                self.num_secondary_nodes as _,
            )
            .into_iter()
            .map(move |n| n + (n >= self.primary_node_of_shard(shard)) as NodeIndex)
    }
}

type StateStore = HashMap<Vec<u8>, Option<Vec<u8>>>;
type FetchSeq = u64;

pub struct BigStorage<C> {
    context: C,
    config: BigStorageConfig,
    #[allow(dead_code)]
    node_index: NodeIndex,
    // cached config
    primary_shards: HashSet<u32>,
    secondary_shards: HashSet<u32>,

    fetch_seq: FetchSeq,
    response_contexts: VecDeque<(FetchSeq, ResponseContext<FetchResponse>)>,
    backend_fetching: HashMap<BackendFetchId, (FetchSeq, u32)>,
    shard_states: HashMap<FetchSeq, HashMap<u32, StateStore>>,

    metrics: BigStorageMetrics,
}

pub struct BigStorageFetchResponse {
    shards: HashMap<u32, HashMap<Vec<u8>, Option<Vec<u8>>>>,
    config: BigStorageConfig,
}

impl BigStorageFetchResponse {
    fn get(&self, key: &[u8]) -> &Option<Vec<u8>> {
        let shard = self.config.shard_of_key(key);
        &self.shards[&shard][key]
    }
}

struct BigStorageMetrics {
    prepare: Latency,
    backend: Latency,
    network: Latency,
}

impl<C> BigStorage<C> {
    pub fn new(context: C, config: BigStorageConfig, node_index: NodeIndex) -> Self {
        let mut primary_shards = HashSet::default();
        let mut secondary_shards = HashSet::default();
        for shard in 0..config.num_shards() {
            if config.primary_node_of_shard(shard) == node_index {
                primary_shards.insert(shard);
            }
            if config
                .secondary_nodes_of_shard(shard)
                .any(|n| n == node_index)
            {
                secondary_shards.insert(shard);
            }
        }
        Self {
            context,
            config,
            node_index,
            primary_shards,
            secondary_shards,
            fetch_seq: 0,
            response_contexts: Default::default(),
            backend_fetching: Default::default(),
            shard_states: Default::default(),
            metrics: BigStorageMetrics {
                prepare: Default::default(),
                backend: Default::default(),
                network: Default::default(),
            },
        }
    }

    pub fn log_metrics(&self) {
        info!(
            "BigStorage\nprepare: {}\nbackend: {}\nnetwork: {}",
            self.metrics.prepare, self.metrics.backend, self.metrics.network
        );
    }
}

impl<C: BigStorageContext> BigStorage<C> {
    pub fn invoke(&mut self, op: StorageOp) {
        match op {
            StorageOp::Fetch(keys, context) => {
                self.start_fetch(keys, context);
            }
            StorageOp::Post(mut updates) => {
                updates.retain(|(key, _)| {
                    let shard = self.config.shard_of_key(key);
                    self.primary_shards.contains(&shard) || self.secondary_shards.contains(&shard)
                });
                self.context.backend_post(updates);
            }
        }
    }

    fn start_fetch(&mut self, keys: HashSet<Vec<u8>>, context: ResponseContext<FetchResponse>) {
        self.fetch_seq += 1;

        let start = Instant::now();
        for &shard in &self.primary_shards {
            let backend_keys = keys
                .iter()
                .filter(|key| self.config.shard_of_key(key) == shard)
                .cloned()
                .collect();
            let fetch_id = self.context.backend_fetch(backend_keys);
            self.backend_fetching
                .insert(fetch_id, (self.fetch_seq, shard));
        }
        self.metrics.prepare += start.elapsed();

        self.response_contexts.push_back((self.fetch_seq, context));
    }

    pub fn on_message(&mut self, message: message::Message) {
        match message {
            message::Message::PushState(push_state) => {
                self.insert_state(
                    push_state.fetch_seq,
                    push_state.shard_index,
                    push_state.state,
                );
            }
        }
    }

    pub fn on_fetch_response(&mut self, fetch_id: BackendFetchId, response: FetchResponse) {
        let (fetch_seq, shard_index) = self.backend_fetching.remove(&fetch_id).unwrap();
        let FetchResponse::Plain(PlainStorageFetchResponse(state)) = response else {
            unimplemented!()
        };

        let push_state = message::PushState {
            state: state.clone(),
            shard_index,
            fetch_seq,
        };
        self.context.send_to_all(Message::PushState(push_state));
        // self.metrics.backend += fetching.start.elapsed();

        self.insert_state(fetch_seq, shard_index, state);
    }

    fn insert_state(&mut self, seq: FetchSeq, shard_index: u32, state: StateStore) {
        let shard_states = self.shard_states.entry(seq).or_default();
        shard_states.insert(shard_index, state);

        if self
            .shard_states
            .get(&self.response_contexts.front().unwrap().0)
            .is_some_and(|shards| shards.len() == self.config.num_shards() as usize)
        {
            let (seq, context) = self.response_contexts.pop_front().unwrap();
            let shards = self.shard_states.remove(&seq).unwrap();

            let res = BigStorageFetchResponse {
                shards,
                config: self.config.clone(),
            };
            // self.metrics.network += start.elapsed();

            context.respond(FetchResponse::Big(res));
        }
    }
}

pub use message::Message;

mod message {
    use bincode::{Decode, Encode};

    use super::{FetchSeq, StateStore};

    #[derive(Encode, Decode)]
    pub enum Message {
        PushState(PushState),
    }

    #[derive(Encode, Decode, Clone)]
    pub struct PushState {
        pub fetch_seq: FetchSeq,
        pub state: StateStore,
        pub shard_index: u32,
    }
}
