use std::{
    collections::VecDeque,
    hash::{BuildHasher, BuildHasherDefault, DefaultHasher},
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;
use log::info;
use rand::{SeedableRng, rngs::StdRng, seq::IteratorRandom};
use rocksdb::{DB, WriteBatch};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use crate::{
    schema,
    tasks::{RequestId, ResponseContext},
    types::NodeIndex,
};

pub enum StorageOp {
    Fetch(Vec<Vec<u8>>, ResponseContext<Vec<Option<Vec<u8>>>>),
    Post(Vec<(Vec<u8>, Option<Vec<u8>>)>),
}

pub struct PlainStorage {
    db: DB,
    metrics: PlainStorageMetrics,
}

struct PlainStorageMetrics {
    multi_get_size: Histogram<u64>,
    multi_get_tput: Histogram<u64>,
    fetch_time: Duration,
}

impl Default for PlainStorageMetrics {
    fn default() -> Self {
        Self {
            multi_get_size: Histogram::<u64>::new(3).unwrap(),
            multi_get_tput: Histogram::<u64>::new(3).unwrap(),
            fetch_time: Duration::ZERO,
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
            "PlainStorage\n\tmulti_get_size: mean {:.2} p50 {:.2} p99 {:.2}\n\tmulti_get_tput: mean {:.2} p50 {:.2} p99 {:.2}\n\tfetch_time: {:.2?}",
            self.metrics.multi_get_size.mean(),
            self.metrics.multi_get_size.value_at_percentile(50.0),
            self.metrics.multi_get_size.value_at_percentile(99.0),
            self.metrics.multi_get_tput.mean(),
            self.metrics.multi_get_tput.value_at_percentile(50.0),
            self.metrics.multi_get_tput.value_at_percentile(99.0),
            self.metrics.fetch_time,
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
                        .collect::<Result<_, _>>()?;
                    let latency = start.elapsed();
                    self.metrics.fetch_time += latency;
                    self.metrics.multi_get_size += keys.len() as u64;
                    self.metrics.multi_get_tput +=
                        (keys.len() as f64 / latency.as_secs_f64()) as u64;
                    res
                } else {
                    Default::default()
                };
                context.respond(res);
            }
            StorageOp::Post(kvs) => {
                let mut batch = WriteBatch::new();
                for (key, value) in kvs {
                    match value {
                        Some(value) => batch.put(key, value),
                        None => batch.delete(key),
                    }
                }
                self.db.write(batch)?;
            }
        }
        Ok(())
    }
}

pub type BackendFetchId = RequestId;

pub trait BigStorageContext {
    fn backend_fetch(&mut self, keys: Vec<Vec<u8>>) -> BackendFetchId;
    fn backend_post(&mut self, updates: Vec<(Vec<u8>, Option<Vec<u8>>)>);

    fn send_to_all(&mut self, message: message::Message);
}

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

type Values = Vec<Option<Vec<u8>>>;
type FetchSeq = u64;

pub struct BigStorage<C> {
    context: C,
    config: BigStorageConfig,
    node_index: NodeIndex,
    // cached config
    primary_shards: HashSet<u32>,
    secondary_shards: HashSet<u32>,

    fetch_seq: FetchSeq,
    fetching: VecDeque<BackendFetchingState>,
    querying: HashMap<FetchSeq, QueryingState>,
    reordered_push_states: HashMap<FetchSeq, Vec<message::PushState>>,

    metrics: BigStorageMetrics,
}

struct BigStorageMetrics {
    network_start: Instant,
    network_time: Duration,
}

struct BackendFetchingState {
    seq: FetchSeq,
    key_shards: Vec<u32>,
    context: ResponseContext<Vec<Option<Vec<u8>>>>,
    // only for sanity check
    backend: BackendFetchId,
}

struct QueryingState {
    key_shards: Vec<u32>,
    node_states: HashMap<NodeIndex, Values>,
    context: ResponseContext<Vec<Option<Vec<u8>>>>,
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
        let (reordered_push_states, fetching, querying) = Default::default();
        Self {
            context,
            config,
            node_index,
            primary_shards,
            secondary_shards,
            reordered_push_states,
            fetch_seq: 0,
            fetching,
            querying,
            metrics: BigStorageMetrics {
                network_start: Instant::now(),
                network_time: Duration::ZERO,
            },
        }
    }

    pub fn log_metrics(&self) {
        info!(
            "BigStorage\n\tnetwork_time: {:.2?}",
            self.metrics.network_time,
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

    fn start_fetch(&mut self, keys: Vec<Vec<u8>>, context: ResponseContext<Vec<Option<Vec<u8>>>>) {
        self.fetch_seq += 1;
        let key_shards: Vec<u32> = keys
            .iter()
            .map(|key| self.config.shard_of_key(key))
            .collect();
        let backend_keys = keys
            .into_iter()
            .zip(&key_shards)
            .filter_map(|(key, shard)| {
                if self.primary_shards.contains(shard) {
                    Some(key)
                } else {
                    None
                }
            })
            .collect();
        let fetch_id = self.context.backend_fetch(backend_keys);
        let fetching = BackendFetchingState {
            seq: self.fetch_seq,
            key_shards,
            context,
            backend: fetch_id,
        };
        self.fetching.push_back(fetching);
    }

    pub fn on_message(&mut self, message: message::Message) {
        match message {
            message::Message::PushState(push_state) => {
                self.insert_state(push_state);
            }
        }
    }

    pub fn on_fetch_response(&mut self, fetch_id: BackendFetchId, values: Vec<Option<Vec<u8>>>) {
        let Some(fetching) = self.fetching.pop_front() else {
            unimplemented!()
        };
        assert_eq!(fetch_id, fetching.backend);

        let push_state = message::PushState {
            values: values.clone(),
            node_index: self.node_index,
            fetch_seq: fetching.seq,
        };
        self.context
            .send_to_all(Message::PushState(push_state.clone()));

        if self.querying.is_empty() {
            self.metrics.network_start = Instant::now();
        }
        self.querying.insert(
            fetching.seq,
            QueryingState {
                key_shards: fetching.key_shards,
                node_states: Default::default(),
                context: fetching.context,
            },
        );
        self.insert_state(push_state);
        if let Some(reordered) = self.reordered_push_states.remove(&fetching.seq) {
            for push_state in reordered {
                self.insert_state(push_state);
            }
        }
    }

    fn insert_state(&mut self, push_state: message::PushState) {
        let Some(querying) = self.querying.get_mut(&push_state.fetch_seq) else {
            // TODO assert that fetch_seq is in the future
            self.reordered_push_states
                .entry(push_state.fetch_seq)
                .or_default()
                .push(push_state);
            return;
        };

        querying
            .node_states
            .insert(push_state.node_index, push_state.values);
        if querying.node_states.len()
            != (self.config.num_nodes - self.config.num_faulty_nodes) as usize
        {
            return;
        }

        // info!("all node states received for fetch");
        let querying = self.querying.remove(&push_state.fetch_seq).unwrap();
        let mut values = Vec::new();
        let mut node_index = vec![0; self.config.num_nodes as usize];
        for shard in querying.key_shards {
            let i = self.config.primary_node_of_shard(shard);
            let value = querying.node_states[&i][node_index[i as usize]].clone();
            node_index[i as usize] += 1;
            values.push(value);
        }
        assert!(
            querying
                .node_states
                .iter()
                .all(|(i, values)| node_index[*i as usize] == values.len())
        );
        querying.context.respond(values);
        if self.querying.is_empty() {
            self.metrics.network_time += self.metrics.network_start.elapsed();
        }
    }
}

pub use message::Message;

mod message {
    use big_schema::NodeIndex;
    use bincode::{Decode, Encode};

    use super::{FetchSeq, Values};

    #[derive(Encode, Decode)]
    pub enum Message {
        PushState(PushState),
    }

    #[derive(Encode, Decode, Clone)]
    pub struct PushState {
        pub fetch_seq: FetchSeq,
        pub values: Values,
        pub node_index: NodeIndex,
    }
}
