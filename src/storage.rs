use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::{BuildHasher, BuildHasherDefault, DefaultHasher},
    time::Instant,
};

use hdrhistogram::Histogram;
use log::info;
use rand::{SeedableRng, rngs::StdRng, seq::IteratorRandom};
use rocksdb::{DB, WriteBatch};
use tokio::sync::oneshot;

use crate::{schema, types::NodeIndex};

pub enum StorageOp {
    Fetch(Vec<Vec<u8>>, oneshot::Sender<Vec<Option<Vec<u8>>>>),
    Post(Vec<(Vec<u8>, Option<Vec<u8>>)>),
}

pub struct PlainStorage {
    db: DB,
    metrics: PlainStorageMetrics,
}

struct PlainStorageMetrics {
    multi_get_size: Histogram<u64>,
    multi_get_tput: Histogram<u64>,
}

impl Default for PlainStorageMetrics {
    fn default() -> Self {
        Self {
            multi_get_size: Histogram::<u64>::new(3).unwrap(),
            multi_get_tput: Histogram::<u64>::new(3).unwrap(),
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
            "PlainStorage\n\tmulti_get_size: mean {:.2} p50 {:.2} p99 {:.2}\n\tmulti_get_tput: mean {:.2} p50 {:.2} p99 {:.2}",
            self.metrics.multi_get_size.mean(),
            self.metrics.multi_get_size.value_at_percentile(50.0),
            self.metrics.multi_get_size.value_at_percentile(99.0),
            self.metrics.multi_get_tput.mean(),
            self.metrics.multi_get_tput.value_at_percentile(50.0),
            self.metrics.multi_get_tput.value_at_percentile(99.0),
        );
    }

    pub fn invoke(&mut self, op: StorageOp) -> anyhow::Result<()> {
        match op {
            StorageOp::Fetch(keys, tx_response) => {
                let res = if !keys.is_empty() {
                    let start = Instant::now();
                    let res = self
                        .db
                        .multi_get(&keys)
                        .into_iter()
                        .collect::<Result<_, _>>()?;
                    let latency = start.elapsed();
                    self.metrics.multi_get_size += keys.len() as u64;
                    self.metrics.multi_get_tput +=
                        (keys.len() as f64 / latency.as_secs_f64()) as u64;
                    res
                } else {
                    Default::default()
                };
                let _ = tx_response.send(res);
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

pub type FetchId = u64;

pub trait BigStorageContext {
    fn fetch(&mut self, keys: Vec<Vec<u8>>) -> FetchId;
    fn post(&mut self, kvs: Vec<(Vec<u8>, Option<Vec<u8>>)>);

    fn send_to_all(&mut self, message: message::Message);
}

pub struct BigStorageConfig {
    num_nodes: NodeIndex,
    num_stripes: u32,
    num_secondary_nodes: NodeIndex,
}

impl From<&schema::ReplicaConfig> for BigStorageConfig {
    fn from(value: &schema::ReplicaConfig) -> Self {
        Self {
            num_nodes: value.num_nodes,
            num_stripes: 100,
            num_secondary_nodes: 7,
        }
    }
}

impl BigStorageConfig {
    fn num_shards(&self) -> u32 {
        self.num_stripes * self.num_nodes as u32
    }

    fn shard_of_key(&self, key: &[u8]) -> u32 {
        (BuildHasherDefault::<DefaultHasher>::default().hash_one(key) % self.num_shards() as u64)
            as _
    }

    fn primary_node_of_shard(&self, shard: u32) -> NodeIndex {
        (shard % self.num_nodes as u32) as _
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

pub struct BigStorage<C> {
    context: C,
    config: BigStorageConfig,
    node_index: NodeIndex,
    // cached config
    primary_shards: HashSet<u32>,
    secondary_shards: HashSet<u32>,

    fetching: Option<FetchingState>,
    reordered_node_states: HashMap<NodeIndex, VecDeque<Values>>,
}

struct FetchingState {
    keys: Vec<Vec<u8>>,
    backend: Option<(FetchId, Vec<Vec<u8>>)>,
    node_states: HashMap<NodeIndex, Values>,
    tx_response: oneshot::Sender<Vec<Option<Vec<u8>>>>,
}

impl<C> BigStorage<C> {
    pub fn new(context: C, config: BigStorageConfig, node_index: NodeIndex) -> Self {
        let mut primary_shards = HashSet::new();
        let mut secondary_shards = HashSet::new();
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
        let (reordered_node_states,) = Default::default();
        Self {
            context,
            config,
            node_index,
            primary_shards,
            secondary_shards,
            reordered_node_states,
            fetching: None,
        }
    }
}

impl<C: BigStorageContext> BigStorage<C> {
    pub fn invoke(&mut self, op: StorageOp) {
        match op {
            StorageOp::Fetch(keys, tx_response) => {
                let backend_keys = keys
                    .iter()
                    .filter(|key| {
                        let shard = self.config.shard_of_key(key);
                        self.primary_shards.contains(&shard)
                            || self.secondary_shards.contains(&shard)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let fetch_id = self.context.fetch(backend_keys.clone());
                let fetching = FetchingState {
                    keys,
                    backend: Some((fetch_id, backend_keys)),
                    node_states: Default::default(),
                    tx_response,
                };
                let replaced = self.fetching.replace(fetching);
                assert!(replaced.is_none(), "concurrent fetches are not supported");

                let mut reorder_inserts = Vec::new();
                for (&node_index, queue) in &mut self.reordered_node_states {
                    if let Some(state) = queue.pop_front() {
                        reorder_inserts.push((node_index, state));
                    }
                }
                for (node_index, state) in reorder_inserts {
                    self.insert_state(node_index, state);
                }
            }
            StorageOp::Post(mut updates) => {
                updates.retain(|(key, _)| {
                    let shard = self.config.shard_of_key(key);
                    self.primary_shards.contains(&shard) || self.secondary_shards.contains(&shard)
                });
                self.context.post(updates);
            }
        }
    }

    pub fn on_message(&mut self, message: message::Message) {
        match message {
            message::Message::PushState(push_state) => {
                self.insert_state(push_state.node_index, push_state.values);
            }
        }
    }

    pub fn on_fetch_response(&mut self, fetch_id: FetchId, values: Vec<Option<Vec<u8>>>) {
        let Some(fetching) = &mut self.fetching else {
            unimplemented!()
        };
        let (expected_fetch_id, keys) = fetching.backend.take().unwrap();
        assert_eq!(fetch_id, expected_fetch_id);

        let mut pushed_values = Vec::new();
        for (key, value) in keys.iter().zip(&values) {
            let shard = self.config.shard_of_key(key);
            if self.primary_shards.contains(&shard) {
                pushed_values.push(value.clone());
            }
        }
        let push_state = message::PushState {
            values: pushed_values,
            node_index: self.node_index,
        };
        self.context
            .send_to_all(message::Message::PushState(push_state));

        self.insert_state(self.node_index, values);
    }

    fn insert_state(&mut self, node_index: NodeIndex, values: Values) {
        let Some(fetching) = &mut self.fetching else {
            self.reordered_node_states
                .entry(node_index)
                .or_default()
                .push_back(values);
            return;
        };
        if fetching.node_states.contains_key(&node_index) {
            self.reordered_node_states
                .entry(node_index)
                .or_default()
                .push_back(values);
            return;
        }

        fetching.node_states.insert(node_index, values);
        if fetching.node_states.len() != self.config.num_nodes as usize {
            return;
        }

        let fetching = self.fetching.take().unwrap();
        let mut values = Vec::new();
        let mut node_index = vec![0; self.config.num_nodes as usize];
        for key in fetching.keys {
            let shard = self.config.shard_of_key(&key);
            let primary_node = self.config.primary_node_of_shard(shard);
            let value =
                fetching.node_states[&primary_node][node_index[primary_node as usize]].clone();
            node_index[primary_node as usize] += 1;
            values.push(value);
        }
        let _ = fetching.tx_response.send(values);
    }
}

mod message {
    use big_schema::NodeIndex;
    use bincode::{Decode, Encode};

    use super::Values;

    #[derive(Encode, Decode)]
    pub enum Message {
        PushState(PushState),
    }

    #[derive(Encode, Decode)]
    pub struct PushState {
        pub values: Values,
        pub node_index: NodeIndex,
    }
}
