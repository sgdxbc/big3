use std::sync::Arc;

use hashbrown::{HashMap, HashSet};
use log::{debug, info};
use lru::LruCache;
use ring::digest;
use rocksdb::DB;
use rustc_hash::FxBuildHasher;
use tempfile::tempdir;
use tokio::{
    process::Command,
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::{NodeIndex, PREFILL_PATH},
    execute2::{FetchHandle, PostHandle},
    merkle::{MerkleHash, MerkleProof, MerkleTree},
    network::interconnect::{NetworkInterconnectHandle, ReceiveHandle},
    schema,
    storage2::{FetchedHandle, PostDoneHandle},
};

pub use message::Message;

pub struct StorageChannels {
    pub tx_fetch: UnboundedSender<HashSet<Vec<u8>>>,
    pub rx_fetch: UnboundedReceiver<HashSet<Vec<u8>>>,

    pub tx_post: UnboundedSender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
    pub rx_post: UnboundedReceiver<Vec<(Vec<u8>, Option<Vec<u8>>)>>,

    tx_message: UnboundedSender<Message>,
    rx_message: UnboundedReceiver<Message>,
}

impl StorageChannels {
    pub fn new() -> Self {
        let (tx_fetch, rx_fetch) = tokio::sync::mpsc::unbounded_channel();
        let (tx_post, rx_post) = tokio::sync::mpsc::unbounded_channel();
        let (tx_message, rx_message) = tokio::sync::mpsc::unbounded_channel();
        Self {
            tx_fetch,
            rx_fetch,
            tx_post,
            rx_post,
            tx_message,
            rx_message,
        }
    }

    pub fn receive_handle(&self) -> ReceiveHandle<Message> {
        ReceiveHandle::new(self.tx_message.clone())
    }

    pub fn fetch_handle(&self) -> FetchHandle {
        FetchHandle {
            tx_keys: self.tx_fetch.clone(),
        }
    }

    pub fn post_handle(&self) -> PostHandle {
        PostHandle {
            tx_post: self.tx_post.clone(),
        }
    }
}

#[derive(Clone)]
pub struct BigStorageConfig {
    pub num_nodes: NodeIndex,
    pub num_faulty_nodes: NodeIndex,
    num_secondary_nodes: NodeIndex,

    cache_size: usize,
}

impl From<&schema::ReplicaConfig> for BigStorageConfig {
    fn from(value: &schema::ReplicaConfig) -> Self {
        let config = Self {
            num_nodes: value.num_nodes,
            num_faulty_nodes: value.num_faulty_nodes,
            num_secondary_nodes: 6,
            cache_size: value.cache_size,
        };
        let num_faulty_nodes = value.num_faulty_nodes;
        assert!((0..config.num_shards()).all(|shard| {
            config
                .nodes_of_shard(shard)
                .any(|n| n < config.num_nodes - num_faulty_nodes)
        }));
        config
    }
}

impl BigStorageConfig {
    pub fn num_shards(&self) -> u32 {
        1000
    }

    pub fn shard_of_key(&self, key: &[u8]) -> u32 {
        use std::hash::BuildHasher;
        (FxBuildHasher.hash_one(key) % self.num_shards() as u64) as _
    }

    pub fn nodes_of_shard(&self, shard: u32) -> impl Iterator<Item = NodeIndex> {
        (0..)
            .map(move |i| ((shard + i * self.num_faulty_nodes as u32) % self.num_nodes as u32) as _)
            .take((1 + self.num_secondary_nodes) as _)
    }

    pub fn pushing_node_of_shard(&self, shard: u32) -> NodeIndex {
        for n in self.nodes_of_shard(shard) {
            if n < self.num_nodes - self.num_faulty_nodes {
                return n;
            }
        }
        panic!("no pushing node for shard {}", shard);
    }

    pub fn storing_shards(&self, node_index: NodeIndex) -> HashSet<u32> {
        (0..self.num_shards())
            .filter(|&shard| self.nodes_of_shard(shard).any(|n| n == node_index))
            .collect()
    }

    pub fn pushing_shards(&self, node_index: NodeIndex) -> HashSet<u32> {
        (0..self.num_shards())
            .filter(|&shard| self.pushing_node_of_shard(shard) == node_index)
            .collect()
    }
}

pub struct StorageTask {
    channels: StorageChannels,
    fetched_handle: FetchedHandle,
    post_done_handle: PostDoneHandle,
    network_interconnect: NetworkInterconnectHandle,

    config: BigStorageConfig,
    #[allow(dead_code)]
    node_index: NodeIndex,
    storing_shards: HashSet<u32>,
    pushing_shards: HashSet<u32>,

    temp_dir: tempfile::TempDir,
    db: Arc<DB>,
    merkle_trees: HashMap<u32, MerkleTree>,
    merkle_roots: Vec<MerkleHash>,
    cache: LruCache<Vec<u8>, Option<Vec<u8>>>,

    tx_fetch_dispatch: flume::Sender<(Vec<u8>, flume::Sender<(Vec<u8>, Option<(Vec<u8>, u32)>)>)>,
    rx_fetch_dispatch: flume::Receiver<(Vec<u8>, flume::Sender<(Vec<u8>, Option<(Vec<u8>, u32)>)>)>,
}

impl StorageTask {
    const NUM_GET_WORKER_THREADS: usize = 20;

    pub async fn load(
        channels: StorageChannels,
        fetched_handle: FetchedHandle,
        post_done_handle: PostDoneHandle,
        network_interconnect: NetworkInterconnectHandle,
        config: BigStorageConfig,
        node_index: NodeIndex,
        app: &schema::App,
    ) -> anyhow::Result<Self> {
        let temp_dir = tempdir()?;
        let status = Command::new("cp")
            .arg("-rT")
            .arg(PREFILL_PATH)
            .arg(temp_dir.path())
            .status()
            .await?;
        anyhow::ensure!(status.success(), "failed to copy prefill data");
        let db = DB::open_cf(&Default::default(), temp_dir.path(), ["merkle"])?;

        let (tx_fetch_dispatch, rx_fetch_dispatch) = flume::unbounded();

        let storing_shards = config.storing_shards(node_index);
        let pushing_shards = config.pushing_shards(node_index);

        let mut merkle_trees = HashMap::new();
        let cf = db.cf_handle("merkle").unwrap();
        for shard in 0..config.num_shards() {
            if !pushing_shards.contains(&shard) {
                continue;
            }
            let tree_bytes = db
                .get_cf(cf, shard.to_be_bytes())?
                .expect("Merkle tree not found for shard");
            let tree = bincode::decode_from_slice(&tree_bytes, bincode::config::standard())?.0;
            merkle_trees.insert(shard, tree);
        }
        let roots_bytes = db.get_cf(cf, b"roots")?.expect("Merkle roots not found");
        let merkle_roots = bincode::decode_from_slice(&roots_bytes, bincode::config::standard())?.0;

        let mut cache = LruCache::new(config.cache_size.try_into().unwrap());
        if let schema::App::Ycsb(num_keys) = app {
            let zipfian = crate::workload::zipfian::ScrambledZipfian::new_range(0, *num_keys - 1);
            for i in (0..cache.cap().get()).rev() {
                let key = crate::execute::ycsb::key(zipfian.scramble(i as _));
                cache.put(key.into_bytes(), Some(vec![0u8; 1000]));
            }
            info!("storage cache preloaded size {}", cache.len());
        }

        Ok(Self {
            channels,
            fetched_handle,
            post_done_handle,
            network_interconnect,
            config,
            node_index,
            storing_shards,
            pushing_shards,
            merkle_trees,
            merkle_roots,
            temp_dir,
            db: Arc::new(db),
            tx_fetch_dispatch,
            rx_fetch_dispatch,
            cache,
        })
    }

    fn get_worker(
        db: Arc<DB>,
        config: BigStorageConfig,
        rx_key: flume::Receiver<(Vec<u8>, flume::Sender<(Vec<u8>, Option<(Vec<u8>, u32)>)>)>,
    ) -> anyhow::Result<()> {
        while let Ok((key, tx)) = rx_key.recv() {
            // assert!(pushing_shards.contains(&config.shard_of_key(&key)));
            let shard = config.shard_of_key(&key);
            let value = db
                .get([&shard.to_be_bytes()[..], &key].concat())?
                .map(|mut v| {
                    let i = v.split_off(v.len() - 4);
                    let index = u32::from_le_bytes(i.try_into().unwrap());
                    // v.truncate(100);
                    (v, index)
                });
            let _ = tx.send((key, value));
        }
        Ok(())
    }

    pub async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        let mut workers = JoinSet::new();
        for _ in 0..Self::NUM_GET_WORKER_THREADS {
            let db = self.db.clone();
            let config = self.config.clone();
            let rx_fetch_dispatch = self.rx_fetch_dispatch.clone();
            workers.spawn_blocking(move || {
                Self::get_worker(db, config, rx_fetch_dispatch).unwrap();
            });
        }

        let mut retrieve = RetrieveWorker {
            rx_message: self.channels.rx_message,
            rx_fetch: self.channels.rx_fetch,
            rx_post: self.channels.rx_post,
            fetched_handle: self.fetched_handle,
            network_interconnect: self.network_interconnect,
            tx_fetch_dispatch: self.tx_fetch_dispatch,
            post_done_handle: self.post_done_handle,
            config: self.config,
            storing_shards: self.storing_shards,
            pushing_shards: self.pushing_shards,
            version: 0,
            reorder_push_values: HashMap::default(),
            update_table: HashMap::default(),
            merkle_trees: self.merkle_trees,
            merkle_roots: self.merkle_roots,
            cache: self.cache,
            metrics: RetrieveWorkerMetrics {
                num_keys: 0,
                num_update_hits: 0,
                num_cache_hits: 0,
            },
        };
        workers.spawn(async move {
            cancel.run_until_cancelled(retrieve.run_inner()).await;
            retrieve.log_metrics();
        });
        drop(self.channels.tx_post);
        while let Some(res) = workers.join_next().await {
            res?;
        }
        let db = Arc::into_inner(self.db);
        assert!(db.is_some());
        drop(db);
        DB::destroy(&Default::default(), self.temp_dir.keep().as_path())?;
        Ok(())
    }
}

struct RetrieveWorker {
    rx_message: UnboundedReceiver<Message>,
    rx_fetch: UnboundedReceiver<HashSet<Vec<u8>>>,
    rx_post: UnboundedReceiver<Vec<(Vec<u8>, Option<Vec<u8>>)>>,

    fetched_handle: FetchedHandle,
    post_done_handle: PostDoneHandle,
    network_interconnect: NetworkInterconnectHandle,
    tx_fetch_dispatch: flume::Sender<(Vec<u8>, flume::Sender<(Vec<u8>, Option<(Vec<u8>, u32)>)>)>,

    config: BigStorageConfig,
    storing_shards: HashSet<u32>,
    pushing_shards: HashSet<u32>,
    version: u64,
    reorder_push_values: HashMap<u64, Vec<message::PushValue>>,

    update_table: HashMap<Vec<u8>, Option<Vec<u8>>>,
    merkle_trees: HashMap<u32, MerkleTree>,
    merkle_roots: Vec<MerkleHash>,
    cache: LruCache<Vec<u8>, Option<Vec<u8>>>,

    metrics: RetrieveWorkerMetrics,
}

struct RetrieveWorkerMetrics {
    num_keys: u64,
    num_update_hits: u64,
    num_cache_hits: u64,
}

impl RetrieveWorker {
    fn log_metrics(&self) {
        info!(
            "storage retrieve metrics: total keys {}, update hits {}, cache hits {}, cache miss {}%",
            self.metrics.num_keys,
            self.metrics.num_update_hits,
            self.metrics.num_cache_hits,
            (self.metrics.num_keys - self.metrics.num_cache_hits - self.metrics.num_update_hits)
                as f64
                / self.metrics.num_keys as f64
                * 100.0
        );
    }

    async fn handle_fetch(&mut self, keys: HashSet<Vec<u8>>) -> anyhow::Result<()> {
        self.version += 1;
        debug!("version {} start", self.version);

        let mut state = HashMap::new();
        let mut pending_shards = HashSet::new();
        let (tx, rx) = flume::unbounded();
        for key in keys {
            self.metrics.num_keys += 1;
            if let Some(value) = self.update_table.get(&key) {
                self.metrics.num_update_hits += 1;
                state.insert(key, value.clone());
                continue;
            }

            if let Some(value) = self.cache.get(&key) {
                self.metrics.num_cache_hits += 1;
                state.insert(key.clone(), value.clone());
                continue;
            }

            let shard = self.config.shard_of_key(&key);
            if !self.storing_shards.contains(&shard) {
                // if !self.pushing_shards.contains(&shard) {
                pending_shards.insert(shard);
                continue;
            }
            let _ = self.tx_fetch_dispatch.send((key, tx.clone()));
        }
        drop(tx);
        let mut pushing_shard_states = self
            .pushing_shards
            .iter()
            .map(|&shard| (shard, Vec::new()))
            .collect::<HashMap<_, _>>();
        while let Ok((key, value)) = rx.recv() {
            let shard = self.config.shard_of_key(&key);
            if let Some(shard_state) = pushing_shard_states.get_mut(&shard) {
                shard_state.push((
                    key.clone(),
                    value.as_ref().map(|(v, index)| {
                        let proof = self.merkle_trees[&shard].prove(*index as usize);
                        // let proof = MerkleProof { siblings: vec![] };
                        (v.clone(), proof)
                    }),
                ));
            }
            let value = value.map(|(v, _)| v);
            state.insert(key.clone(), value.clone());
            self.cache.put(key, value);
        }
        debug!("version {} storage done", self.version);
        let push_value = message::PushValue {
            version: self.version,
            state: pushing_shard_states
                .into_iter()
                .filter(|(_, state)| !state.is_empty())
                .collect(),
        };
        debug!(
            "version {} pushing to network {} shards",
            self.version,
            push_value.state.len()
        );
        let message = Message::PushValue(push_value);
        self.network_interconnect.send_to_all(message);

        if let Some(push_values) = self.reorder_push_values.remove(&self.version) {
            for push_value in push_values {
                self.insert_shards(&mut state, &mut pending_shards, push_value.state);
                debug!(
                    "version {} reordered push value applied; {} shards left",
                    self.version,
                    pending_shards.len()
                );
            }
        }
        while !pending_shards.is_empty() {
            let Some(message) = self.rx_message.recv().await else {
                anyhow::bail!("storage channel closed");
            };
            match message {
                Message::PushValue(push_value) => {
                    if push_value.version > self.version {
                        debug!(
                            "version {} push value reordered; waiting for version {}",
                            self.version, push_value.version
                        );
                        self.reorder_push_values
                            .entry(push_value.version)
                            .or_default()
                            .push(push_value);
                        continue;
                    }
                    if push_value.version == self.version {
                        self.insert_shards(&mut state, &mut pending_shards, push_value.state);
                        debug!(
                            "version {} push value applied; {} shards left",
                            self.version,
                            pending_shards.len()
                        );
                    }
                }
            }
        }
        debug!("version {} network done", self.version);
        let _ = self.fetched_handle.tx_fetched.send(state);
        Ok(())
    }

    fn insert_shards(
        &mut self,
        state: &mut HashMap<Vec<u8>, Option<Vec<u8>>>,
        pending_shards: &mut HashSet<u32>,
        shard_states: Vec<(u32, Vec<(Vec<u8>, Option<(Vec<u8>, MerkleProof)>)>)>,
    ) {
        for (shard, entries) in shard_states {
            if !pending_shards.remove(&shard) {
                continue;
            }
            let root = self.merkle_roots[shard as usize];
            for (key, value_proof) in entries {
                let value = if let Some((value, proof)) = value_proof {
                    let mut hasher = digest::Context::new(&digest::SHA256);
                    hasher.update(&key);
                    hasher.update(&value);
                    hasher.update(&0u32.to_le_bytes());
                    let leaf = hasher.finish();
                    let leaf = leaf.as_ref().try_into().unwrap();
                    proof
                        .verify(leaf, &root)
                        .expect("Merkle proof verification failed");
                    // let _ = proof.verify(leaf, &Default::default());
                    Some(value)
                } else {
                    None
                };
                state.insert(key.clone(), value.clone());
                self.cache.put(key, value);
            }
        }
    }

    fn handle_post(&mut self, posts: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> anyhow::Result<()> {
        for (key, value) in posts {
            self.cache.demote(&key);
            self.update_table.insert(key, value);
        }
        let _ = self.post_done_handle.tx_post_done.send(());
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            select! {
                Some(keys) = self.rx_fetch.recv() => {
                    self.handle_fetch(keys).await?;
                }
                Some(posts) = self.rx_post.recv() => {
                    self.handle_post(posts)?;
                }
            }
        }
    }
}

mod message {
    use bincode::{Decode, Encode};

    use crate::merkle::MerkleProof;

    #[derive(Decode, Encode)]
    pub enum Message {
        PushValue(PushValue),
    }

    #[derive(Decode, Encode)]
    pub struct PushValue {
        pub version: u64,
        pub state: Vec<(u32, Vec<(Vec<u8>, Option<(Vec<u8>, MerkleProof)>)>)>,
    }
}
