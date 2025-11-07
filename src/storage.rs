use std::{
    hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher},
    sync::Arc,
    time::Duration,
};

use rand::{SeedableRng as _, rngs::StdRng, seq::IteratorRandom as _};
use rocksdb::{DB, WriteBatch};
use rustc_hash::{FxHashMap, FxHashSet};
use tempfile::{TempDir, tempdir};
use tokio::{
    process::Command,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        oneshot,
    },
    task::JoinSet,
    time::sleep,
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::{NodeIndex, PREFILL_PATH},
    network::interconnect::{NetworkInterconnectHandle, ReceiveHandle},
    schema,
};

use self::message::Message;

pub struct StorageWorkersChannels {
    pub tx_fetch: flume::Sender<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    pub rx_fetch: flume::Receiver<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,

    pub tx_post: UnboundedSender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
    pub rx_post: UnboundedReceiver<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
}

pub struct StorageWorkersHandle {
    pub tx_fetch: flume::Sender<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    pub tx_post: UnboundedSender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
}

impl Default for StorageWorkersChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageWorkersChannels {
    pub fn new() -> Self {
        let (tx_fetch, rx_fetch) = flume::unbounded();
        let (tx_post, rx_post) = unbounded_channel();
        Self {
            tx_fetch,
            rx_fetch,
            tx_post,
            rx_post,
        }
    }

    pub fn handle(&self) -> StorageWorkersHandle {
        StorageWorkersHandle {
            tx_fetch: self.tx_fetch.clone(),
            tx_post: self.tx_post.clone(),
        }
    }
}

#[derive(Clone)]
pub struct BigStorageConfig {
    num_nodes: NodeIndex,
    // num_faulty_nodes: NodeIndex,
    num_stripes: u32,
    num_secondary_nodes: NodeIndex,
}

impl From<&schema::ReplicaConfig> for BigStorageConfig {
    fn from(value: &schema::ReplicaConfig) -> Self {
        let config = Self {
            num_nodes: value.num_nodes,
            // num_faulty_nodes: value.num_faulty_nodes,
            num_stripes: 100,
            num_secondary_nodes: 6,
        };
        let num_faulty_nodes = value.num_faulty_nodes;
        assert!((0..config.num_shards()).all(|shard| {
            config.primary_node_of_shard(shard) < config.num_nodes - num_faulty_nodes
                || config
                    .secondary_nodes_of_shard(shard)
                    .any(|n| n < config.num_nodes - num_faulty_nodes)
        }));
        config
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
                // &mut StdRng::seed_from_u64(shard as _),
                &mut StdRng::seed_from_u64((shard % self.num_stripes) as _),
                self.num_secondary_nodes as _,
            )
            .into_iter()
            .map(move |n| n + (n >= self.primary_node_of_shard(shard)) as NodeIndex)
    }
}

pub struct BigStorageWorkerChannels {
    tx_key: flume::Sender<(FetchSeq, Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    rx_key: flume::Receiver<(FetchSeq, Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,

    tx_message: UnboundedSender<Message>,
    rx_message: UnboundedReceiver<Message>,
}

impl Default for BigStorageWorkerChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl BigStorageWorkerChannels {
    pub fn new() -> Self {
        let (tx_key, rx_key) = flume::unbounded();
        let (tx_message, rx_message) = unbounded_channel();
        Self {
            tx_key,
            rx_key,
            tx_message,
            rx_message,
        }
    }

    pub fn receive_handle(&self) -> ReceiveHandle<Message> {
        ReceiveHandle::new(self.tx_message.clone())
    }
}

pub struct BigStorageWorkersTask {
    pub channels: StorageWorkersChannels,
    pub big_channels: BigStorageWorkerChannels,
    tx_post_done: UnboundedSender<u64>,
    network_interconnect: NetworkInterconnectHandle,

    config: BigStorageConfig,
    // node_index: NodeIndex,
    primary_shards: FxHashSet<u32>,
    secondary_shards: FxHashSet<u32>,
    temp_dir: TempDir,
    db: DB,
}

type FetchSeq = u64;

impl BigStorageWorkersTask {
    #[allow(clippy::too_many_arguments)]
    fn new(
        channels: StorageWorkersChannels,
        big_channels: BigStorageWorkerChannels,
        tx_post_done: UnboundedSender<u64>,
        network_interconnect: NetworkInterconnectHandle,
        config: BigStorageConfig,
        node_index: NodeIndex,
        temp_dir: TempDir,
        db: DB,
    ) -> Self {
        let primary_shards = (0..config.num_shards())
            .filter(|&shard| config.primary_node_of_shard(shard) == node_index)
            .collect();
        let secondary_shards = (0..config.num_shards())
            .filter(|&shard| {
                config
                    .secondary_nodes_of_shard(shard)
                    .any(|n| n == node_index)
            })
            .collect();

        Self {
            channels,
            big_channels,
            tx_post_done,
            network_interconnect,
            config,
            // node_index,
            primary_shards,
            secondary_shards,
            temp_dir,
            db,
        }
    }

    pub async fn load(
        channels: StorageWorkersChannels,
        big_channels: BigStorageWorkerChannels,
        tx_post_done: UnboundedSender<u64>,
        network_interconnect: NetworkInterconnectHandle,
        config: BigStorageConfig,
        node_index: NodeIndex,
    ) -> anyhow::Result<Self> {
        let temp_dir = tempdir()?;
        let status = Command::new("cp")
            .arg("-rT")
            .arg(PREFILL_PATH)
            .arg(temp_dir.path())
            .status()
            .await?;
        anyhow::ensure!(status.success(), "failed to copy prefill data");
        let db = DB::open_default(temp_dir.path())?;
        Ok(Self::new(
            channels,
            big_channels,
            tx_post_done,
            network_interconnect,
            config,
            node_index,
            temp_dir,
            db,
        ))
    }

    const NUM_GET_WORKER_THREADS: usize = 20;

    pub async fn run(self, cancel: CancellationToken) -> anyhow::Result<()> {
        drop(self.channels.tx_fetch);
        drop(self.channels.tx_post);

        let db = Arc::new(self.db);
        let shards = &self.primary_shards | &self.secondary_shards;

        let mut join_set = JoinSet::new();
        for _ in 0..Self::NUM_GET_WORKER_THREADS {
            let db = db.clone();
            let config = self.config.clone();
            let rx_key = self.big_channels.rx_key.clone();
            let network_interconnect = self.network_interconnect.clone();
            let shards = shards.clone();
            join_set.spawn_blocking(move || {
                Self::get_worker(db, config, shards, rx_key, network_interconnect)
            });
        }
        {
            let db = db.clone();
            let config = self.config.clone();
            let shards = shards.clone();
            join_set.spawn_blocking(move || {
                Self::write_worker(db, config, shards, self.channels.rx_post, self.tx_post_done)
            });
        }
        let retrieve = BigStorageRetrieveWorkerTask::new(
            self.channels.rx_fetch,
            self.big_channels.rx_message,
            self.big_channels.tx_key,
            self.config,
            shards,
        );
        join_set.spawn(async move { retrieve.run(cancel).await });
        while let Some(res) = join_set.join_next().await {
            res??;
        }
        // stats if any
        sleep(Duration::from_millis(100)).await;
        self.temp_dir.close()?;
        Ok(())
    }

    fn get_worker(
        db: Arc<DB>,
        config: BigStorageConfig,
        shards: FxHashSet<u32>,
        rx_key: flume::Receiver<(FetchSeq, Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
        network_interconnect: NetworkInterconnectHandle,
    ) -> anyhow::Result<()> {
        while let Ok((seq, key, tx)) = rx_key.recv() {
            assert!(shards.contains(&config.shard_of_key(&key)));
            let value = db.get(&key)?;
            let _ = tx.send(value.clone());

            let push_value = message::PushValue { seq, key, value };
            network_interconnect.send_to_all(Message::PushValue(push_value));
        }
        Ok(())
    }

    fn write_worker(
        db: Arc<DB>,
        config: BigStorageConfig,
        shards: FxHashSet<u32>,
        mut rx_updates: UnboundedReceiver<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
        tx_write_done: UnboundedSender<u64>,
    ) -> anyhow::Result<()> {
        let mut count = 0;
        let mut updates_buf = Vec::new();
        while rx_updates.blocking_recv_many(&mut updates_buf, 10_000) > 0 {
            count += updates_buf.len() as u64;
            let mut batch = WriteBatch::new();
            for updates in updates_buf.drain(..) {
                for (key, value) in updates {
                    if !shards.contains(&config.shard_of_key(&key)) {
                        continue;
                    }
                    match value {
                        Some(v) => batch.put(key, v),
                        None => batch.delete(key),
                    }
                }
            }

            db.write(batch)?;
            let _ = tx_write_done.send(count);
        }
        Ok(())
    }
}

pub struct BigStorageRetrieveWorkerTask {
    rx_fetch: flume::Receiver<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    rx_message: UnboundedReceiver<Message>,
    tx_key: flume::Sender<(FetchSeq, Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,

    config: BigStorageConfig,
    shards: FxHashSet<u32>,

    retrieving: FxHashMap<FetchSeq, (Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    fetch_seq: FetchSeq,
    reorder_push_value: FxHashMap<FetchSeq, message::PushValue>,
}

impl BigStorageRetrieveWorkerTask {
    pub fn new(
        rx_fetch: flume::Receiver<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
        rx_message: UnboundedReceiver<Message>,
        tx_key: flume::Sender<(FetchSeq, Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
        config: BigStorageConfig,
        shards: FxHashSet<u32>,
    ) -> Self {
        Self {
            rx_fetch,
            rx_message,
            tx_key,
            config,
            shards,
            retrieving: Default::default(),
            fetch_seq: 0,
            reorder_push_value: Default::default(),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        cancel.run_until_cancelled(self.run_inner()).await;
        Ok(())
    }

    async fn run_inner(&mut self) {
        loop {
            tokio::select! {
                Ok((key, tx)) = self.rx_fetch.recv_async() => {
                    self.handle_fetch(key, tx);
                }
                Some(message) = self.rx_message.recv() => {
                    self.handle_message(message);
                }
            }
        }
    }

    fn handle_fetch(&mut self, key: Vec<u8>, tx: oneshot::Sender<Option<Vec<u8>>>) {
        self.fetch_seq += 1;
        if self.shards.contains(&self.config.shard_of_key(&key)) {
            let _ = self.tx_key.send((self.fetch_seq, key, tx));
        } else {
            self.retrieving.insert(self.fetch_seq, (key, tx));
        }

        if let Some(push_value) = self.reorder_push_value.remove(&self.fetch_seq) {
            self.handle_push_value(push_value);
        }
    }

    fn handle_message(&mut self, message: Message) {
        match message {
            Message::PushValue(push_value) => {
                self.handle_push_value(push_value);
            }
        }
    }

    fn handle_push_value(&mut self, push_value: message::PushValue) {
        if push_value.seq > self.fetch_seq {
            self.reorder_push_value.insert(push_value.seq, push_value);
            return;
        }
        if let Some((key, tx)) = self.retrieving.remove(&push_value.seq) {
            assert_eq!(key, push_value.key);
            let _ = tx.send(push_value.value);
        }
    }
}

mod message {
    use bincode::{Decode, Encode};

    use super::FetchSeq;

    #[derive(Decode, Encode)]
    pub enum Message {
        PushValue(PushValue),
    }

    #[derive(Decode, Encode)]
    pub struct PushValue {
        pub seq: FetchSeq,
        pub key: Vec<u8>,
        pub value: Option<Vec<u8>>,
    }
}
