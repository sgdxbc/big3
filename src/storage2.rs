use std::sync::Arc;

use rocksdb::{DB, WriteBatch};
use rustc_hash::{FxHashMap, FxHashSet};
use tempfile::tempdir;
use tokio::{
    process::Command,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::{NodeIndex, PREFILL_PATH},
    network::interconnect::NetworkInterconnectHandle,
    storage::BigStorageConfig,
};

pub use message::Message;

pub struct StorageChannels {
    tx_fetch: UnboundedSender<FxHashSet<Vec<u8>>>,
    rx_fetch: UnboundedReceiver<FxHashSet<Vec<u8>>>,

    tx_post: UnboundedSender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
    rx_post: UnboundedReceiver<Vec<(Vec<u8>, Option<Vec<u8>>)>>,

    tx_message: UnboundedSender<Message>,
    rx_message: UnboundedReceiver<Message>,
}

pub struct FetchedHandle {
    pub tx_fetched: UnboundedSender<FxHashMap<Vec<u8>, Option<Vec<u8>>>>,
}

pub struct PostDoneHandle {
    pub tx_post_done: UnboundedSender<()>,
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
}

pub struct StorageTask {
    channels: StorageChannels,
    fetched_handle: FetchedHandle,
    post_done_handle: PostDoneHandle,
    network_interconnect: NetworkInterconnectHandle,

    config: BigStorageConfig,
    #[allow(dead_code)]
    node_index: NodeIndex,
    storing_shards: FxHashSet<u32>,
    pushing_shards: FxHashSet<u32>,

    temp_dir: tempfile::TempDir,
    db: Arc<DB>,

    tx_fetch_dispatch: flume::Sender<(Vec<u8>, flume::Sender<(Vec<u8>, Option<Vec<u8>>)>)>,
    rx_fetch_dispatch: flume::Receiver<(Vec<u8>, flume::Sender<(Vec<u8>, Option<Vec<u8>>)>)>,
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
    ) -> anyhow::Result<Self> {
        let temp_dir = tempdir()?;
        let status = Command::new("cp")
            .arg("-rT")
            .arg(PREFILL_PATH)
            .arg(temp_dir.path())
            .status()
            .await?;
        anyhow::ensure!(status.success(), "failed to copy prefill data");
        let mut db = DB::open_default(temp_dir.path())?;
        db.create_cf("archive", &Default::default())?;

        let (tx_fetch_dispatch, rx_fetch_dispatch) = flume::unbounded();

        let storing_shards = config.storing_shards(node_index);
        let pushing_shards = config.pushing_shards(node_index);
        Ok(Self {
            channels,
            fetched_handle,
            post_done_handle,
            network_interconnect,
            config,
            node_index,
            storing_shards,
            pushing_shards,
            temp_dir,
            db: Arc::new(db),
            tx_fetch_dispatch,
            rx_fetch_dispatch,
        })
    }

    fn get_worker(
        db: Arc<DB>,
        config: BigStorageConfig,
        rx_key: flume::Receiver<(Vec<u8>, flume::Sender<(Vec<u8>, Option<Vec<u8>>)>)>,
    ) -> anyhow::Result<()> {
        while let Ok((key, tx)) = rx_key.recv() {
            // assert!(pushing_shards.contains(&config.shard_of_key(&key)));
            let shard = config.shard_of_key(&key);
            let value = db.get([&shard.to_be_bytes()[..], &key].concat())?;
            let _ = tx.send((key, value.clone()));
        }
        Ok(())
    }

    fn write_worker(
        db: Arc<DB>,
        config: BigStorageConfig,
        storing_shards: FxHashSet<u32>,
        mut rx_updates: UnboundedReceiver<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
        tx_write_done: UnboundedSender<()>,
    ) -> anyhow::Result<()> {
        while let Some(updates) = rx_updates.blocking_recv() {
            let mut batch = WriteBatch::new();
            for (key, value) in updates {
                let shard = config.shard_of_key(&key);
                if !storing_shards.contains(&shard) {
                    continue;
                }
                let key = [&shard.to_be_bytes()[..], &key[..]].concat();
                match value {
                    Some(v) => batch.put(key, v),
                    None => batch.delete(key),
                }
            }

            db.write(batch)?;
            let _ = tx_write_done.send(());
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

        {
            let db = self.db.clone();
            let config = self.config.clone();
            let storing_shards = self.storing_shards.clone();
            workers.spawn_blocking(move || {
                Self::write_worker(
                    db,
                    config,
                    storing_shards,
                    self.channels.rx_post,
                    self.post_done_handle.tx_post_done,
                )
                .unwrap();
            });
        }

        let mut retrieve = RetrieveWorker {
            rx_message: self.channels.rx_message,
            rx_fetch: self.channels.rx_fetch,
            fetched_handle: self.fetched_handle,
            network_interconnect: self.network_interconnect,
            tx_fetch_dispatch: self.tx_fetch_dispatch,
            config: self.config,
            storing_shards: self.storing_shards,
            pushing_shards: self.pushing_shards,
            version: 0,
            reorder_push_values: FxHashMap::default(),
        };
        workers.spawn(async move {
            cancel.run_until_cancelled(retrieve.run_inner()).await;
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
    rx_fetch: UnboundedReceiver<FxHashSet<Vec<u8>>>,

    fetched_handle: FetchedHandle,
    network_interconnect: NetworkInterconnectHandle,
    tx_fetch_dispatch: flume::Sender<(Vec<u8>, flume::Sender<(Vec<u8>, Option<Vec<u8>>)>)>,

    config: BigStorageConfig,
    storing_shards: FxHashSet<u32>,
    pushing_shards: FxHashSet<u32>,
    version: u64,
    reorder_push_values: FxHashMap<u64, Vec<message::PushValue>>,
}

impl RetrieveWorker {
    async fn handle_fetch(&mut self, keys: FxHashSet<Vec<u8>>) -> anyhow::Result<()> {
        self.version += 1;

        let (tx, rx) = flume::unbounded();
        for key in keys {
            let shard = self.config.shard_of_key(&key);
            if !self.storing_shards.contains(&shard) {
                continue;
            }
            let _ = self.tx_fetch_dispatch.send((key, tx.clone()));
        }
        drop(tx);
        let mut shard_states = FxHashMap::<_, Vec<_>>::default();
        while let Ok((key, value)) = rx.recv() {
            let shard = self.config.shard_of_key(&key);
            shard_states.entry(shard).or_default().push((key, value));
        }
        for (shard, state) in &shard_states {
            if self.pushing_shards.contains(shard) {
                let push_value = message::PushValue {
                    version: self.version,
                    shard: *shard,
                    state: state.clone(),
                };
                let message = Message::PushValue(push_value);
                self.network_interconnect.send_to_all(message);
            }
        }

        if let Some(push_values) = self.reorder_push_values.remove(&self.version) {
            for push_value in push_values {
                shard_states.insert(push_value.shard, push_value.state);
            }
        }
        while shard_states.len() < self.config.num_shards() as usize {
            let Some(message) = self.rx_message.recv().await else {
                anyhow::bail!("storage channel closed");
            };
            match message {
                Message::PushValue(push_value) => {
                    if push_value.version > self.version {
                        self.reorder_push_values
                            .entry(push_value.version)
                            .or_default()
                            .push(push_value);
                        continue;
                    }
                    if push_value.version == self.version {
                        shard_states.insert(push_value.shard, push_value.state);
                    }
                }
            }
        }
        let state = shard_states
            .into_values()
            .flatten()
            .collect::<FxHashMap<_, _>>();
        let _ = self.fetched_handle.tx_fetched.send(state);
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(keys) = self.rx_fetch.recv().await {
            self.handle_fetch(keys).await?;
        }
        Ok(())
    }
}

mod message {
    use bincode::{Decode, Encode};

    #[derive(Decode, Encode)]
    pub enum Message {
        PushValue(PushValue),
    }

    #[derive(Decode, Encode)]
    pub struct PushValue {
        pub version: u64,
        pub shard: u32,
        pub state: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    }
}
