use std::sync::Arc;

use rocksdb::{DB, WriteBatch};
use tempfile::{TempDir, tempdir};
use tokio::{
    process::Command,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        oneshot,
    },
    task::JoinSet,
};

use crate::common::PREFILL_PATH;

pub struct StorageWorkersChannels {
    tx_fetch: flume::Sender<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    rx_fetch: flume::Receiver<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,

    tx_post: UnboundedSender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
    rx_post: UnboundedReceiver<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
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

pub struct StorageWorkersTask {
    pub channels: StorageWorkersChannels,
    tx_post_done: UnboundedSender<u64>,

    temp_dir: TempDir,
    db: DB,
}

impl StorageWorkersTask {
    fn new(
        channels: StorageWorkersChannels,
        tx_post_done: UnboundedSender<u64>,
        temp_dir: TempDir,
        db: DB,
    ) -> Self {
        Self {
            channels,
            tx_post_done,
            temp_dir,
            db,
        }
    }

    pub async fn load(
        channels: StorageWorkersChannels,
        tx_post_done: UnboundedSender<u64>,
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
        Ok(Self::new(channels, tx_post_done, temp_dir, db))
    }

    const NUM_GET_WORKER_THREADS: usize = 20;

    pub async fn run(self) -> anyhow::Result<()> {
        drop(self.channels.tx_fetch);
        drop(self.channels.tx_post);

        let db = Arc::new(self.db);
        let mut join_set = JoinSet::new();
        for _ in 0..Self::NUM_GET_WORKER_THREADS {
            let db = db.clone();
            let rx_get = self.channels.rx_fetch.clone();
            join_set.spawn_blocking(move || Self::get_worker(db, rx_get));
        }
        let db = db.clone();
        join_set.spawn_blocking(move || {
            Self::write_worker(db, self.channels.rx_post, self.tx_post_done)
        });
        while let Some(res) = join_set.join_next().await {
            res??;
        }
        // stats if any
        self.temp_dir.close()?;
        Ok(())
    }

    fn get_worker(
        db: Arc<DB>,
        rx_key: flume::Receiver<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    ) -> anyhow::Result<()> {
        while let Ok((key, tx)) = rx_key.recv() {
            let value = db.get(key)?;
            let _ = tx.send(value);
        }
        Ok(())
    }

    fn write_worker(
        db: Arc<DB>,
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
