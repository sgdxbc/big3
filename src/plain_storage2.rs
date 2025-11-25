use std::sync::Arc;

use hashbrown::HashMap;
use rocksdb::{DB, WriteBatch};
use tempfile::{TempDir, tempdir};
use tokio::{
    process::Command,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};

use crate::{
    common::PREFILL_PATH,
    storage2::{FetchedHandle, PostDoneHandle, StorageChannels},
};

pub struct PlainStorageTask {
    pub channels: StorageChannels,
    fetched_handle: FetchedHandle,
    post_done_handle: PostDoneHandle,

    temp_dir: TempDir,
    db: DB,
}

impl PlainStorageTask {
    fn new(
        channels: StorageChannels,
        fetched_handle: FetchedHandle,
        post_done_handle: PostDoneHandle,
        temp_dir: TempDir,
        db: DB,
    ) -> Self {
        Self {
            channels,
            fetched_handle,
            post_done_handle,
            temp_dir,
            db,
        }
    }

    pub async fn load(
        channels: StorageChannels,
        fetched_handle: FetchedHandle,
        post_done_handle: PostDoneHandle,
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
            fetched_handle,
            post_done_handle,
            temp_dir,
            db,
        ))
    }

    const NUM_GET_WORKER_THREADS: usize = 20;

    pub async fn run(mut self) -> anyhow::Result<()> {
        drop(self.channels.tx_fetch);
        drop(self.channels.tx_post);

        let db = Arc::new(self.db);
        let mut join_set = JoinSet::new();

        let (tx_get, rx_get) = flume::unbounded();
        for _ in 0..Self::NUM_GET_WORKER_THREADS {
            let db = db.clone();
            let rx_get = rx_get.clone();
            join_set.spawn_blocking(move || Self::get_worker(db, rx_get));
        }

        join_set.spawn(async move {
            while let Some(keys) = self.channels.rx_fetch.recv().await {
                let (tx, rx) = flume::unbounded();
                for key in keys {
                    let _ = tx_get.send((key, tx.clone()));
                }
                drop(tx);
                let mut state = HashMap::new();
                while let Ok((key, value)) = rx.recv() {
                    state.insert(key, value);
                }
                let _ = self.fetched_handle.tx_fetched.send(state);
            }
            Ok(())
        });

        {
            let db = db.clone();
            join_set.spawn_blocking(move || {
                Self::write_worker(
                    db,
                    self.channels.rx_post,
                    self.post_done_handle.tx_post_done,
                )
            });
        }
        while let Some(res) = join_set.join_next().await {
            res??;
        }
        // stats if any
        // sleep(Duration::from_millis(500)).await;
        // self.temp_dir.close()?;
        let db = Arc::into_inner(db);
        assert!(db.is_some());
        drop(db);
        DB::destroy(&Default::default(), self.temp_dir.keep().as_path())?;
        Ok(())
    }

    fn get_worker(
        db: Arc<DB>,
        rx_key: flume::Receiver<(Vec<u8>, flume::Sender<(Vec<u8>, Option<Vec<u8>>)>)>,
    ) -> anyhow::Result<()> {
        while let Ok((key, tx)) = rx_key.recv() {
            let value = db.get(&key)?;
            let _ = tx.send((key, value));
        }
        Ok(())
    }

    fn write_worker(
        db: Arc<DB>,
        mut rx_updates: UnboundedReceiver<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
        tx_write_done: UnboundedSender<()>,
    ) -> anyhow::Result<()> {
        while let Some(updates) = rx_updates.blocking_recv() {
            let mut batch = WriteBatch::new();
            for (key, value) in updates {
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
}
