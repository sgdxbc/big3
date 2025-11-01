use rocksdb::DB;
use tempfile::{TempDir, tempdir};
use tokio::{
    process::Command,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};
use tokio_util::sync::CancellationToken;

use crate::{
    storage::{PlainStorage, StorageOp},
    tasks::PREFILL_PATH,
};

use super::{RequestId, ResponseContext};

#[derive(Clone)]
pub struct StorageHandle {
    tx_storage_op: UnboundedSender<StorageOp>,
}

impl StorageHandle {
    pub fn new(tx_storage_op: UnboundedSender<StorageOp>) -> Self {
        Self { tx_storage_op }
    }

    pub fn post(&self, updates: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> anyhow::Result<()> {
        self.tx_storage_op.send(StorageOp::Post(updates))?;
        anyhow::Ok(())
    }
}

pub struct StorageContext {
    storage: StorageHandle,
    tx_fetch_response: UnboundedSender<(RequestId, Vec<Option<Vec<u8>>>)>,
    fetch_id: RequestId,
}

impl StorageContext {
    pub fn new(
        storage: StorageHandle,
        tx_fetch_response: UnboundedSender<(RequestId, Vec<Option<Vec<u8>>>)>,
    ) -> Self {
        Self {
            storage,
            tx_fetch_response,
            fetch_id: 0,
        }
    }

    pub fn fetch(&mut self, keys: Vec<Vec<u8>>) -> RequestId {
        self.fetch_id += 1;
        let context = ResponseContext::new(self.fetch_id, self.tx_fetch_response.clone());
        let op = StorageOp::Fetch(keys, context);
        let _ = self.storage.tx_storage_op.send(op);
        self.fetch_id
    }

    pub fn post(&self, updates: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        let _ = self.storage.post(updates);
    }
}

pub struct PlainStorageChannels {
    tx_storage_op: UnboundedSender<StorageOp>,
    rx_storage_op: UnboundedReceiver<StorageOp>,
}

impl Default for PlainStorageChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl PlainStorageChannels {
    pub fn new() -> Self {
        let (tx_storage_op, rx_storage_op) = unbounded_channel();
        Self {
            tx_storage_op,
            rx_storage_op,
        }
    }

    pub fn handle(&self) -> StorageHandle {
        StorageHandle {
            tx_storage_op: self.tx_storage_op.clone(),
        }
    }
}

pub struct PlainStorageTask {
    pub channels: PlainStorageChannels,
    state: PlainStorage,
    _temp_dir: TempDir,
}

impl PlainStorageTask {
    fn new(channels: PlainStorageChannels, state: PlainStorage, temp_dir: TempDir) -> Self {
        Self {
            channels,
            state,
            _temp_dir: temp_dir,
        }
    }

    pub async fn load(channels: PlainStorageChannels) -> anyhow::Result<Self> {
        let temp_dir = tempdir()?;
        let status = Command::new("cp")
            .arg("-rT")
            .arg(PREFILL_PATH)
            .arg(temp_dir.path())
            .status()
            .await?;
        anyhow::ensure!(status.success(), "failed to copy prefill data");
        let db = DB::open_default(temp_dir.path())?;
        // use rocksdb::Options;
        // let mut db_opts = Options::default();
        // db_opts.create_if_missing(true);
        // // Explicitly include the "default" CF so we get a handle for it
        // let default_cf_opts = Options::default();
        // let cfs = vec![rocksdb::ColumnFamilyDescriptor::new(
        //     "default",
        //     default_cf_opts,
        // )];
        // let db = DB::open_cf_descriptors(&db_opts, temp_dir.path(), cfs)?;

        let state = PlainStorage::new(db)?;
        Ok(Self::new(channels, state, temp_dir))
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move {
            stop.run_until_cancelled(self.run_inner()).await;
            self.state.log_metrics();
        })
        .await?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(op) = self.channels.rx_storage_op.recv().await {
            self.state.invoke(op)?;
        }
        Ok(())
    }
}
