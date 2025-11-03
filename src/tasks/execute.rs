use rustc_hash::FxHashSet;
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};
use tokio_util::sync::CancellationToken;

use crate::{
    consensus::Block,
    execute::{Execute, ExecuteContext, FetchId},
    schema,
    storage::FetchResponse,
    types::{ClientId, Reply},
};

use super::{
    ResponseContext,
    network::server::NetworkOutgoingHandle,
    storage::{StorageContext, StorageHandle},
};

pub struct ExecuteChannels {
    tx_blocks: UnboundedSender<(Vec<Block>, ResponseContext<()>)>,
    rx_blocks: UnboundedReceiver<(Vec<Block>, ResponseContext<()>)>,

    tx_fetch_response: UnboundedSender<(FetchId, FetchResponse)>,
    rx_fetch_response: UnboundedReceiver<(FetchId, FetchResponse)>,
}

#[derive(Clone)]
pub struct ExecuteHandle {
    pub tx_blocks: UnboundedSender<(Vec<Block>, ResponseContext<()>)>,
}

impl Default for ExecuteChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecuteChannels {
    pub fn new() -> Self {
        let (tx_blocks, rx_blocks) = unbounded_channel();
        let (tx_fetch_response, rx_fetch_response) = unbounded_channel();
        Self {
            tx_blocks,
            rx_blocks,
            tx_fetch_response,
            rx_fetch_response,
        }
    }

    pub fn handle(&self) -> ExecuteHandle {
        ExecuteHandle {
            tx_blocks: self.tx_blocks.clone(),
        }
    }

    fn storage_context(&self, storage: StorageHandle) -> StorageContext {
        StorageContext::new(storage, self.tx_fetch_response.clone())
    }
}

pub struct ExecuteTask {
    channels: ExecuteChannels,
    state: Execute<ExecuteTaskContext>,
}

impl ExecuteTask {
    fn new(channels: ExecuteChannels, state: Execute<ExecuteTaskContext>) -> Self {
        Self { channels, state }
    }

    pub async fn load(
        channels: ExecuteChannels,
        storage: StorageHandle,
        network_outgoing: NetworkOutgoingHandle,
        schema: &schema::ReplicaTask,
    ) -> anyhow::Result<Self> {
        let context = ExecuteTaskContext::new(channels.storage_context(storage), network_outgoing);
        let state = Execute::new(context, (&schema.config).into(), schema.node_index);
        Ok(Self::new(channels, state))
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move {
            stop.run_until_cancelled(self.run_inner()).await;
            self.state.log_metrics();
        })
        .await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        loop {
            select! {
                Some((fetch_id, values)) = self.channels.rx_fetch_response.recv() => {
                    self.state.on_fetch_response(fetch_id, values);
                }
                Some((block, tx_response)) = self.channels.rx_blocks.recv() => {
                    self.state.on_blocks(block, tx_response);
                }
            }
        }
    }
}

struct ExecuteTaskContext {
    storage: StorageContext,
    network_outgoing: NetworkOutgoingHandle,
}

impl ExecuteTaskContext {
    fn new(storage: StorageContext, network_outgoing: NetworkOutgoingHandle) -> Self {
        Self {
            storage,
            network_outgoing,
        }
    }
}

impl ExecuteContext for ExecuteTaskContext {
    fn send(&mut self, id: ClientId, reply: Reply) {
        let _ = self.network_outgoing.send_message(id, reply);
    }

    fn fetch(&mut self, keys: FxHashSet<Vec<u8>>) -> FetchId {
        self.storage.fetch(keys)
    }

    fn post(&mut self, updates: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        self.storage.post(updates);
    }
}
