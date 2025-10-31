use tokio::{
    select,
    sync::mpsc::{
        Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{
    consensus::Block,
    execute::{Execute, ExecuteContext, FetchId},
    schema,
    types::{ClientId, Reply},
};

use super::{ResponseContext, network::server::NetworkOutgoingHandle, storage::StorageHandle};

pub struct ExecuteChannels {
    tx_blocks: UnboundedSender<(Vec<Block>, ResponseContext<()>)>,
    rx_blocks: UnboundedReceiver<(Vec<Block>, ResponseContext<()>)>,

    tx_fetch_response: Sender<(FetchId, Vec<Option<Vec<u8>>>)>,
    rx_fetch_response: Receiver<(FetchId, Vec<Option<Vec<u8>>>)>,
}

#[derive(Clone)]
pub struct ExecuteHandle {
    pub tx_blocks: UnboundedSender<(Vec<Block>, ResponseContext<()>)>,
    tx_fetch_response: Sender<(FetchId, Vec<Option<Vec<u8>>>)>,
}

impl Default for ExecuteChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecuteChannels {
    pub fn new() -> Self {
        let (tx_blocks, rx_blocks) = unbounded_channel();
        let (tx_fetch_response, rx_fetch_response) = channel(100);
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
            tx_fetch_response: self.tx_fetch_response.clone(),
        }
    }
}

impl ExecuteHandle {
    pub async fn fetch_response(
        &self,
        fetch_id: FetchId,
        values: Vec<Option<Vec<u8>>>,
    ) -> anyhow::Result<()> {
        self.tx_fetch_response.send((fetch_id, values)).await?;
        anyhow::Ok(())
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
        let context = ExecuteTaskContext::new(channels.handle(), storage, network_outgoing);
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
                    self.state.on_block(block, tx_response);
                }
            }
        }
    }
}

struct ExecuteTaskContext {
    execute: ExecuteHandle,
    storage: StorageHandle,
    network_outgoing: NetworkOutgoingHandle,
    fetch_id: FetchId,
}

impl ExecuteTaskContext {
    fn new(
        execute: ExecuteHandle,
        storage: StorageHandle,
        network_outgoing: NetworkOutgoingHandle,
    ) -> Self {
        Self {
            execute,
            storage,
            network_outgoing,
            fetch_id: 0,
        }
    }
}

impl ExecuteContext for ExecuteTaskContext {
    fn send(&mut self, id: ClientId, reply: Reply) {
        let _ = self.network_outgoing.send_message(id, reply);
    }

    fn fetch(&mut self, keys: Vec<Vec<u8>>) -> FetchId {
        self.fetch_id += 1;
        let fetch_id = self.fetch_id;
        let execute = self.execute.clone();
        let storage = self.storage.clone();
        tokio::spawn(async move {
            let response = storage.fetch(keys).await?;
            execute.fetch_response(fetch_id, response).await?;
            anyhow::Ok(())
        });
        fetch_id
    }

    fn post(&mut self, updates: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        let _ = self.storage.post(updates);
    }
}
