use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio_util::sync::CancellationToken;

use crate::{
    schema,
    storage::{BackendFetchId, BigStorage, BigStorageContext, StorageOp},
};

use super::{
    consensus::{ConsensusChannels, ConsensusTask},
    execute::{ExecuteChannels, ExecuteTask},
    network::{
        interconnect::{NetworkInterconnectHandle, NetworkInterconnectTask, ReceiveHandle},
        server::{NetworkAcceptTask, NetworkOutgoingChannels, NetworkOutgoingTask},
    },
    storage::{PlainStorageChannels, PlainStorageTask, StorageContext, StorageHandle},
};

struct BigStorageChannels {
    tx_storage_op: UnboundedSender<StorageOp>,
    rx_storage_op: UnboundedReceiver<StorageOp>,

    tx_fetch_response: UnboundedSender<(BackendFetchId, Vec<Option<Vec<u8>>>)>,
    rx_fetch_response: UnboundedReceiver<(BackendFetchId, Vec<Option<Vec<u8>>>)>,

    tx_incoming_message: Sender<crate::storage::Message>,
    rx_incoming_message: Receiver<crate::storage::Message>,
}

#[derive(Clone)]
struct BigStorageHandle {
    storage: StorageHandle,
    receive: ReceiveHandle<crate::storage::Message>,
}

impl BigStorageChannels {
    fn new() -> Self {
        let (tx_storage_op, rx_storage_op) = tokio::sync::mpsc::unbounded_channel();
        let (tx_fetch_response, rx_fetch_response) = tokio::sync::mpsc::unbounded_channel();
        let (tx_incoming_message, rx_incoming_message) = tokio::sync::mpsc::channel(100);
        Self {
            tx_storage_op,
            rx_storage_op,
            tx_fetch_response,
            rx_fetch_response,
            tx_incoming_message,
            rx_incoming_message,
        }
    }

    fn handle(&self) -> BigStorageHandle {
        BigStorageHandle {
            storage: StorageHandle::new(self.tx_storage_op.clone()),
            receive: ReceiveHandle::new(self.tx_incoming_message.clone()),
        }
    }

    fn backend_storage_context(&self, storage: StorageHandle) -> StorageContext {
        StorageContext::new(storage, self.tx_fetch_response.clone())
    }
}

struct BigStorageTask {
    channels: BigStorageChannels,
    state: BigStorage<BigStorageTaskContext>,
}

impl BigStorageTask {
    fn new(channels: BigStorageChannels, state: BigStorage<BigStorageTaskContext>) -> Self {
        Self { channels, state }
    }

    async fn load(
        channels: BigStorageChannels,
        network_interconnect: NetworkInterconnectHandle,
        storage: StorageHandle,
        schema: &schema::ReplicaTask,
    ) -> anyhow::Result<Self> {
        let context = BigStorageTaskContext::new(
            network_interconnect,
            channels.backend_storage_context(storage),
        );
        let state = BigStorage::new(context, (&schema.config).into(), schema.node_index);
        Ok(Self::new(channels, state))
    }

    async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await }).await?;
        Ok(())
    }

    async fn run_inner(mut self) {
        loop {
            tokio::select! {
                Some(op) = self.channels.rx_storage_op.recv() => {
                    self.state.invoke(op)
                }
                Some((fetch_id, values)) = self.channels.rx_fetch_response.recv() => {
                    self.state.on_fetch_response(fetch_id, values);
                }
                Some(message) = self.channels.rx_incoming_message.recv() => {
                    self.state.on_message(message);
                }
            }
        }
    }
}

struct BigStorageTaskContext {
    network_interconnect: NetworkInterconnectHandle,
    storage: StorageContext,
}

impl BigStorageTaskContext {
    fn new(network_interconnect: NetworkInterconnectHandle, storage: StorageContext) -> Self {
        Self {
            network_interconnect,
            storage,
        }
    }
}

impl BigStorageContext for BigStorageTaskContext {
    fn backend_fetch(&mut self, keys: Vec<Vec<u8>>) -> BackendFetchId {
        self.storage.fetch(keys)
    }

    fn backend_post(&mut self, updates: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        self.storage.post(updates);
    }

    fn send_to_all(&mut self, message: crate::storage::Message) {
        self.network_interconnect.send_to_all(message);
    }
}

pub struct BigReplicaNodeTask {
    network_accept: NetworkAcceptTask,
    network_outgoing: NetworkOutgoingTask,
    network_connect_consensus: NetworkInterconnectTask,
    consensus: ConsensusTask,
    execute: ExecuteTask,
    plain_storage: PlainStorageTask,
    network_connect_big: NetworkInterconnectTask,
    big_storage: BigStorageTask,
}

impl BigReplicaNodeTask {
    pub async fn load(schema: schema::ReplicaTask) -> anyhow::Result<Self> {
        let network_outgoing_channels = NetworkOutgoingChannels::new();
        let consensus_channels = ConsensusChannels::new();
        let execute_channels = ExecuteChannels::new();
        let plain_storage_channels = PlainStorageChannels::new();
        let big_storage_channels = BigStorageChannels::new();

        let network_accept = NetworkAcceptTask::load(
            consensus_channels.handle().submit,
            network_outgoing_channels.handle(),
        )
        .await?;
        let network_outgoing = NetworkOutgoingTask::load(network_outgoing_channels).await?;
        let network_connect =
            NetworkInterconnectTask::load(consensus_channels.handle().receive, &schema, 5001)
                .await?;
        let consensus = ConsensusTask::load(
            consensus_channels,
            execute_channels.handle(),
            network_connect.handle(),
            &schema,
        )
        .await?;
        let execute = ExecuteTask::load(
            execute_channels,
            big_storage_channels.handle().storage,
            network_outgoing.channels.handle(),
            &schema,
        )
        .await?;
        let plain_storage = PlainStorageTask::load(plain_storage_channels).await?;
        let network_connect_big =
            NetworkInterconnectTask::load(big_storage_channels.handle().receive, &schema, 5002)
                .await?;
        let big_storage = BigStorageTask::load(
            big_storage_channels,
            network_connect_big.handle(),
            plain_storage.channels.handle(),
            &schema,
        )
        .await?;
        Ok(Self {
            network_outgoing,
            network_accept,
            network_connect_consensus: network_connect,
            execute,
            plain_storage,
            consensus,
            network_connect_big,
            big_storage,
        })
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::try_join!(
            self.network_outgoing.run(stop.clone()),
            self.network_accept.run(stop.clone()),
            self.network_connect_consensus.run(stop.clone()),
            self.execute.run(stop.clone()),
            self.consensus.run(stop.clone()),
            self.plain_storage.run(stop.clone()),
            self.network_connect_big.run(stop.clone()),
            self.big_storage.run(stop.clone()),
        )?;
        Ok(())
    }
}
