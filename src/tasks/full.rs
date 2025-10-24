use tokio_util::sync::CancellationToken;

use crate::schema;

use super::{
    consensus::{ConsensusChannels, ConsensusTask},
    execute::{ExecuteChannels, ExecuteTask},
    network::{
        interconnect::NetworkInterconnectTask,
        server::{NetworkAcceptTask, NetworkOutgoingChannels, NetworkOutgoingTask},
    },
    storage::PlainStorageTask,
};

pub struct FullReplicaNodeTask {
    network_accept: NetworkAcceptTask,
    network_outgoing: NetworkOutgoingTask,
    network_connect: NetworkInterconnectTask,
    consensus: ConsensusTask,
    execute: ExecuteTask,
    storage: PlainStorageTask,
}

impl FullReplicaNodeTask {
    pub async fn load(schema: schema::ReplicaTask) -> anyhow::Result<Self> {
        let network_outgoing_channels = NetworkOutgoingChannels::new();
        let consensus_channels = ConsensusChannels::new();
        let execute_channels = ExecuteChannels::new();
        let storage_channels = super::storage::PlainStorageChannels::new();

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
            storage_channels.handle(),
            network_outgoing.channels.handle(),
            &schema,
        )
        .await?;
        let storage = PlainStorageTask::load(storage_channels).await?;
        Ok(Self {
            network_outgoing,
            network_accept,
            network_connect,
            execute,
            storage,
            consensus,
        })
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::try_join!(
            self.network_outgoing.run(stop.clone()),
            self.network_accept.run(stop.clone()),
            self.network_connect.run(stop.clone()),
            self.execute.run(stop.clone()),
            self.consensus.run(stop.clone()),
            self.storage.run(stop.clone()),
        )?;
        Ok(())
    }
}
