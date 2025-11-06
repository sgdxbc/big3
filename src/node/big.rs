use tokio_util::sync::CancellationToken;

use crate::{
    consensus::{ConsensusChannels, ConsensusTask},
    network::{
        interconnect::NetworkInterconnectTask,
        server::{NetworkAcceptTask, NetworkOutgoingChannels, NetworkOutgoingTask},
    },
    schema,
};

pub struct BigReplicaNodeTask {
    network_accept: NetworkAcceptTask,
    network_outgoing: NetworkOutgoingTask,
    network_connect_consensus: NetworkInterconnectTask,
    consensus: ConsensusTask,
    network_connect_big: NetworkInterconnectTask,
}

impl BigReplicaNodeTask {
    pub async fn load(schema: schema::ReplicaTask) -> anyhow::Result<Self> {
        let network_outgoing_channels = NetworkOutgoingChannels::new();
        let consensus_channels = ConsensusChannels::new();

        let network_accept = NetworkAcceptTask::load(
            consensus_channels.handle().submit,
            network_outgoing_channels.handle(),
        )
        .await?;
        let network_outgoing = NetworkOutgoingTask::load(network_outgoing_channels).await?;
        let network_connect =
            NetworkInterconnectTask::load(consensus_channels.handle().receive, &schema, 5001)
                .await?;
        //
        todo!()
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::try_join!(
            self.network_outgoing.run(stop.clone()),
            self.network_accept.run(stop.clone()),
            self.network_connect_consensus.run(stop.clone()),
            self.consensus.run(stop.clone()),
            self.network_connect_big.run(stop.clone()),
        )?;
        Ok(())
    }
}
