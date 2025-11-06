use tokio_util::sync::CancellationToken;

use crate::{
    consensus::{ConsensusChannels, ConsensusTask},
    execute::{
        ExecuteSchedChannels, ExecuteSchedTask, ExecuteSourceChannels, ExecuteSourceTask,
        GeneralExecuteSchedTask, GeneralExecuteSourceTask,
    },
    network::{
        interconnect::NetworkInterconnectTask,
        server::{NetworkAcceptTask, NetworkOutgoingChannels, NetworkOutgoingTask},
    },
    schema,
    storage::{BigStorageWorkerChannels, BigStorageWorkersTask, StorageWorkersChannels},
};

pub struct BigReplicaNodeTask {
    network_accept: NetworkAcceptTask,
    network_outgoing: NetworkOutgoingTask,
    network_interconnect_consensus: NetworkInterconnectTask,
    network_interconnect_big: NetworkInterconnectTask,
    consensus: ConsensusTask,
    execute_source: GeneralExecuteSourceTask,
    execute_sched: GeneralExecuteSchedTask,
    storage: BigStorageWorkersTask,
}

impl BigReplicaNodeTask {
    pub async fn load(schema: schema::ReplicaTask) -> anyhow::Result<Self> {
        let network_outgoing_channels = NetworkOutgoingChannels::new();
        let consensus_channels = ConsensusChannels::new();
        let execute_source_channels = ExecuteSourceChannels::new();
        let execute_sched_channels = ExecuteSchedChannels::new();
        let storage_channels = StorageWorkersChannels::new();
        let big_storage_channels = BigStorageWorkerChannels::new();

        let network_accept = NetworkAcceptTask::load(
            consensus_channels.handle().submit,
            network_outgoing_channels.handle(),
        )
        .await?;
        let network_outgoing = NetworkOutgoingTask::load(network_outgoing_channels).await?;
        let network_interconnect_consensus =
            NetworkInterconnectTask::load(consensus_channels.handle().receive, &schema, 5001)
                .await?;
        let network_interconnect_big =
            NetworkInterconnectTask::load(big_storage_channels.receive_handle(), &schema, 5002)
                .await?;
        let consensus = ConsensusTask::load(
            consensus_channels,
            execute_source_channels.handle(),
            network_interconnect_consensus.handle(),
            &schema,
        )
        .await?;
        let execute_source = ExecuteSourceTask::new(
            execute_source_channels,
            storage_channels.handle().tx_fetch,
            execute_sched_channels.handle(),
            (&schema).into(),
        );
        let execute_sched = ExecuteSchedTask::new(
            execute_sched_channels,
            storage_channels.handle().tx_post,
            network_outgoing.channels.handle(),
            (&schema).into(),
            crate::execute::ycsb::YcsbExecute,
        );
        let storage = BigStorageWorkersTask::load(
            storage_channels,
            big_storage_channels,
            execute_source.channels.handle().tx_post_done,
            network_interconnect_big.handle(),
            (&schema.config).into(),
            schema.node_index,
        )
        .await?;
        Ok(Self {
            network_accept,
            network_outgoing,
            network_interconnect_consensus,
            network_interconnect_big,
            consensus,
            execute_source: GeneralExecuteSourceTask::Ycsb(execute_source),
            execute_sched: GeneralExecuteSchedTask::Ycsb(execute_sched),
            storage,
        })
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::try_join!(
            self.network_outgoing.run(stop.clone()),
            self.network_accept.run(stop.clone()),
            self.network_interconnect_consensus.run(stop.clone()),
            self.network_interconnect_big.run(stop.clone()),
            self.consensus.run(stop.clone()),
            self.execute_source.run(stop.clone()),
            self.execute_sched.run(stop.clone()),
            self.storage.run(stop.clone()),
        )?;
        Ok(())
    }
}
