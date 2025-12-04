use tokio_util::sync::CancellationToken;

use crate::{
    archive::ArchiveChannels,
    consensus::{ConsensusChannels, ConsensusTask},
    execute::AbstractExecute,
    execute2::{ExecuteChannels, ExecuteTask, GeneralExecuteTask},
    network::{
        interconnect::NetworkInterconnectTask,
        server::{NetworkAcceptTask, NetworkOutgoingChannels, NetworkOutgoingTask},
    },
    schema,
    storage3::{StorageChannels, StorageTask},
};

pub struct BigReplicaNodeTask {
    network_accept: NetworkAcceptTask<true>,
    network_outgoing: NetworkOutgoingTask,
    network_interconnect_consensus: NetworkInterconnectTask,
    network_interconnect_big: NetworkInterconnectTask,
    network_interconnect_archive: NetworkInterconnectTask,
    consensus: ConsensusTask,
    execute: GeneralExecuteTask,
    storage: StorageTask,
}

impl BigReplicaNodeTask {
    async fn load_inner<E: AbstractExecute>(
        schema: schema::ReplicaTask,
        execute: E,
        into_execute: impl FnOnce(ExecuteTask<E>) -> GeneralExecuteTask,
    ) -> anyhow::Result<Self> {
        let network_outgoing_channels = NetworkOutgoingChannels::new();
        let consensus_channels = ConsensusChannels::new();
        let archive_channels = ArchiveChannels::new();
        let execute_channels = ExecuteChannels::new();
        let storage_channels = StorageChannels::new();

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
            NetworkInterconnectTask::load(storage_channels.receive_handle(), &schema, 5002).await?;
        let network_interconnect_archive =
            NetworkInterconnectTask::load(archive_channels.receive_handle(), &schema, 5003).await?;

        let consensus = ConsensusTask::load(
            consensus_channels,
            execute_channels.deliver_handle(),
            network_interconnect_consensus.handle(),
            &schema,
        )
        .await?;
        let execute = ExecuteTask::new(
            execute_channels,
            storage_channels.fetch_handle(),
            storage_channels.post_handle(),
            network_outgoing.channels.handle(),
            schema.shard_index,
            schema.config.node_index,
            schema.config.num_faulty_nodes,
            execute,
        );
        let storage = StorageTask::load(
            storage_channels,
            archive_channels,
            execute.channels.fetched_handle(),
            execute.channels.post_done_handle(),
            network_interconnect_big.handle(),
            network_interconnect_archive.handle(),
            &schema,
        )
        .await?;

        Ok(Self {
            network_accept,
            network_outgoing,
            network_interconnect_consensus,
            network_interconnect_big,
            network_interconnect_archive,
            consensus,
            execute: into_execute(execute),
            storage,
        })
    }

    pub async fn load(schema: schema::ReplicaTask) -> anyhow::Result<Self> {
        assert_eq!(schema.num_shards, 1);
        match &schema.app {
            schema::App::Ycsb(num_keys) => {
                let execute = crate::execute::ycsb::YcsbExecute::new(
                    schema.num_shards,
                    schema.shard_index,
                    *num_keys,
                );
                Self::load_inner(schema, execute, GeneralExecuteTask::Ycsb).await
            }
            schema::App::Utxo(_) => {
                Self::load_inner(
                    schema,
                    crate::execute::utxo::UtxoExecute,
                    GeneralExecuteTask::Utxo,
                )
                .await
            }
        }
    }

    pub async fn run(
        self,
        stop: CancellationToken,
        wait: CancellationToken,
    ) -> anyhow::Result<schema::StoppedReplicaBig> {
        let (
            (),
            reply_egress,
            consensus_egress,
            retrieve_egress,
            checkpoint_egress,
            (),
            (),
            archive_metrics,
        ) = tokio::try_join!(
            self.network_outgoing.run(stop.clone()),
            self.network_accept.run(stop.clone()),
            self.network_interconnect_consensus.run(stop.clone()),
            self.network_interconnect_big.run(stop.clone()),
            self.network_interconnect_archive.run(stop.clone()),
            self.consensus.run(stop.clone()),
            self.execute.run(stop.clone()),
            self.storage.run(stop.clone(), wait),
        )?;
        let stopped = schema::StoppedReplicaBig {
            checkpoint: archive_metrics.round.1.as_secs_f32(),
            checkpoint_scan: archive_metrics.scan.1.as_secs_f32(),
            checkpoint_network: archive_metrics.network.1.as_secs_f32(),
            checkpoint_verify: archive_metrics.verify.1.as_secs_f32(),
            checkpoint_update: archive_metrics.update.1.as_secs_f32(),

            replica_egress: reply_egress + consensus_egress,
            retrieve_egress,
            checkpoint_egress,
        };
        Ok(stopped)
    }
}
