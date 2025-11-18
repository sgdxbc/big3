use tokio_util::sync::CancellationToken;

use crate::{
    consensus::{ConsensusChannels, ConsensusTask},
    execute::{
        AbstractExecute, ExecuteSchedChannels, ExecuteSchedTask, ExecuteSourceChannels,
        ExecuteSourceTask, GeneralExecuteSchedTask, GeneralExecuteSourceTask,
    },
    network::{
        interconnect::NetworkInterconnectTask,
        server::{NetworkAcceptTask, NetworkOutgoingChannels, NetworkOutgoingTask},
    },
    plain_storage::StorageWorkersTask,
    schema,
    storage::StorageWorkersChannels,
};

pub struct FullReplicaNodeTask {
    network_accept: NetworkAcceptTask<true>,
    network_outgoing: NetworkOutgoingTask,
    network_connect: NetworkInterconnectTask,
    consensus: ConsensusTask,
    execute_source: GeneralExecuteSourceTask,
    execute_sched: GeneralExecuteSchedTask,
    storage: StorageWorkersTask,
}

impl FullReplicaNodeTask {
    async fn load_inner<E: AbstractExecute>(
        schema: schema::ReplicaTask,
        execute: E,
        into_execute_source: impl FnOnce(ExecuteSourceTask<E::Op>) -> GeneralExecuteSourceTask,
        into_execute_sched: impl FnOnce(ExecuteSchedTask<E>) -> GeneralExecuteSchedTask,
    ) -> anyhow::Result<Self> {
        let network_outgoing_channels = NetworkOutgoingChannels::new();
        let consensus_channels = ConsensusChannels::new();
        let execute_source_channels = ExecuteSourceChannels::new();
        let execute_sched_channels = ExecuteSchedChannels::new();
        let storage_channels = StorageWorkersChannels::new();

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
            execute_source_channels.deliver_handle(),
            network_connect.handle(),
            &schema,
        )
        .await?;
        let storage = StorageWorkersTask::load(
            storage_channels,
            execute_source_channels.handle().tx_post_done,
        )
        .await?;

        let execute_source = ExecuteSourceTask::new(
            execute_source_channels,
            storage.channels.handle().tx_fetch,
            execute_sched_channels.handle(),
            (&schema).into(),
        );
        let execute_sched = ExecuteSchedTask::new(
            execute_sched_channels,
            storage.channels.handle().tx_post,
            network_outgoing.channels.handle(),
            (&schema).into(),
            execute,
        );

        Ok(Self {
            network_outgoing,
            network_accept,
            network_connect,
            execute_source: into_execute_source(execute_source),
            execute_sched: into_execute_sched(execute_sched),
            storage,
            consensus,
        })
    }

    pub async fn load(schema: schema::ReplicaTask) -> anyhow::Result<Self> {
        assert!(schema.num_shards >= 1);
        match &schema.app {
            schema::App::Ycsb => {
                Self::load_inner(
                    schema,
                    crate::execute::ycsb::YcsbExecute,
                    GeneralExecuteSourceTask::Ycsb,
                    GeneralExecuteSchedTask::Ycsb,
                )
                .await
            }
            schema::App::Utxo if schema.num_shards == 1 => {
                Self::load_inner(
                    schema,
                    crate::execute::utxo::UtxoExecute,
                    GeneralExecuteSourceTask::Utxo,
                    GeneralExecuteSchedTask::Utxo,
                )
                .await
            }
            schema::App::Utxo => {
                let execute = crate::execute::sharded_utxo::ShardedUtxoExecute::new(
                    schema.num_shards,
                    schema.shard_index,
                );
                Self::load_inner(
                    schema,
                    execute,
                    GeneralExecuteSourceTask::ShardedUtxo,
                    GeneralExecuteSchedTask::ShardedUtxo,
                )
                .await
            }
        }
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::try_join!(
            self.network_outgoing.run(stop.clone()),
            self.network_accept.run(stop.clone()),
            self.network_connect.run(stop.clone()),
            self.execute_source.run(stop.clone()),
            self.execute_sched.run(stop.clone()),
            self.consensus.run(stop.clone()),
            self.storage.run(),
        )?;
        Ok(())
    }
}
