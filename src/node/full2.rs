use tokio_util::sync::CancellationToken;

use crate::{
    consensus::{ConsensusChannels, ConsensusTask},
    execute::AbstractExecute,
    execute2::{ExecuteChannels, ExecuteTask, GeneralExecuteTask},
    network::{
        interconnect::NetworkInterconnectTask,
        server::{NetworkAcceptTask, NetworkOutgoingChannels, NetworkOutgoingTask},
    },
    plain_storage2::PlainStorageTask,
    schema,
    storage2::StorageChannels,
};

pub struct FullReplicaNodeTask {
    network_accept: NetworkAcceptTask<true>,
    network_outgoing: NetworkOutgoingTask,
    network_connect: NetworkInterconnectTask,
    consensus: ConsensusTask,
    execute: GeneralExecuteTask,
    storage: PlainStorageTask,
}

impl FullReplicaNodeTask {
    async fn load_inner<E: AbstractExecute>(
        schema: schema::ReplicaTask,
        execute: E,
        into_execute: impl FnOnce(ExecuteTask<E>) -> GeneralExecuteTask,
    ) -> anyhow::Result<Self> {
        let network_outgoing_channels = NetworkOutgoingChannels::new();
        let consensus_channels = ConsensusChannels::new();
        let execute_channels = ExecuteChannels::new();
        let storage_channels = StorageChannels::new();

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
            execute_channels.deliver_handle(),
            network_connect.handle(),
            &schema,
        )
        .await?;
        let storage = PlainStorageTask::load(
            storage_channels,
            execute_channels.fetched_handle(),
            execute_channels.post_done_handle(),
            schema.cache_size,
            &schema.app,
        )
        .await?;

        let execute = ExecuteTask::new(
            execute_channels,
            storage.channels.fetch_handle(),
            storage.channels.post_handle(),
            network_outgoing.channels.handle(),
            schema.shard_index,
            schema.shard_node_index,
            schema.num_shard_faulty_nodes,
            execute,
        );

        Ok(Self {
            network_outgoing,
            network_accept,
            network_connect,
            execute: into_execute(execute),
            storage,
            consensus,
        })
    }

    pub async fn load(schema: schema::ReplicaTask) -> anyhow::Result<Self> {
        assert!(schema.num_shards >= 1);
        match &schema.app {
            schema::App::Ycsb(num_keys) => {
                let execute = crate::execute::ycsb::YcsbExecute::new(
                    schema.num_shards,
                    schema.shard_index,
                    *num_keys,
                );
                Self::load_inner(schema, execute, GeneralExecuteTask::Ycsb).await
            }
            schema::App::Utxo(_) if schema.num_shards == 1 => {
                Self::load_inner(
                    schema,
                    crate::execute::utxo::UtxoExecute,
                    GeneralExecuteTask::Utxo,
                )
                .await
            }
            schema::App::Utxo(_) => {
                let execute = crate::execute::sharded_utxo::ShardedUtxoExecute::new(
                    schema.num_shards,
                    schema.shard_index,
                );
                Self::load_inner(schema, execute, GeneralExecuteTask::ShardedUtxo).await
            }
        }
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::try_join!(
            self.network_outgoing.run(stop.clone()),
            self.network_accept.run(stop.clone()),
            self.network_connect.run(stop.clone()),
            self.consensus.run(stop.clone()),
            self.storage.run(),
            self.execute.run(stop.clone()),
        )?;
        Ok(())
    }
}
