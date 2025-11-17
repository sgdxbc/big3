use tokio::{spawn, sync::mpsc::UnboundedReceiver};
use tokio_util::sync::CancellationToken;

use crate::{
    common::{NodeIndex, Reply},
    consensus::{Block, ConsensusChannels, ConsensusTask},
    execute::{
        AbstractExecute, ExecuteSchedTask, ExecuteSourceHandle, ExecuteSourceTask,
        GeneralExecuteSchedTask, GeneralExecuteSourceTask, ycsb::YcsbRes,
    },
    network::{
        interconnect::NetworkInterconnectTask,
        server::{
            NetworkAcceptTask, NetworkOutgoingChannels, NetworkOutgoingHandle, NetworkOutgoingTask,
        },
    },
    schema,
};

struct ExecuteTask {
    rx_blocks: UnboundedReceiver<Vec<Block>>,
    network_outgoing: NetworkOutgoingHandle,
    node_index: NodeIndex,
}

impl ExecuteTask {
    async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        spawn(async move { stop.run_until_cancelled(self.run_inner()).await }).await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        while let Some(blocks) = self.rx_blocks.recv().await {
            for block in blocks {
                for request in block.txns {
                    let res = YcsbRes::Get(vec![0; 100 - 16]);
                    let reply = Reply {
                        client_seq: request.client_seq,
                        res: bincode::encode_to_vec(res, bincode::config::standard()).unwrap(),
                        node_index: self.node_index,
                    };
                    let _ = self.network_outgoing.send_message(request.client_id, reply);
                }
            }
        }
    }
}

pub struct BigReplicaNodeTask {
    network_accept: NetworkAcceptTask<true>,
    network_outgoing: NetworkOutgoingTask,
    network_interconnect_consensus: NetworkInterconnectTask,
    consensus: ConsensusTask,
    execute: ExecuteTask,
}

impl BigReplicaNodeTask {
    #[allow(unused)]
    async fn load_inner<E: AbstractExecute>(
        schema: schema::ReplicaTask,
        execute: E,
        into_execute_source: impl FnOnce(ExecuteSourceTask<E::Op>) -> GeneralExecuteSourceTask,
        into_execute_sched: impl FnOnce(ExecuteSchedTask<E>) -> GeneralExecuteSchedTask,
    ) -> anyhow::Result<Self> {
        let network_outgoing_channels = NetworkOutgoingChannels::new();
        let consensus_channels = ConsensusChannels::new();
        let (tx_blocks, rx_blocks) = tokio::sync::mpsc::unbounded_channel();

        let execute = ExecuteTask {
            rx_blocks,
            network_outgoing: network_outgoing_channels.handle(),
            node_index: schema.node_index,
        };
        let execute_handle = ExecuteSourceHandle {
            tx_blocks,
            tx_post_done: tokio::sync::mpsc::unbounded_channel().0,
        };

        let network_accept = NetworkAcceptTask::load(
            consensus_channels.handle().submit,
            network_outgoing_channels.handle(),
        )
        .await?;
        let network_outgoing = NetworkOutgoingTask::load(network_outgoing_channels).await?;
        let network_interconnect_consensus =
            NetworkInterconnectTask::load(consensus_channels.handle().receive, &schema, 5001)
                .await?;

        let consensus = ConsensusTask::load(
            consensus_channels,
            execute_handle,
            network_interconnect_consensus.handle(),
            &schema,
        )
        .await?;
        Ok(Self {
            network_accept,
            network_outgoing,
            network_interconnect_consensus,
            consensus,
            execute,
        })
    }

    pub async fn load(schema: schema::ReplicaTask) -> anyhow::Result<Self> {
        assert_eq!(schema.num_shards, 1);
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
            schema::App::Utxo => {
                Self::load_inner(
                    schema,
                    crate::execute::utxo::UtxoExecute,
                    GeneralExecuteSourceTask::Utxo,
                    GeneralExecuteSchedTask::Utxo,
                )
                .await
            }
        }
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::try_join!(
            self.network_outgoing.run(stop.clone()),
            self.network_accept.run(stop.clone()),
            self.network_interconnect_consensus.run(stop.clone()),
            self.consensus.run(stop.clone()),
            self.execute.run(stop.clone()),
        )?;
        Ok(())
    }
}
