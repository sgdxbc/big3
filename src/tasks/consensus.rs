use std::time::Instant;

use tokio::{
    select,
    sync::mpsc::{Receiver, Sender, channel},
};
use tokio_util::sync::CancellationToken;

use crate::{
    consensus::{Block, Bullshark, BullsharkContext, OutputId},
    schema,
    types::{NodeIndex, Request},
};

use super::{
    execute::ExecuteHandle,
    network::{
        interconnect::{NetworkInterconnectHandle, ReceiveHandle},
        server::SubmitHandle,
    },
};

pub struct ConsensusChannels {
    tx_request: Sender<(Instant, Request)>,
    rx_request: Receiver<(Instant, Request)>,

    tx_incoming_message: Sender<crate::consensus::message::Message>,
    rx_incoming_message: Receiver<crate::consensus::message::Message>,

    tx_output_response: Sender<OutputId>,
    rx_output_response: Receiver<OutputId>,
}

#[derive(Clone)]
pub struct ConsensusHandle {
    pub receive: ReceiveHandle<crate::consensus::message::Message>,
    pub submit: SubmitHandle,
    tx_output_response: Sender<OutputId>,
}

impl Default for ConsensusChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsensusChannels {
    pub fn new() -> Self {
        let (tx_request, rx_request) = channel(100);
        let (tx_incoming_message, rx_incoming_message) = channel(100);
        let (tx_output_response, rx_output_response) = channel(100);
        Self {
            tx_request,
            rx_request,
            tx_incoming_message,
            rx_incoming_message,
            tx_output_response,
            rx_output_response,
        }
    }

    pub fn handle(&self) -> ConsensusHandle {
        ConsensusHandle {
            receive: ReceiveHandle::new(self.tx_incoming_message.clone()),
            submit: SubmitHandle::new(self.tx_request.clone()),
            tx_output_response: self.tx_output_response.clone(),
        }
    }
}

impl ConsensusHandle {
    pub async fn output_response(&self, output_id: OutputId) -> anyhow::Result<()> {
        self.tx_output_response.send(output_id).await?;
        anyhow::Ok(())
    }
}

pub struct ConsensusTask {
    channels: ConsensusChannels,
    state: Bullshark<ConsensusTaskContext>,
}

impl ConsensusTask {
    fn new(channels: ConsensusChannels, state: Bullshark<ConsensusTaskContext>) -> Self {
        Self { channels, state }
    }

    pub async fn load(
        channels: ConsensusChannels,
        execute: ExecuteHandle,
        network_connect: NetworkInterconnectHandle,
        schema: &schema::ReplicaTask,
    ) -> anyhow::Result<Self> {
        let context = ConsensusTaskContext::new(channels.handle(), execute, network_connect);
        let state = Bullshark::new(context, (&schema.config).into(), schema.node_index);
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
        self.state.start();
        loop {
            select! {
                Some(message) = self.channels.rx_incoming_message.recv() => {
                    self.state.on_message(message);
                }
                Some((at, request)) = self.channels.rx_request.recv() => {
                    self.state.on_request(at, request);
                }
                Some(output_id) = self.channels.rx_output_response.recv() => {
                    self.state.on_output_response(output_id);
                }
            }
        }
    }
}

struct ConsensusTaskContext {
    consensus: ConsensusHandle,
    execute: ExecuteHandle,
    network_connect: NetworkInterconnectHandle,
    output_id: OutputId,
}

impl ConsensusTaskContext {
    fn new(
        consensus: ConsensusHandle,
        execute: ExecuteHandle,
        network_connect: NetworkInterconnectHandle,
    ) -> Self {
        Self {
            consensus,
            execute,
            network_connect,
            output_id: 0,
        }
    }
}

impl BullsharkContext for ConsensusTaskContext {
    fn output(&mut self, blocks: Vec<Block>) -> OutputId {
        self.output_id += 1;
        let output_id = self.output_id;
        let execute = self.execute.clone();
        let consensus = self.consensus.clone();
        tokio::spawn(async move {
            execute.execute(blocks).await?;
            consensus.output_response(output_id).await?;
            anyhow::Ok(())
        });
        output_id
    }

    fn send(&mut self, node_index: NodeIndex, message: crate::consensus::message::Message) {
        self.network_connect.send(node_index, message);
    }

    fn send_to_all(&mut self, message: crate::consensus::message::Message) {
        self.network_connect.send_to_all(message);
    }
}
