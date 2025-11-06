use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use rand::{Rng, rng};
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    time::{interval, sleep},
};
use tokio_util::sync::CancellationToken;

use crate::{
    client::ClientHandle,
    common::{RequestContext, RequestId},
    schema,
};

use self::state::{InvokeId, ShardIndex, Workload, WorkloadContext};

mod state;
mod zipfian;

pub use state::ClientScrapeState;

pub struct ClientWorkerChannels {
    tx_invoke_response: UnboundedSender<(RequestId, Vec<u8>)>,
    rx_invoke_response: UnboundedReceiver<(RequestId, Vec<u8>)>,
}

impl Default for ClientWorkerChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientWorkerChannels {
    pub fn new() -> Self {
        let (tx_invoke_response, rx_invoke_response) = unbounded_channel();
        Self {
            tx_invoke_response,
            rx_invoke_response,
        }
    }

    fn invoke_contexts(&self, clients: Vec<ClientHandle>) -> Vec<RequestContext<Vec<u8>, Vec<u8>>> {
        clients
            .into_iter()
            .map(|client_handle| {
                RequestContext::new(client_handle.tx_invoke, self.tx_invoke_response.clone())
            })
            .collect()
    }
}

pub struct WorkloadTask {
    channels: ClientWorkerChannels,
    state: Workload<ClientWorkerTaskContext>,
}

impl WorkloadTask {
    fn new(channels: ClientWorkerChannels, state: Workload<ClientWorkerTaskContext>) -> Self {
        Self { channels, state }
    }

    pub fn load(
        client_worker_channels: ClientWorkerChannels,
        clients: Vec<ClientHandle>,
        scrape_state: Arc<Mutex<ClientScrapeState>>,
        schema: &schema::WorkloadConfig,
        num_concurrent: u32,
        workload_index: u32,
    ) -> anyhow::Result<Self> {
        let context = ClientWorkerTaskContext::new(client_worker_channels.invoke_contexts(clients));
        let state = Workload::new(
            context,
            scrape_state,
            schema,
            num_concurrent,
            workload_index,
        );
        Ok(Self::new(client_worker_channels, state))
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move {
            stop.run_until_cancelled(self.run_inner()).await;
            self.state.log_metrics();
        })
        .await?;
        Ok(())
    }

    const TICK_INTERVAL: Duration = Duration::from_millis(10);

    async fn run_inner(&mut self) {
        let duration = rng().random_range(Duration::ZERO..Self::TICK_INTERVAL);
        sleep(duration).await;
        self.state.start();
        let mut ticker = interval(Self::TICK_INTERVAL);
        loop {
            select! {
                _ = ticker.tick() => {
                    // self.state.on_tick();
                }
                Some((seq, res)) = self.channels.rx_invoke_response.recv() => {
                    self.state.on_invoke_response(seq, res);
                }
            }
        }
    }
}

struct ClientWorkerTaskContext {
    invoke_id: InvokeId,
    invokes: Vec<RequestContext<Vec<u8>, Vec<u8>>>,
}

impl ClientWorkerTaskContext {
    fn new(invokes: Vec<RequestContext<Vec<u8>, Vec<u8>>>) -> Self {
        Self {
            invokes,
            invoke_id: 0,
        }
    }
}

impl WorkloadContext for ClientWorkerTaskContext {
    fn invoke(&mut self, shard: ShardIndex, command: Vec<u8>) -> InvokeId {
        self.invoke_id += 1;
        self.invokes[shard as usize].request_with_id(self.invoke_id, command)
    }
}
