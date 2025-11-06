use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use log::debug;
use quinn::{Endpoint, TransportConfig};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{
    cert::client_config,
    client::{ClientChannels, ClientTask, NetworkConnectTask},
    schema,
    workload::{ClientScrapeState, ClientWorkerChannels, WorkloadTask},
};

pub struct ClientNodeTask {
    scrape_state: Arc<Mutex<ClientScrapeState>>,
    connected_clients: Vec<ConnectedClientTask>,
    workloads: Vec<WorkloadTask>,
}

struct ConnectedClientTask {
    network_connect: NetworkConnectTask<true>,
    client: ClientTask,
}

impl ClientNodeTask {
    pub async fn load(schema: schema::ClientTask) -> anyhow::Result<Self> {
        debug!("loading client node task");
        let scrape_state = Arc::new(Mutex::new(ClientScrapeState::now()));

        let mut transport_config = TransportConfig::default();
        transport_config.keep_alive_interval(Duration::from_secs(10).into());
        let mut config = client_config();
        config.transport_config(transport_config.into());

        let mut connected_clients = Vec::new();
        let mut workloads = Vec::new();
        // let num_group = 4;
        let num_group = 1;
        for group_index in 0..num_group {
            let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
            endpoint.set_default_client_config(config.clone());

            let mut client_handles = Vec::new();
            for shard in 0..schema.ips.len() {
                let client_channels = ClientChannels::new();

                let client_id = rand::random();

                let network_connect = NetworkConnectTask::load(
                    endpoint.clone(),
                    client_channels.handle(),
                    &schema,
                    shard as _,
                    client_id,
                )
                .await?;
                debug!("[{:08x}] network connect loaded", client_id);
                let client = ClientTask::load(
                    client_channels,
                    network_connect.handle(),
                    &schema,
                    client_id,
                )
                .await?;
                debug!("[{:08x}] client loaded", client_id);

                client_handles.push(client.channels.handle());
                connected_clients.push(ConnectedClientTask {
                    network_connect,
                    client,
                });
            }

            let num_concurrent = schema.workload_config.num_concurrent / num_group
                + (group_index < schema.workload_config.num_concurrent % num_group) as u32;
            let scrape_state = scrape_state.clone();

            let client_worker_channels = ClientWorkerChannels::new();
            let client_worker = WorkloadTask::load(
                client_worker_channels,
                client_handles.clone(),
                scrape_state.clone(),
                &schema.workload_config.app,
                num_concurrent,
                schema.node_index as u32 * num_group + group_index,
            )?;
            workloads.push(client_worker);
        }
        debug!("client node task loaded");

        Ok(Self {
            scrape_state,
            connected_clients,
            workloads,
        })
    }

    pub fn scrape_state(&self) -> Arc<Mutex<ClientScrapeState>> {
        self.scrape_state.clone()
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        let mut tasks = JoinSet::new();
        // TODO remove double layer spawn
        for ConnectedClientTask {
            network_connect,
            client,
        } in self.connected_clients
        {
            tasks.spawn(network_connect.run(stop.clone()));
            tasks.spawn(client.run(stop.clone()));
        }
        for workload in self.workloads {
            tasks.spawn(workload.run(stop.clone()));
        }
        while let Some(res) = tasks.join_next().await {
            res??;
        }
        Ok(())
    }
}
