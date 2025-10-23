use std::time::Duration;

use big_control::{Cluster, Instance, configs::NUM_KEYS, load_all, run_endpoints, stop_all};
use big_schema::{PrefillTask, Task};
use reqwest::Client;
use tokio::{time::sleep, try_join};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    let endpoints = run_endpoints(cluster.servers.clone());
    let workload = run_workload(cluster.servers);
    let workload = async {
        let result = workload.await;
        sleep(Duration::from_millis(3000)).await;
        result
    };
    try_join!(endpoints, workload)?;
    Ok(())
}

async fn run_workload(server_instances: Vec<Instance>) -> anyhow::Result<()> {
    let control_client = Client::new();
    sleep(Duration::from_millis(2000)).await;
    println!("load servers");
    let task = PrefillTask { num_keys: NUM_KEYS };
    load_all(
        server_instances
            .iter()
            .map(|instance| (instance, Task::Prefill(task.clone()))),
        control_client.clone(),
    )
    .await?;
    println!("stop servers");
    stop_all(&server_instances, control_client.clone()).await?;
    Ok(())
}
