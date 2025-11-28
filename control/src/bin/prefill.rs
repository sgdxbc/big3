use std::time::Duration;

use big_control::{
    Cluster, Instance,
    configs::{APP, SHARDING, STORAGE},
    load_all, run_endpoints, stop_all,
};
use big_schema::{PrefillTask, ReplicaConfig, Task};
use reqwest::Client;
use tokio::{time::sleep, try_join};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    let servers = &cluster.servers[..];
    let endpoints = run_endpoints(servers.to_vec());
    let workload = run_workload(servers.to_vec());
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
    let replica_items = server_instances.iter().enumerate().map(|(i, instance)| {
        let schema = PrefillTask {
            node_index: i as _,
            config: ReplicaConfig {
                num_nodes: SHARDING.num_nodes(),
                num_faulty_nodes: SHARDING.num_faulty_nodes(),
                cache_size: 0,               // unused
                max_concurrent_executing: 0, // unused
            },
            storage: STORAGE,
            app: APP.to_schema_app(),
        };
        (instance, Task::Prefill(schema))
    });

    load_all(replica_items, control_client.clone()).await?;
    println!("stop servers");
    stop_all(&server_instances, control_client.clone()).await?;
    Ok(())
}
