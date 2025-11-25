use std::time::Duration;

use big_control::{
    Cluster, Instance,
    configs::{APP, NUM_FAULTY_NODES, NUM_SHARDS, STORAGE, num_nodes},
    load_all, run_endpoints, stop_all,
};
use big_schema::{PrefillTask, ReplicaConfig, Task};
use reqwest::Client;
use tokio::{time::sleep, try_join};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    let servers = &cluster.servers[..((2 * NUM_FAULTY_NODES + 1) * NUM_SHARDS as u16) as usize];
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

    let shard_size = 2 * NUM_FAULTY_NODES + 1;
    assert!(server_instances.len().is_multiple_of(shard_size as usize));
    let num_shards = (server_instances.len() / shard_size as usize) as _;

    println!("load servers");
    let replica_items = server_instances.iter().enumerate().map(|(i, instance)| {
        let shard_index = (i / shard_size as usize) as _;
        let schema = PrefillTask {
            node_index: (i % shard_size as usize) as _,
            num_shards,
            shard_index,
            config: ReplicaConfig {
                num_nodes: num_nodes(),
                num_faulty_nodes: NUM_FAULTY_NODES,
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
