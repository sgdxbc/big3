use std::{env::args, time::Duration};

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
    // let servers = &cluster.servers[..];
    let servers = &cluster.servers[..SHARDING.num_running_nodes() as usize];
    let endpoints = run_endpoints(servers.to_vec());
    let workload = run_workload(servers.to_vec());
    let workload = async {
        let result = workload.await;
        if result.is_err() {
            sleep(Duration::from_millis(3000)).await;
        }
        result
    };
    try_join!(endpoints, workload)?;
    Ok(())
}

async fn run_workload(server_instances: Vec<Instance>) -> anyhow::Result<()> {
    let full = args().nth(1) == Some("full".to_string());
    if full {
        println!("Prefilling FULL storage");
    } else {
        println!("Prefilling LIGHT storage");
    }

    let control_client = Client::new();
    sleep(Duration::from_millis(2000)).await;

    let num_shard_running_nodes = 2 * SHARDING.num_shard_faulty_nodes + 1;
    let num_shards = SHARDING.num_shards;

    println!("load servers");
    let replica_items = server_instances.iter().enumerate().map(|(i, instance)| {
        let shard_index = (i / num_shard_running_nodes as usize) as _;
        let schema = PrefillTask {
            config: ReplicaConfig {
                node_index: i as _,
                num_nodes: SHARDING.num_nodes(),
                num_faulty_nodes: SHARDING.num_faulty_nodes(),
            },
            full,
            storage: STORAGE,
            app: APP.to_schema_app(),
            num_shards,
            shard_index,
        };
        (instance, Task::Prefill(schema))
    });

    load_all(replica_items, control_client.clone()).await?;
    println!("stop servers");
    stop_all(&server_instances, control_client.clone()).await?;
    Ok(())
}
