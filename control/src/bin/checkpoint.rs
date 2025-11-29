use std::time::Duration;

use big_control::{
    Cluster, Instance,
    configs::{APP, CACHE_SIZE, NETWORK, SHARDING, STORAGE, STRIPE_INTERVAL},
    load_all, run_endpoints, start_all, wait_all,
};
use big_schema::Task;
use reqwest::Client;
use tokio::{time::sleep, try_join};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    assert!(matches!(STORAGE, big_schema::Storage::Big));
    assert!(SHARDING.num_shards == 1);

    let cluster = Cluster::from_terraform().await?;

    let num_running_nodes = SHARDING.num_running_nodes();
    let servers = &cluster.servers[..num_running_nodes as usize];
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

    let ips = server_instances
        .iter()
        .map(|instance| instance.private_ip)
        .collect::<Vec<_>>();

    println!("load servers");
    let replica_items = server_instances.iter().enumerate().map(|(i, instance)| {
        let schema = big_schema::ReplicaTask {
            num_shards: 1,
            shard_index: 0,
            shard_node_index: i as _,
            num_shard_faulty_nodes: SHARDING.num_shard_faulty_nodes,
            ips: ips.clone(),
            latencies: NETWORK.to_latencies(),
            config: big_schema::ReplicaConfig {
                node_index: i as _,
                num_nodes: SHARDING.num_nodes(),
                num_faulty_nodes: SHARDING.num_faulty_nodes(),
            },
            cache_size: CACHE_SIZE,
            max_concurrent_executing: NETWORK.max_concurrent_executing(),
            storage: STORAGE,
            app: APP.to_schema_app(),
            stripe_interval: STRIPE_INTERVAL,
            checkpoint: true,
        };
        (instance, Task::Replica(schema))
    });
    load_all(replica_items, control_client.clone()).await?;

    println!("start servers");
    start_all(&server_instances, control_client.clone()).await?;

    println!("wait servers");
    wait_all(&server_instances, control_client.clone()).await?;
    Ok(())
}
