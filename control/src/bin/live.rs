use std::time::Duration;

use big_control::{
    Cluster, Instance,
    configs::{
        APP, CACHE_SIZE, LIVE_DURATION, NETWORK, NUM_CONCURRENT, Network, SHARDING, STORAGE,
        STRIPE_INTERVAL,
    },
    load_all, run_endpoints, scrape_all, start_all, stop_all,
};
use big_schema::Task;
use reqwest::Client;
use tokio::{
    time::{Instant, sleep, sleep_until},
    try_join,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    run(&cluster).await
}

async fn run(cluster: &Cluster) -> anyhow::Result<()> {
    let num_running_nodes = SHARDING.num_running_nodes();

    let endpoints = run_endpoints(
        [
            &cluster.servers[..num_running_nodes as usize],
            &cluster.clients,
        ]
        .concat(),
    );
    let workload = run_workload(
        &cluster.servers[..num_running_nodes as usize],
        &cluster.clients,
    );
    let workload = async {
        let result = workload.await;
        if result.is_err() {
            sleep(Duration::from_millis(5000)).await;
        }
        result
    };
    try_join!(endpoints, workload)?;
    Ok(())
}

async fn run_workload(
    server_instances: &[Instance],
    client_instances: &[Instance],
) -> anyhow::Result<()> {
    let control_client = Client::new();
    println!("wait for servers to boot");
    sleep(Duration::from_millis(2000)).await;

    let ips = server_instances
        .iter()
        .map(|instance| instance.private_ip)
        .collect::<Vec<_>>();

    let num_shard_running_nodes = 2 * SHARDING.num_shard_faulty_nodes + 1;
    let num_shards = SHARDING.num_shards as _;

    println!("load servers");
    let replica_items = server_instances.iter().enumerate().map(|(i, instance)| {
        let shard_index = (i / num_shard_running_nodes as usize) as _;
        let schema = big_schema::ReplicaTask {
            node_index: i as _,
            num_shards,
            shard_index,
            shard_node_index: (i % num_shard_running_nodes as usize) as _,
            num_shard_faulty_nodes: SHARDING.num_shard_faulty_nodes,
            ips: ips.clone(),
            latencies: NETWORK.to_latencies(),
            config: big_schema::ReplicaConfig {
                num_nodes: SHARDING.num_nodes(),
                num_faulty_nodes: SHARDING.num_faulty_nodes(),
                cache_size: CACHE_SIZE,
                max_concurrent_executing: match NETWORK {
                    Network::Lan => 1,
                    Network::Wan => 1000,
                },
            },
            storage: STORAGE,
            app: APP.to_schema_app(),
            stripe_interval: STRIPE_INTERVAL,
        };
        (instance, Task::Replica(schema))
    });
    load_all(replica_items, control_client.clone()).await?;

    println!("start servers");
    start_all(server_instances, control_client.clone()).await?;

    println!("load clients");
    let client_items = client_instances.iter().enumerate().map(|(i, instance)| {
        let client_task = big_schema::ClientTask {
            ips: vec![ips.clone()],
            config: big_schema::ClientConfig {
                // num_nodes: num_nodes(),
                num_faulty_nodes: SHARDING.num_shard_faulty_nodes,
            },
            workload_config: big_schema::ClientWorkloadConfig {
                num_concurrent: NUM_CONCURRENT,
                num_shards: SHARDING.num_shards,
                app: APP.to_schema_workload(),
            },
            node_index: i as _,
        };
        (instance, Task::Client(client_task))
    });
    load_all(client_items, control_client.clone()).await?;
    println!("start clients");
    start_all(client_instances, control_client.clone()).await?;

    let mut next_scrape = Instant::now() + Duration::from_secs(1);
    let mut sec_tputs = Vec::new();
    for i in 0..LIVE_DURATION.as_secs() {
        sleep_until(next_scrape).await;
        println!("scrape clients round {}", i + 1);
        let metrics = scrape_all(client_instances, control_client.clone()).await?;
        sec_tputs.push(metrics.tput);
        println!(
            "last 5 sec avg tput: {}",
            sec_tputs.iter().rev().take(5).sum::<f64>() / sec_tputs.len().min(5) as f64
        );
        next_scrape += Duration::from_secs(1);
    }

    println!("stop clients");
    stop_all(client_instances, control_client.clone()).await?;
    println!("stop servers");
    stop_all(server_instances, control_client.clone()).await?;
    println!("done");
    Ok(())
}
