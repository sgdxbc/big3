use std::time::Duration;

use big_control::{
    Cluster, Instance,
    configs::{
        APP, LIVE_DURATION, NETWORK, NUM_CONCURRENT, NUM_FAULTY_NODES, NUM_KEYS, NUM_SHARDS,
        READ_RATIO, STORAGE, num_nodes,
    },
    load_all, run_endpoints, scrape_all, stop_all,
};
use big_schema::Task;
use reqwest::Client;
use tokio::{
    task::JoinSet,
    time::{Instant, sleep, sleep_until},
    try_join,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    run(&cluster).await
}

async fn run(cluster: &Cluster) -> anyhow::Result<()> {
    let num_running_nodes = (2 * NUM_FAULTY_NODES + 1) * NUM_SHARDS as u16;

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
        sleep(Duration::from_millis(2000)).await;
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

    let shard_size = 2 * NUM_FAULTY_NODES + 1;
    let ips = server_instances
        .iter()
        .map(|instance| instance.private_ip)
        .collect::<Vec<_>>()
        .chunks_exact(shard_size as _)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<_>>();

    println!("load servers");
    let replica_items = server_instances.iter().enumerate().map(|(i, instance)| {
        let schema = big_schema::ReplicaTask {
            node_index: (i % shard_size as usize) as _,
            ips: ips[i / shard_size as usize].clone(),
            latencies: NETWORK.to_latencies(),
            config: big_schema::ReplicaConfig {
                num_nodes: num_nodes(),
                num_faulty_nodes: NUM_FAULTY_NODES,
            },
            storage: STORAGE,
            app: APP,
        };
        (instance, Task::Replica(schema))
    });
    load_all(replica_items, control_client.clone()).await?;

    println!("start servers");
    start_all(server_instances, control_client.clone()).await?;

    println!("load clients");
    let client_items = client_instances.iter().enumerate().map(|(i, instance)| {
        let client_task = big_schema::ClientTask {
            ips: ips.clone(),
            config: big_schema::ClientConfig {
                num_nodes: num_nodes(),
                num_faulty_nodes: NUM_FAULTY_NODES,
            },
            workload_config: big_schema::ClientWorkloadConfig {
                num_concurrent: NUM_CONCURRENT,
                app: match APP {
                    big_schema::App::Ycsb => {
                        big_schema::WorkloadConfig::Ycsb(big_schema::YcsbWorkloadConfig {
                            num_keys: NUM_KEYS,
                            read_ratio: READ_RATIO,
                            num_shards: NUM_SHARDS,
                        })
                    }
                    big_schema::App::Utxo => {
                        big_schema::WorkloadConfig::Utxo(big_schema::UtxoWorkloadConfig {
                            num_outputs: NUM_KEYS,
                        })
                    }
                },
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

async fn start_all(
    instances: impl IntoIterator<Item = &Instance>,
    control_client: Client,
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();
    for instance in instances {
        let client = control_client.clone();
        let url = format!("http://{}:3000/start", instance.public_dns);
        tasks.spawn(async move { client.post(url).send().await });
    }
    while let Some(result) = tasks.join_next().await {
        result??.error_for_status()?;
    }
    Ok(())
}
