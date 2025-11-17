use std::{fmt::Write as _, time::Duration};

use big_control::{
    Cluster, Instance, PerformanceMetrics,
    configs::{
        APP, NETWORK, NUM_CONCURRENT, NUM_FAULTY_NODES, NUM_KEYS, NUM_SHARDS, READ_RATIO, STORAGE,
        STRIPE_INTERVAL, num_nodes,
    },
    load_all, run_endpoints, scrape_all, start_all, stop_all,
};
use big_schema::{Storage, Task};
use reqwest::Client;
use tokio::{
    fs::{File, create_dir_all},
    io::AsyncWriteExt,
    time::sleep,
    try_join,
};

#[derive(Debug)]
enum Setting {
    Full,
    Sharded,
    Big,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;

    let mut data = String::from("network,app,setting,num_nodes,tput,p50,p99,_notes\n");
    writeln!(
        &mut data,
        ",,,,,,,\"num of keys = {}, read ratio = {}\"",
        NUM_KEYS,
        if matches!(APP, big_schema::App::Ycsb) {
            READ_RATIO.to_string()
        } else {
            "n/a".to_string()
        }
    )?;

    let setting = match STORAGE {
        Storage::Full if NUM_SHARDS == 1 => Setting::Full,
        Storage::Full => Setting::Sharded,
        Storage::Big => Setting::Big,
    };

    let metrics = run(&cluster, NUM_FAULTY_NODES, NUM_SHARDS).await?;
    writeln!(
        &mut data,
        "{:?},{:?},{:?},{},{},{},{}",
        NETWORK,
        APP,
        setting,
        num_nodes() * NUM_SHARDS as u16,
        metrics.tput,
        metrics.p50.as_secs_f64(),
        metrics.p99.as_secs_f64(),
    )?;

    create_dir_all("data").await?;
    let mut data_file = File::create("data/nodes-tput-scratch.csv").await?;
    data_file.write_all(data.as_bytes()).await?;
    Ok(())
}

async fn run(
    cluster: &Cluster,
    num_faulty_nodes: u16,
    num_shards: u8,
) -> anyhow::Result<PerformanceMetrics> {
    let num_running_nodes = (2 * num_faulty_nodes + 1) * num_shards as u16;
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
        sleep(Duration::from_millis(1000)).await;
        result
    };
    let ((), run) = try_join!(endpoints, workload)?;
    Ok(run)
}

async fn run_workload(
    server_instances: &[Instance],
    client_instances: &[Instance],
) -> anyhow::Result<PerformanceMetrics> {
    if NUM_SHARDS > 1 {
        assert!(matches!(STORAGE, Storage::Full));
    }
    assert!(STRIPE_INTERVAL >= Duration::from_hours(1));

    let control_client = Client::new();
    println!("wait for servers to boot");
    sleep(Duration::from_millis(2000)).await;

    let shard_size = 2 * NUM_FAULTY_NODES + 1;
    assert!(server_instances.len().is_multiple_of(shard_size as usize));
    assert_eq!(
        server_instances.len(),
        shard_size as usize * NUM_SHARDS as usize
    );

    let ips = server_instances
        .iter()
        .map(|instance| instance.private_ip)
        .collect::<Vec<_>>()
        .chunks_exact(shard_size as _)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<_>>();

    println!("load servers");
    let replica_items = server_instances.iter().enumerate().map(|(i, instance)| {
        let shard_index = (i / shard_size as usize) as _;
        let schema = big_schema::ReplicaTask {
            node_index: (i % shard_size as usize) as _,
            num_shards: NUM_SHARDS,
            shard_index,
            ips: ips[shard_index as usize].clone(),
            latencies: NETWORK.to_latencies(),
            config: big_schema::ReplicaConfig {
                num_nodes: num_nodes(),
                num_faulty_nodes: NUM_FAULTY_NODES,
            },
            storage: STORAGE,
            app: APP,
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
            ips: ips.clone(),
            config: big_schema::ClientConfig {
                num_nodes: num_nodes(),
                num_faulty_nodes: NUM_FAULTY_NODES,
            },
            workload_config: big_schema::ClientWorkloadConfig {
                num_concurrent: NUM_CONCURRENT,
                num_shards: NUM_SHARDS,
                app: match APP {
                    big_schema::App::Ycsb => {
                        big_schema::WorkloadConfig::Ycsb(big_schema::YcsbWorkloadConfig {
                            num_keys: NUM_KEYS,
                            read_ratio: READ_RATIO,
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

    sleep(Duration::from_secs(10)).await;
    println!("scrape and discard warmup data");
    scrape_all(client_instances, control_client.clone()).await?;
    sleep(Duration::from_secs(30)).await;
    println!("scrape measured data");
    let run = scrape_all(client_instances, control_client.clone()).await?;

    println!("stop clients");
    stop_all(client_instances, control_client.clone()).await?;
    println!("stop servers");
    stop_all(server_instances, control_client.clone()).await?;
    println!("done");
    Ok(run)
}
