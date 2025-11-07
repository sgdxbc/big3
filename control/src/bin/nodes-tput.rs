use std::{fmt::Write as _, time::Duration};

use big_control::{
    Cluster, Instance, PerformanceMetrics,
    configs::{APP, NUM_KEYS, READ_RATIO, STORAGE},
    load_all, run_endpoints, scrape_all, stop_all,
};
use big_schema::{Storage, Task};
use reqwest::Client;
use tokio::{
    fs::{File, create_dir_all},
    io::AsyncWriteExt,
    task::JoinSet,
    time::sleep,
    try_join,
};

fn num_nodes(num_faulty_nodes: u16) -> u16 {
    3 * num_faulty_nodes + 1
}

#[derive(Debug)]
enum Setting {
    YcsbFull,
    YcsbSharded,
    YcsbBig,
    UtxoFull,
    // UtxoSharded,
    UtxoBig,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;

    let mut data = String::from("setting,num_nodes,tput,p50,p99,_notes\n");
    writeln!(
        &mut data,
        ",,,,,\"num of keys = {}, read ratio = {}\"",
        NUM_KEYS,
        if matches!(APP, big_schema::App::Ycsb) {
            READ_RATIO.to_string()
        } else {
            "n/a".to_string()
        }
    )?;
    let s = |run: PerformanceMetrics| {
        format!(
            "{},{},{}",
            run.tput,
            run.p50.as_secs_f64(),
            run.p99.as_secs_f64(),
        )
    };

    let mut metrics;
    match (APP, STORAGE) {
        (big_schema::App::Ycsb, Storage::Full) => {
            for num_faulty_nodes in [1, 3, 8, 13, 18, 23, 28, 33] {
                println!(
                    "running YCSB Full with num_faulty_nodes = {}",
                    num_faulty_nodes
                );
                metrics = run(&cluster, num_faulty_nodes, 1).await?;
                writeln!(
                    &mut data,
                    "{:?},{},{}",
                    Setting::YcsbFull,
                    num_nodes(num_faulty_nodes),
                    s(metrics)
                )?;
            }
            for num_shards in [2, 4, 6, 8, 10] {
                println!("running YCSB Full with num_shards = {}", num_shards,);
                metrics = run(&cluster, 3, num_shards).await?;
                writeln!(
                    &mut data,
                    "{:?},{},{}",
                    Setting::YcsbSharded,
                    num_nodes(3) * num_shards as u16,
                    s(metrics)
                )?;
            }
        }
        (big_schema::App::Ycsb, Storage::Big) => {
            for num_faulty_nodes in [1, 3, 8, 13, 18, 23, 28, 33] {
                println!(
                    "running YCSB Big with num_faulty_nodes = {}",
                    num_faulty_nodes
                );
                metrics = run(&cluster, num_faulty_nodes, 1).await?;
                writeln!(
                    &mut data,
                    "{:?},{},{}",
                    Setting::YcsbBig,
                    num_nodes(num_faulty_nodes),
                    s(metrics)
                )?;
            }
        }
        (big_schema::App::Utxo, Storage::Full) => {
            for num_faulty_nodes in [1, 3, 8, 13, 18, 23, 28, 33] {
                println!(
                    "running UTXO Full with num_faulty_nodes = {}",
                    num_faulty_nodes
                );
                metrics = run(&cluster, num_faulty_nodes, 1).await?;
                writeln!(
                    &mut data,
                    "{:?},{},{}",
                    Setting::UtxoFull,
                    num_nodes(num_faulty_nodes),
                    s(metrics)
                )?;
            }
            // TODO sharded UTXO
        }
        (big_schema::App::Utxo, Storage::Big) => {
            for num_faulty_nodes in [1, 3, 8, 13, 18, 23, 28, 33] {
                println!(
                    "running UTXO Big with num_faulty_nodes = {}",
                    num_faulty_nodes
                );
                metrics = run(&cluster, num_faulty_nodes, 1).await?;
                writeln!(
                    &mut data,
                    "{:?},{},{}",
                    Setting::UtxoBig,
                    num_nodes(num_faulty_nodes),
                    s(metrics)
                )?;
            }
        }
    }

    create_dir_all("data").await?;
    let mut data_file = File::create(format!("data/nodes-tput-{APP:?}-{STORAGE:?}.csv")).await?;
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
        num_faulty_nodes,
        num_shards,
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
    num_faulty_nodes: u16,
    num_shards: u8,
) -> anyhow::Result<PerformanceMetrics> {
    if num_shards > 1 {
        assert!(matches!(STORAGE, Storage::Full));
    }

    let control_client = Client::new();
    println!("wait for servers to boot");
    sleep(Duration::from_millis(2000)).await;

    let shard_size = 2 * num_faulty_nodes + 1;
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
            latencies: None,
            config: big_schema::ReplicaConfig {
                num_nodes: num_nodes(num_faulty_nodes),
                num_faulty_nodes,
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
                num_nodes: num_nodes(num_faulty_nodes),
                num_faulty_nodes,
            },
            workload_config: big_schema::ClientWorkloadConfig {
                num_concurrent: match (STORAGE, num_shards) {
                    (Storage::Full, 1) => 1000,
                    (Storage::Big, 1) => 1000,
                    (Storage::Full, _) => todo!(),
                    _ => unimplemented!(),
                },
                app: match APP {
                    big_schema::App::Ycsb => {
                        big_schema::WorkloadConfig::Ycsb(big_schema::YcsbWorkloadConfig {
                            num_keys: NUM_KEYS,
                            read_ratio: READ_RATIO,
                            num_shards,
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
