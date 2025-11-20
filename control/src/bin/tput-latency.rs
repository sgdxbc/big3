use std::{fmt::Write as _, time::Duration};

use big_control::{
    Cluster, Instance, PerformanceMetrics,
    configs::{APP, NETWORK, NUM_KEYS, Network, READ_RATIO, STORAGE, STRIPE_INTERVAL},
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

fn num_nodes(num_faulty_nodes: u16) -> u16 {
    3 * num_faulty_nodes + 1
}

#[derive(Debug)]
enum Setting {
    Full,
    Sharded,
    Big,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;

    let mut data = String::from("network,app,setting,num_nodes,tput,p50,p99,_notes,_ignore\n");
    writeln!(
        &mut data,
        ",,,,,,,\"num of keys = {}, read ratio = {}\",true",
        NUM_KEYS,
        if matches!(APP, big_schema::App::Ycsb) {
            READ_RATIO.to_string()
        } else {
            "n/a".to_string()
        }
    )?;
    let t = format!("{:?},{:?}", NETWORK, APP);
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
            for &num_concurrent in match NETWORK {
                Network::Lan => &[0, 10, 50, 100, 300, 600, 1000, 2000, 4000][..],
                Network::Wan => &[
                    0, 3000, 6000, 10_000, 13_000, 16_000, 20_000, 30_000, 40_000,
                ],
            } {
                println!("running YCSB Full with num_concurrent = {}", num_concurrent);
                metrics = run(&cluster, 33, 1, num_concurrent).await?;
                writeln!(
                    &mut data,
                    "{t},{:?},{},{},\"concurrent = {}\"",
                    Setting::Full,
                    num_nodes(33),
                    s(metrics),
                    num_concurrent
                )?;
            }
            // for &num_concurrent in match NETWORK {
            //     Network::Lan => &[0, 1000, 2000, 4000][..],
            //     Network::Wan => &[0, 10_000, 20_000, 30_000, 40_000],
            // } {
            //     println!(
            //         "running YCSB Full Sharded with num_concurrent = {}",
            //         num_concurrent
            //     );
            //     metrics = run(&cluster, 3, 10, num_concurrent).await?;
            //     writeln!(
            //         &mut data,
            //         "{t},{:?},{},{},\"concurrent = {}\"",
            //         Setting::Sharded,
            //         num_nodes(3) * 10,
            //         s(metrics),
            //         num_concurrent
            //     )?;
            // }
        }
        (big_schema::App::Ycsb, Storage::Big) => {
            for &num_concurrent in match NETWORK {
                Network::Lan => &[0, 100, 300, 600, 1000, 3000, 6000, 10_000, 20_000, 30_000][..],
                Network::Wan => &[0, 1_000, 5_000, 10_000, 30_000, 60_000, 100_000, 130_000],
            } {
                println!("running YCSB Big with num_concurrent = {}", num_concurrent);
                metrics = run(&cluster, 33, 1, num_concurrent).await?;
                writeln!(
                    &mut data,
                    "{t},{:?},{},{},\"concurrent = {}\"",
                    Setting::Big,
                    num_nodes(33),
                    s(metrics),
                    num_concurrent
                )?;
            }
        }
        (big_schema::App::Utxo, Storage::Full) => {
            for &num_concurrent in match NETWORK {
                Network::Lan => &[0, 10, 50, 100, 200, 300, 500, 1000, 2000, 4000][..],
                Network::Wan => &[
                    0, 4000, 6000, 8000, 10_000, 13_000, 16_000, 20_000, 30_000, 40_000,
                ],
            } {
                println!("running UTXO Full with num_concurrent = {}", num_concurrent);
                metrics = run(&cluster, 33, 1, num_concurrent).await?;
                writeln!(
                    &mut data,
                    "{t},{:?},{},{},\"concurrent = {}\"",
                    Setting::Full,
                    num_nodes(33),
                    s(metrics),
                    num_concurrent
                )?;
            }
            // for &num_concurrent in match NETWORK {
            //     Network::Lan => &[0, 1000, 2000, 4000, 6000, 8000, 10_000, 15_000][..],
            //     Network::Wan => &[0, 30_000, 60_000, 100_000, 200_000, 300_000],
            // } {
            //     println!(
            //         "running UTXO Full Sharded with num_concurrent = {}",
            //         num_concurrent
            //     );
            //     metrics = run(&cluster, 3, 10, num_concurrent).await?;
            //     writeln!(
            //         &mut data,
            //         "{t},{:?},{},{},\"concurrent = {}\"",
            //         Setting::Sharded,
            //         num_nodes(3) * 10,
            //         s(metrics),
            //         num_concurrent
            //     )?;
            // }
        }
        (big_schema::App::Utxo, Storage::Big) => {
            for &num_concurrent in match NETWORK {
                Network::Lan => &[0, 100, 300, 600, 1000, 3000, 6000, 10_000, 20_000, 30_000][..],
                Network::Wan => &[0, 1_000, 5_000, 10_000, 30_000, 60_000, 100_000, 130_000],
            } {
                println!("running UTXO Big with num_concurrent = {}", num_concurrent);
                metrics = run(&cluster, 33, 1, num_concurrent).await?;
                writeln!(
                    &mut data,
                    "{t},{:?},{},{},\"concurrent = {}\"",
                    Setting::Big,
                    num_nodes(33),
                    s(metrics),
                    num_concurrent
                )?;
            }
        }
    }

    create_dir_all("data").await?;
    let mut data_file = File::create("data/tput-latency-scratch.csv").await?;
    data_file.write_all(data.as_bytes()).await?;
    Ok(())
}

async fn run(
    cluster: &Cluster,
    num_faulty_nodes: u16,
    num_shards: u8,
    num_concurrent: u32,
) -> anyhow::Result<PerformanceMetrics> {
    let num_running_nodes = (2 * num_faulty_nodes + 1) * num_shards as u16;

    let (clients, num_concurrent) = if num_concurrent == 0 {
        (&cluster.clients[..1], 1)
    } else {
        (&cluster.clients[..], num_concurrent)
    };

    let endpoints =
        run_endpoints([&cluster.servers[..num_running_nodes as usize], clients].concat());
    let workload = run_workload(
        &cluster.servers[..num_running_nodes as usize],
        clients,
        num_faulty_nodes,
        num_shards,
        num_concurrent,
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
    num_concurrent: u32,
) -> anyhow::Result<PerformanceMetrics> {
    if num_shards > 1 {
        assert!(matches!(STORAGE, Storage::Full));
    }
    assert!(STRIPE_INTERVAL >= Duration::from_hours(1));

    let control_client = Client::new();
    println!("wait for servers to boot");
    sleep(Duration::from_millis(2000)).await;

    let shard_size = 2 * num_faulty_nodes + 1;
    assert!(server_instances.len().is_multiple_of(shard_size as usize));
    let num_shards = (server_instances.len() / shard_size as usize) as _;

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
            num_shards,
            shard_index,
            ips: ips[shard_index as usize].clone(),
            latencies: NETWORK.to_latencies(),
            config: big_schema::ReplicaConfig {
                num_nodes: num_nodes(num_faulty_nodes),
                num_faulty_nodes,
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
                num_nodes: num_nodes(num_faulty_nodes),
                num_faulty_nodes,
            },
            workload_config: big_schema::ClientWorkloadConfig {
                num_concurrent,
                num_shards,
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
