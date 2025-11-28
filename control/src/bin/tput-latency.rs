use std::{fmt::Write as _, time::Duration};

use big_control::{
    Cluster, Instance, PerformanceMetrics,
    configs::{
        APP, App, CACHE_SIZE, NETWORK, Network, SHARDING, STORAGE, STRIPE_INTERVAL, dump_all,
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

#[derive(Debug, Clone, Copy)]
enum Setting {
    Full,
    Sharded,
    Big,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;

    let mut data = String::from("network,app,setting,tput,p50,p99,_notes,_ignore\n");
    writeln!(&mut data, ",,,,,,\"{}\",true", dump_all())?;

    let t = format!("{:?},{:?}", NETWORK, APP);
    let s = |run: PerformanceMetrics| {
        format!(
            "{},{},{}",
            run.tput,
            run.p50.as_secs_f64(),
            run.p99.as_secs_f64(),
        )
    };

    let setting = match STORAGE {
        Storage::Full if SHARDING.num_shards == 1 => Setting::Full,
        Storage::Full if SHARDING.num_shards > 1 => Setting::Sharded,
        Storage::Big if SHARDING.num_shards == 1 => Setting::Big,
        _ => panic!("invalid setting"),
    };
    assert_eq!(SHARDING.num_nodes(), 100);

    let num_concurrent = match (NETWORK, APP, setting) {
        (Network::Lan, App::Ycsb, Setting::Full) => {
            &[0, 10, 50, 100, 300, 600, 1000, 2000, 4000][..]
        }
        (Network::Lan, App::Ycsb, Setting::Sharded) => &[0, 1000, 2000, 4000][..],
        (Network::Lan, App::Ycsb, Setting::Big) => {
            &[0, 100, 300, 600, 1000, 3000, 6000, 10_000, 20_000, 30_000][..]
        }

        (Network::Lan, App::Utxo, Setting::Full) => &[0, 100, 300, 600, 1000, 2000, 4000, 6000][..],
        (Network::Lan, App::Utxo, Setting::Sharded) => {
            &[0, 100, 300, 600, 1000, 3000, 6000, 10_000, 12_000][..]
        }
        (Network::Lan, App::Utxo, Setting::Big) => {
            &[0, 100, 300, 600, 1000, 2000, 3000, 4000, 5000][..]
        }

        (Network::Wan, App::Ycsb, Setting::Full) => &[
            0, 3000, 6000, 10_000, 13_000, 16_000, 20_000, 30_000, 40_000,
        ],
        (Network::Wan, App::Ycsb, Setting::Sharded) => &[0, 10_000, 20_000, 30_000, 40_000],
        (Network::Wan, App::Ycsb, Setting::Big) => {
            &[0, 1_000, 5_000, 10_000, 30_000, 60_000, 100_000, 130_000]
        }

        (Network::Wan, App::Utxo, Setting::Full) => &[0, 5_000, 10_000, 20_000, 40_000, 60_000],
        (Network::Wan, App::Utxo, Setting::Sharded) => &[
            0, 5_000, 10_000, 20_000, 40_000, 60_000, 80_000, 100_000, 150_000,
        ],
        (Network::Wan, App::Utxo, Setting::Big) => &[0, 5_000, 10_000, 20_000, 40_000, 60_000],
    };

    for &num_concurrent in num_concurrent {
        println!(
            "running {:?} {:?} {:?} with num_concurrent = {}",
            NETWORK, APP, setting, num_concurrent
        );
        let metrics = run(&cluster, num_concurrent).await?;
        writeln!(
            &mut data,
            "{t},{:?},{},\"concurrent = {}\"",
            setting,
            s(metrics),
            num_concurrent
        )?;
    }

    create_dir_all("data").await?;
    let mut data_file = File::create("data/tput-latency-scratch.csv").await?;
    data_file.write_all(data.as_bytes()).await?;
    Ok(())
}

async fn run(cluster: &Cluster, num_concurrent: u32) -> anyhow::Result<PerformanceMetrics> {
    let (clients, num_concurrent) = if num_concurrent == 0 {
        (&cluster.clients[..1], 1)
    } else {
        (&cluster.clients[..], num_concurrent)
    };

    let num_running_nodes = SHARDING.num_running_nodes();

    let endpoints =
        run_endpoints([&cluster.servers[..num_running_nodes as usize], clients].concat());
    let workload = run_workload(
        &cluster.servers[..num_running_nodes as usize],
        clients,
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
    num_concurrent: u32,
) -> anyhow::Result<PerformanceMetrics> {
    assert!(STRIPE_INTERVAL >= Duration::from_hours(1));

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
                num_concurrent,
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

    sleep(Duration::from_secs(match NETWORK {
        Network::Lan => 10,
        Network::Wan => 20,
    }))
    .await;
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
