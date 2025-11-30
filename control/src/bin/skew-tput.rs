use std::{fmt::Write as _, time::Duration};

use big_control::{
    Cluster, Instance, PerformanceMetrics,
    configs::{
        APP, App, CACHE_SIZE, NETWORK, NUM_KEYS, READ_RATIO, SHARDING, STORAGE, STRIPE_INTERVAL,
        dump_all,
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
    let mut run = Run {
        cluster,
        data: String::new(),
    };
    run.init();

    match STORAGE {
        Storage::Full => {
            run.perform(1.24, 15_000).await?;
            run.perform(0.99, 5_000).await?;
            run.perform(0.5, 2_000).await?;
            run.perform(0., 1_500).await?;
        }
        Storage::Big => {
            run.perform(1.24, 20_000).await?;
            run.perform(0.99, 6_000).await?;
            run.perform(0.5, 3_000).await?;
            run.perform(0., 3_000).await?;
        }
    }

    create_dir_all("data").await?;
    let mut data_file = File::create("data/skew-tput-scratch.csv").await?;
    data_file.write_all(run.data.as_bytes()).await?;
    Ok(())
}

struct Run {
    cluster: Cluster,
    data: String,
}

impl Run {
    fn init(&mut self) {
        self.data = String::from("network,app,setting,skewness,tput,p50,p99,_notes,_ignore\n");
        writeln!(&mut self.data, ",,,,,,,\"{}\",true", dump_all()).unwrap();
    }

    async fn perform(&mut self, skewness: f64, num_concurrent: u32) -> anyhow::Result<()> {
        let setting = match STORAGE {
            Storage::Full if SHARDING.num_shards == 1 => Setting::Full,
            Storage::Big if SHARDING.num_shards == 1 => Setting::Big,
            _ => panic!("invalid setting"),
        };
        let run = run(&self.cluster, skewness, num_concurrent).await?;
        writeln!(
            &mut self.data,
            "{:?},{:?},{:?},{},{},{},{},\"num_concurrent = {}\"",
            NETWORK,
            APP,
            setting,
            skewness,
            run.tput,
            run.p50.as_secs_f64(),
            run.p99.as_secs_f64(),
            num_concurrent,
        )?;
        Ok(())
    }
}

async fn run(
    cluster: &Cluster,
    skewness: f64,
    num_concurrent: u32,
) -> anyhow::Result<PerformanceMetrics> {
    assert_eq!(SHARDING.num_nodes(), 100);
    assert_eq!(SHARDING.num_shards, 1);
    assert!(matches!(APP, App::Ycsb));

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
        skewness,
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
    skewness: f64,
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
            num_shards,
            shard_index,
            shard_node_index: (i % num_shard_running_nodes as usize) as _,
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
            checkpoint: false,
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
                app: big_schema::WorkloadConfig::Ycsb(big_schema::YcsbWorkloadConfig {
                    num_keys: NUM_KEYS,
                    read_ratio: READ_RATIO,
                    theta: skewness,
                }),
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
