use std::{fmt::Write as _, time::Duration};

use big_control::{
    Cluster, Instance, PerformanceMetrics,
    configs::{NUM_KEYS, READ_RATIO, Storage},
    load_all, run_endpoints, scrape_all, stop_all,
};
use big_schema::Task;
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
    Full,
    Sharded,
    Big,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;

    let mut data = String::from("setting,num_faulty_nodes,num_nodes,tput,p50,p95,p99,_notes\n");
    writeln!(
        &mut data,
        ",,,,,,,\"num of keys = {}, read ratio = {}\"",
        NUM_KEYS, READ_RATIO
    )?;
    for storage in [Storage::Full, Storage::Big] {
        for num_faulty_nodes in [1, 3, 8, 13, 18, 23, 28, 33] {
            println!(
                "running {:?} with num_faulty_nodes = {}",
                storage, num_faulty_nodes
            );
            let run = run(&cluster, storage, num_faulty_nodes, 1).await?;
            writeln!(
                &mut data,
                "{:?},{},{},{},{},{},{}",
                match storage {
                    Storage::Full => Setting::Full,
                    Storage::Big => Setting::Big,
                },
                num_faulty_nodes,
                3 * num_faulty_nodes + 1,
                run.tput,
                run.p50.as_secs_f64(),
                run.p95.as_secs_f64(),
                run.p99.as_secs_f64(),
            )?;
        }
    }
    for num_shards in [2, 4, 6, 8, 10] {
        println!("running Full storage with num_shards = {}", num_shards,);
        let run = run(&cluster, Storage::Full, 3, num_shards).await?;
        writeln!(
            &mut data,
            "{:?},{},{},{},{},{},{}",
            Setting::Sharded,
            3 * num_shards,
            10 * num_shards,
            run.tput,
            run.p50.as_secs_f64(),
            run.p95.as_secs_f64(),
            run.p99.as_secs_f64(),
        )?;
    }

    create_dir_all("data").await?;
    let mut data_file = File::create("data/nodes-tput.csv").await?;
    data_file.write_all(data.as_bytes()).await?;
    Ok(())
}

async fn run(
    cluster: &Cluster,
    storage: Storage,
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
        storage,
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
    storage: Storage,
    num_faulty_nodes: u16,
    num_shards: u8,
) -> anyhow::Result<PerformanceMetrics> {
    if num_shards > 1 {
        assert!(matches!(storage, Storage::Full));
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
            config: big_schema::ReplicaConfig {
                num_nodes: num_nodes(num_faulty_nodes),
                num_faulty_nodes,
            },
        };
        (
            instance,
            match storage {
                Storage::Full => Task::Full(schema),
                Storage::Big => Task::Big(schema),
            },
        )
    });
    load_all(replica_items, control_client.clone()).await?;

    println!("start servers");
    start_all(server_instances, control_client.clone()).await?;

    println!("load clients");
    let client_task = big_schema::ClientTask {
        ips,
        config: big_schema::ClientConfig {
            num_nodes: num_nodes(num_faulty_nodes),
            num_faulty_nodes,
        },
        workload_config: big_schema::WorkloadConfig {
            num_concurrent: match storage {
                Storage::Full => 1_500 * num_shards as u64,
                Storage::Big => 10_000,
            },
            num_keys: NUM_KEYS,
            read_ratio: READ_RATIO,
            num_shards,
        },
    };
    let client_items = client_instances
        .iter()
        .map(|instance| (instance, Task::Client(client_task.clone())));
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
