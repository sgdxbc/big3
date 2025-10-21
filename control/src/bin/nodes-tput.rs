use std::{fmt::Write as _, time::Duration};

use big_control::{
    Cluster, Instance,
    configs::{NUM_KEYS, READ_RATIO},
    load_all, run_endpoints, stop_all,
};
use big_schema::{Scrape, Task};
use hdrhistogram::serialization::Deserializer;
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

const CLIENT_RATE: f64 = 10_000.0;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    let agg_client_rate = CLIENT_RATE * cluster.clients.len() as f64;

    let mut data = String::from("num_faulty_nodes,tput,p50,p95,p99,_notes\n");
    writeln!(
        &mut data,
        ",,,,,\"num of keys = {}, read ratio = {}\"",
        NUM_KEYS, READ_RATIO
    )?;
    for num_faulty_nodes in [1, 2, 3] {
        println!("running with num_faulty_nodes = {}", num_faulty_nodes);
        let run = run(&cluster, num_faulty_nodes).await?;
        anyhow::ensure!(
            run.tput < agg_client_rate * 0.8,
            "throughput {} exceeds client rate {} * 0.8",
            run.tput,
            agg_client_rate
        );
        writeln!(
            &mut data,
            "{},{},{},{},{}",
            num_faulty_nodes,
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

struct Run {
    tput: f64,
    p50: Duration,
    p95: Duration,
    p99: Duration,
}

async fn run(cluster: &Cluster, num_faulty_nodes: u16) -> anyhow::Result<Run> {
    let endpoints = run_endpoints(
        [
            &cluster.servers[..num_nodes(num_faulty_nodes) as usize],
            &cluster.clients,
        ]
        .concat(),
    );
    let endpoints = async {
        let result = endpoints.await;
        sleep(Duration::from_millis(1000)).await;
        result
    };
    let workload = run_workload(
        &cluster.servers[..num_nodes(num_faulty_nodes) as usize],
        &cluster.clients,
        num_faulty_nodes,
    );
    let ((), run) = try_join!(endpoints, workload)?;
    Ok(run)
}

async fn run_workload(
    server_instances: &[Instance],
    client_instances: &[Instance],
    num_faulty_nodes: u16,
) -> anyhow::Result<Run> {
    let control_client = Client::new();
    println!("wait for servers to boot");
    sleep(Duration::from_millis(2000)).await;

    let ips = server_instances
        .iter()
        .map(|instance| instance.private_ip)
        .collect::<Vec<_>>();

    println!("load servers");
    let replica_items = server_instances
        .iter()
        .enumerate()
        .map(|(node_index, instance)| {
            let schema = big_schema::ReplicaTask {
                node_index: node_index as _,
                ips: ips.clone(),
                config: big_schema::ReplicaConfig {
                    num_nodes: num_nodes(num_faulty_nodes),
                    num_faulty_nodes,
                },
            };
            (instance, Task::Replica(schema))
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
        worker_config: big_schema::ClientWorkerConfig {
            rate: CLIENT_RATE,
            num_keys: NUM_KEYS,
            read_ratio: READ_RATIO,
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
    sleep(Duration::from_secs(10)).await;
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

async fn scrape_all(
    instances: impl IntoIterator<Item = &Instance>,
    control_client: Client,
) -> anyhow::Result<Run> {
    let mut tasks = JoinSet::new();
    for instance in instances {
        let client = control_client.clone();
        let url = format!("http://{}:3000/scrape", instance.public_dns);
        tasks.spawn(async move { client.post(url).send().await });
    }
    let mut agg_throughput = 0.;
    let mut agg_histogram = hdrhistogram::Histogram::<u64>::new(3).unwrap();
    while let Some(result) = tasks.join_next().await {
        let scrape = result??.error_for_status()?.json::<Scrape>().await?;
        let latency_histogram =
            Deserializer::new().deserialize::<u64, _>(&mut &*scrape.latency_histogram)?;
        let throughput = latency_histogram.len() as f64 / scrape.interval.as_secs_f64();
        let p50 = Duration::from_nanos(latency_histogram.value_at_quantile(0.5));
        let p95 = Duration::from_nanos(latency_histogram.value_at_quantile(0.95));
        let p99 = Duration::from_nanos(latency_histogram.value_at_quantile(0.99));
        println!(
            "interval {:12?}, throughput {throughput:.0} req/s, p50 {p50:?}, p95 {p95:?}, p99 {p99:?}",
            scrape.interval
        );

        agg_throughput += throughput;
        agg_histogram += latency_histogram;
    }
    let agg_p50 = Duration::from_nanos(agg_histogram.value_at_quantile(0.5));
    let agg_p95 = Duration::from_nanos(agg_histogram.value_at_quantile(0.95));
    let agg_p99 = Duration::from_nanos(agg_histogram.value_at_quantile(0.99));
    println!(
        "AGGREGATE: throughput {agg_throughput:.0} req/s, p50 {agg_p50:?}, p95 {agg_p95:?}, p99 {agg_p99:?}",
    );
    Ok(Run {
        tput: agg_throughput,
        p50: agg_p50,
        p95: agg_p95,
        p99: agg_p99,
    })
}
