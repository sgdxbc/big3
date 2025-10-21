use std::time::Duration;

use big_control::{
    Cluster, Instance,
    configs::{NUM_FAULTY_NODES, NUM_KEYS, READ_RATIO, num_nodes},
    load_all, run_endpoints, stop_all,
};
use big_schema::{Scrape, Task};
use hdrhistogram::serialization::Deserializer;
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
    let endpoints =
        run_endpoints([&cluster.servers[..num_nodes() as usize], &cluster.clients].concat());
    let endpoints = async {
        let result = endpoints.await;
        sleep(Duration::from_millis(1000)).await;
        result
    };
    let workload = run_workload(&cluster.servers[..num_nodes() as usize], &cluster.clients);
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

    println!("load servers");
    let replica_items = server_instances
        .iter()
        .enumerate()
        .map(|(node_index, instance)| {
            let schema = big_schema::ReplicaTask {
                node_index: node_index as _,
                ips: ips.clone(),
                config: big_schema::ReplicaConfig {
                    num_nodes: num_nodes(),
                    num_faulty_nodes: NUM_FAULTY_NODES,
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
            num_nodes: num_nodes(),
            num_faulty_nodes: NUM_FAULTY_NODES,
        },
        worker_config: big_schema::ClientWorkerConfig {
            rate: 30_000.,
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

    let mut next_scrape = Instant::now() + Duration::from_secs(1);
    for i in 0..20 {
        sleep_until(next_scrape).await;
        println!("scrape clients round {}", i + 1);
        scrape_all(client_instances, control_client.clone()).await?;
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

async fn scrape_all(
    instances: impl IntoIterator<Item = &Instance>,
    control_client: Client,
) -> anyhow::Result<(f64, Duration, Duration, Duration)> {
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
    Ok((agg_throughput, agg_p50, agg_p95, agg_p99))
}
