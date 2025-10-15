use std::time::Duration;

use big_control::{Cluster, Instance, load_all, run_endpoints, stop_all};
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
    let endpoints = run_endpoints([cluster.servers.clone(), cluster.clients.clone()].concat());
    let workload = run_workload(cluster.servers, cluster.clients);
    try_join!(endpoints, workload)?;
    Ok(())
}

async fn run_workload(
    server_instances: Vec<Instance>,
    client_instances: Vec<Instance>,
) -> anyhow::Result<()> {
    let control_client = Client::new();
    sleep(Duration::from_secs(3)).await;
    println!("load servers");
    load_all(&server_instances, Task::Replica, control_client.clone()).await?;
    println!("start servers");
    start_all(&server_instances, control_client.clone()).await?;

    println!("load clients");
    let client_task = big_schema::ClientTask {
        addrs: server_instances
            .iter()
            .map(|instance| (instance.private_ip, 5000).into())
            .collect(),
        config: big_schema::ClientConfig {
            num_nodes: 1,
            num_faulty_nodes: 0,
        },
        worker_config: big_schema::ClientWorkerConfig { num_concurrent: 40 },
    };
    load_all(
        &client_instances,
        Task::Client(client_task),
        control_client.clone(),
    )
    .await?; //
    println!("start clients");
    start_all(&client_instances, control_client.clone()).await?;

    let mut next_scrape = Instant::now() + Duration::from_secs(1);
    for _ in 0..10 {
        sleep_until(next_scrape).await;
        println!("scrape clients");
        scrape_all(&client_instances, control_client.clone()).await?;
        next_scrape += Duration::from_secs(1);
    }

    println!("stop clients");
    stop_all(&client_instances, control_client.clone()).await?;
    println!("stop servers");
    stop_all(&server_instances, control_client.clone()).await?;
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
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();
    for instance in instances {
        let client = control_client.clone();
        let url = format!("http://{}:3000/scrape", instance.public_dns);
        tasks.spawn(async move { client.post(url).send().await });
    }
    while let Some(result) = tasks.join_next().await {
        let scrape = result??.error_for_status()?.json::<Scrape>().await?;
        let latency_histogram =
            Deserializer::new().deserialize::<u64, _>(&mut &*scrape.latency_histogram)?;
        let throughput = latency_histogram.len() as f64 / scrape.interval.as_secs_f64();
        let p50 = Duration::from_nanos(latency_histogram.value_at_quantile(0.5));
        let p95 = Duration::from_nanos(latency_histogram.value_at_quantile(0.95));
        let p99 = Duration::from_nanos(latency_histogram.value_at_quantile(0.99));
        println!(
            "interval {:?}, throughput {throughput:.0} req/s, p50 {p50:?}, p95 {p95:?}, p99 {p99:?}",
            scrape.interval
        );
    }
    Ok(())
}
