use std::{collections::HashSet, net::IpAddr, time::Duration};

use big_schema::{Scrape, Stopped, Task};
use hdrhistogram::serialization::Deserializer;
use reqwest::Client;
use serde::{Deserialize, de::DeserializeOwned};
use tokio::{
    fs,
    process::Command,
    task::JoinSet,
    time::{Instant, sleep, timeout_at},
};

pub mod configs;

#[derive(Debug, Deserialize)]
struct TerraformOutputInstances(Vec<TerraformOutputInstance>);

#[derive(Debug, Clone, Deserialize)]
pub struct TerraformOutputInstance {
    pub private_ip: IpAddr,
    pub public_dns: String,
}

pub type Instance = TerraformOutputInstance;

#[derive(Debug)]
pub struct Cluster {
    pub servers: Vec<Instance>,
    pub clients: Vec<Instance>,
    pub build: Instance,
}

impl Cluster {
    async fn terraform_output<T: DeserializeOwned>(key: &str) -> anyhow::Result<T> {
        let output = Command::new("terraform")
            .args(["-chdir=control/terraform", "output", "-json", key])
            .output()
            .await?;
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    pub async fn from_terraform() -> anyhow::Result<Self> {
        let servers = Self::terraform_output::<TerraformOutputInstances>("servers").await?;
        let clients = Self::terraform_output::<TerraformOutputInstances>("clients").await?;
        let build = Self::terraform_output::<TerraformOutputInstance>("build").await?;
        Ok(Self {
            servers: servers.0,
            clients: clients.0,
            build,
        })
    }
}

impl Instance {
    pub fn ssh(&self) -> Command {
        let mut cmd = Command::new("ssh");
        cmd.arg(&self.public_dns);
        cmd
    }
}

pub async fn run_endpoints(
    instances: impl IntoIterator<Item = Instance> + Clone,
) -> anyhow::Result<()> {
    let _ = fs::remove_dir_all("log").await;
    fs::create_dir("log").await?;
    fs::write("log/.gitignore", "*").await?;
    fs::create_dir("log/stderr").await?;

    let mut tasks = JoinSet::new();
    for instance in instances.clone() {
        tasks.spawn(async move {
            let output = instance
                .ssh()
                .arg("RUST_LOG=info ./big > big.log")
                // .arg("RUST_BACKTRACE=1 RUST_LOG=info,big ./big > big.log")
                .output()
                .await?;
            anyhow::Ok((instance.public_dns, output))
        });
    }
    while let Some(result) = tasks.join_next().await {
        let (dns, output) = result??;
        if !output.status.success() {
            fs::write(format!("log/stderr/{dns}.log"), output.stderr).await?;
            anyhow::bail!("instance {dns} failed");
        }
    }
    println!("endpoints joined");

    let mut tasks = JoinSet::new();
    for instance in instances {
        tasks.spawn(async move {
            let status = Command::new("rsync")
                .args([
                    "-az",
                    &format!("{}:big.log", instance.public_dns),
                    &format!("log/{}.log", instance.public_dns),
                ])
                .status()
                .await?;
            anyhow::ensure!(status.success(), "rsync failed for {}", instance.public_dns);
            anyhow::Ok(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    println!("logs collected");
    Ok(())
}

pub async fn load_all(
    items: impl IntoIterator<Item = (&Instance, Task)>,
    control_client: Client,
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();
    let mut loading_hosts = HashSet::new();
    for (instance, task) in items {
        let public_dns = instance.public_dns.clone();
        loading_hosts.insert(public_dns.clone());
        let client = control_client.clone();
        let url = format!("http://{}:3000/load", public_dns);
        tasks.spawn(async move {
            let mut retry = 3;
            loop {
                match client.post(&url).json(&task).send().await {
                    Ok(resp) => {
                        break anyhow::Ok(resp);
                    }
                    Err(err) if err.is_request() && retry > 0 => {
                        println!("load request failed: {}. retrying...", err);
                        retry -= 1;
                        sleep(Duration::from_millis(100)).await;
                    }
                    Err(err) => Err(err)?,
                }
            }
        });
    }
    let start = Instant::now();
    let mut deadline = start + Duration::from_secs(60);
    let mut i = 0;
    loop {
        match timeout_at(deadline, tasks.join_next()).await {
            Ok(None) => break,
            Ok(Some(result)) => {
                let resp = result??.error_for_status()?;
                loading_hosts.remove(resp.url().host_str().unwrap());
            }
            Err(_) => {
                i += 1;
                println!("still loading after {i} minutes: {loading_hosts:?}");
                deadline += Duration::from_secs(60);
            }
        }
    }
    println!("all loaded in {:?}", start.elapsed());
    Ok(())
}

pub async fn stop_all(
    instances: impl IntoIterator<Item = &Instance>,
    control_client: Client,
) -> anyhow::Result<Vec<Stopped>> {
    let mut tasks = JoinSet::new();
    for instance in instances {
        let client = control_client.clone();
        let url = format!("http://{}:3000/stop", instance.public_dns);
        tasks.spawn(async move { client.post(url).send().await });
    }
    let mut results = Vec::new();
    while let Some(result) = tasks.join_next().await {
        results.push(result??.error_for_status()?.json::<Stopped>().await?);
    }
    Ok(results)
}

pub struct PerformanceMetrics {
    pub tput: f64,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
}

pub async fn scrape_all(
    instances: impl IntoIterator<Item = &Instance>,
    control_client: Client,
) -> anyhow::Result<PerformanceMetrics> {
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
        let p99 = Duration::from_nanos(latency_histogram.value_at_quantile(0.99));
        println!(
            "interval {:12?}, throughput {throughput:.0} req/s, p50 {p50:?}, p99 {p99:?}",
            scrape.interval
        );

        agg_throughput += throughput;
        agg_histogram += latency_histogram;
    }
    let agg_p50 = Duration::from_nanos(agg_histogram.value_at_quantile(0.5));
    let agg_p95 = Duration::from_nanos(agg_histogram.value_at_quantile(0.95));
    let agg_p99 = Duration::from_nanos(agg_histogram.value_at_quantile(0.99));
    println!("AGGREGATE: throughput {agg_throughput:.0} req/s, p50 {agg_p50:?}, p99 {agg_p99:?}",);
    Ok(PerformanceMetrics {
        tput: agg_throughput,
        p50: agg_p50,
        p95: agg_p95,
        p99: agg_p99,
    })
}

pub async fn start_all(
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
