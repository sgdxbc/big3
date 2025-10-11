use std::time::Duration;

use big_control::{Cluster, Instance};
use reqwest::Client;
use tokio::{fs, task::JoinSet, time::sleep, try_join};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = fs::remove_dir_all("log").await;
    fs::create_dir("log").await?;
    fs::write("log/.gitignore", "*").await?;
    fs::create_dir("log/stderr").await?;

    let cluster = Cluster::from_terraform().await?;
    let endpoints = run_endpoints([cluster.servers.clone(), cluster.clients.clone()].concat());
    let workload = run_workload(cluster.servers, cluster.clients);
    try_join!(endpoints, workload)?;
    Ok(())
}

async fn run_endpoints(instances: impl IntoIterator<Item = Instance>) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();
    for instance in instances {
        tasks.spawn(async move {
            let output = instance.ssh().arg("./big").output().await?;
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
    Ok(())
}

async fn run_workload(
    server_instances: Vec<Instance>,
    client_instances: Vec<Instance>,
) -> anyhow::Result<()> {
    let control_client = Client::new();
    sleep(Duration::from_secs(3)).await;
    request_all(&server_instances, "load", control_client.clone()).await?;
    request_all(&server_instances, "start", control_client.clone()).await?;
    request_all(&client_instances, "load", control_client.clone()).await?;
    request_all(&client_instances, "start", control_client.clone()).await?;
    // TODO wait for a while
    request_all(&client_instances, "stop", control_client.clone()).await?;
    request_all(&server_instances, "stop", control_client.clone()).await?;
    Ok(())
}

async fn request_all(
    instances: impl IntoIterator<Item = &Instance>,
    path: &str,
    control_client: Client,
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();
    for instance in instances {
        let client = control_client.clone();
        let url = format!("http://{}:3000/{}", instance.public_dns, path);
        tasks.spawn(async move { client.post(url).send().await });
    }
    while let Some(result) = tasks.join_next().await {
        result??.error_for_status()?;
    }
    Ok(())
}
