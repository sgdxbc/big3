use std::time::Duration;

use big_control::{Cluster, Instance};
use big_schema::{Stopped, Task};
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
        worker_config: big_schema::ClientWorkerConfig { num_concurrent: 10 },
    };
    load_all(
        &client_instances,
        Task::Client(client_task),
        control_client.clone(),
    )
    .await?; //
    println!("start clients");
    start_all(&client_instances, control_client.clone()).await?;

    sleep(Duration::from_secs(10)).await;

    println!("stop clients");
    stop_all(&client_instances, control_client.clone()).await?;
    println!("stop servers");
    stop_all(&server_instances, control_client.clone()).await?;
    Ok(())
}

async fn load_all(
    instances: impl IntoIterator<Item = &Instance>,
    task: Task,
    control_client: Client,
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();
    for instance in instances {
        let client = control_client.clone();
        let url = format!("http://{}:3000/load", instance.public_dns);
        let task = task.clone();
        tasks.spawn(async move { client.post(url).json(&task).send().await });
    }
    while let Some(result) = tasks.join_next().await {
        result??.error_for_status()?;
    }
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

async fn stop_all(
    instances: impl IntoIterator<Item = &Instance>,
    control_client: Client,
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();
    for instance in instances {
        let client = control_client.clone();
        let url = format!("http://{}:3000/stop", instance.public_dns);
        tasks.spawn(async move { client.post(url).send().await });
    }
    while let Some(result) = tasks.join_next().await {
        result??.error_for_status()?.json::<Stopped>().await?;
    }
    Ok(())
}
