use std::net::IpAddr;

use big_schema::{Stopped, Task};
use reqwest::Client;
use serde::{Deserialize, de::DeserializeOwned};
use tokio::{fs, process::Command, task::JoinSet};

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
            // let output = instance.ssh().arg("./big").output().await?;
            let output = instance
                .ssh()
                .arg("RUST_LOG=info,big::consensus=trace,big::execute=trace ./big")
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
    Ok(())
}

pub async fn load_all(
    items: impl IntoIterator<Item = (&Instance, Task)>,
    control_client: Client,
) -> anyhow::Result<()> {
    let mut tasks = JoinSet::new();
    for (instance, task) in items {
        let client = control_client.clone();
        let url = format!("http://{}:3000/load", instance.public_dns);
        tasks.spawn(async move { client.post(url).json(&task).send().await });
    }
    while let Some(result) = tasks.join_next().await {
        result??.error_for_status()?;
    }
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
