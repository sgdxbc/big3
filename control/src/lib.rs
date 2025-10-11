use std::net::Ipv4Addr;

use serde::{Deserialize, de::DeserializeOwned};
use tokio::process::Command;

#[derive(Debug, Deserialize)]
struct TerraformOutputInstances(Vec<TerraformOutputInstance>);

#[derive(Debug, Clone, Deserialize)]
pub struct TerraformOutputInstance {
    pub private_ip: Ipv4Addr,
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
