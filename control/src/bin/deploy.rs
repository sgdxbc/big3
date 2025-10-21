use std::time::Duration;

use big_control::{Cluster, Instance};
use tokio::{process::Command, task::JoinSet, time::sleep, try_join};

const BUILD_DIR: &str = "/home/ubuntu/big-build";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    build(&cluster.build).await?;
    let http_server = async {
        let status = cluster
            .build
            .ssh()
            .arg(format!(
                "python3 -m http.server --directory {BUILD_DIR}/target/release"
            ))
            .status()
            .await?;
        anyhow::ensure!(status.success());
        anyhow::Ok(())
    };
    let download = async {
        sleep(Duration::from_secs(1)).await;
        let mut tasks = JoinSet::new();
        for server in cluster.servers.into_iter().chain(cluster.clients) {
            tasks.spawn(
                server
                    .ssh()
                    .arg(format!(
                        "curl -s -O http://{}:8000/big && chmod +x big",
                        cluster.build.private_ip
                    ))
                    .status(),
            );
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        cluster
            .build
            .ssh()
            .arg("pkill -INT -f 'python3 -m http.server'")
            .status()
            .await?;
        Ok(())
    };
    try_join!(http_server, download)?;
    Ok(())
}

async fn build(instance: &Instance) -> anyhow::Result<()> {
    // sanity check
    let status = Command::new("cargo").args(["build", "-r"]).status().await?;
    anyhow::ensure!(status.success());
    let status = instance
        .ssh()
        .arg(format!("mkdir -p {BUILD_DIR}"))
        .status()
        .await?;
    anyhow::ensure!(status.success());
    let status = Command::new("rsync")
        .args([
            "-avz",
            "--delete",
            "--exclude",
            ".git",
            "--exclude",
            ".venv",
            "--exclude",
            "target",
            "--exclude",
            "control/terraform",
            "--exclude",
            "log",
            "./",
            &format!("{}:{BUILD_DIR}/", instance.public_dns),
        ])
        .status()
        .await?;
    anyhow::ensure!(status.success());
    let status = instance
        .ssh()
        .arg(format!(
            "cd {BUILD_DIR} && /bin/bash -l -c 'cargo build -r'"
        ))
        .status()
        .await?;
    anyhow::ensure!(status.success());
    Ok(())
}
