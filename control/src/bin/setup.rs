use std::time::Duration;

use big_control::{Cluster, Instance};
use tokio::{task::JoinSet, time::sleep};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    let mut tasks = JoinSet::new();
    tasks.spawn(async move { setup_build(&cluster.build).await });
    for instance in cluster.servers.clone() {
        tasks.spawn(async move { setup_storage(&instance).await });
    }
    for instance in cluster.servers.into_iter().chain(cluster.clients) {
        tasks.spawn(async move { setup_common(&instance).await });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}

async fn setup_build(instance: &Instance) -> anyhow::Result<()> {
    let output = instance.ssh().arg([
        "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --profile minimal -y",
        "sudo apt-get -q update",
        "sudo apt-get -q install -y clang make pkg-config liburing-dev"
    ].join(" && ")).output().await?;
    if !output.status.success() {
        anyhow::bail!(
            "{} build setup failed: {}",
            instance.public_dns,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

async fn setup_storage(instance: &Instance) -> anyhow::Result<()> {
    let status = instance.ssh().arg("mount | grep /tmp").status().await?;
    if !status.success() {
        loop {
            let output = instance
                .ssh()
                .arg(
                    [
                        "sudo mkfs.xfs -f /dev/nvme1n1",
                        "sudo mount -o discard /dev/nvme1n1 /tmp",
                        "sudo chmod 777 /tmp",
                    ]
                    .join(" && "),
                )
                .output()
                .await?;
            if output.status.success() {
                break;
            }
            if String::from_utf8_lossy(&output.stderr).contains("Device or resource busy") {
                println!("{} rebooting to clear busy device", instance.public_dns);
                let status = instance.ssh().arg("sudo reboot").status().await?;
                anyhow::ensure!(status.success(), "{} reboot failed", instance.public_dns);
                sleep(Duration::from_secs(30)).await;
                continue;
            }
            anyhow::bail!(
                "{} storage setup failed: {}",
                instance.public_dns,
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
    Ok(())
}

async fn setup_common(instance: &Instance) -> anyhow::Result<()> {
    let output = instance
        .ssh()
        .arg(
            [
                "sudo sysctl -w net.core.rmem_max=7500000",
                "sudo sysctl -w net.core.wmem_max=7500000",
            ]
            .join(" && "),
        )
        .output()
        .await?;
    if !output.status.success() {
        anyhow::bail!(
            "{} common setup failed: {}",
            instance.public_dns,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}
