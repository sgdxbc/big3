use big_control::{Cluster, Instance};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    let mut tasks = JoinSet::new();
    tasks.spawn(async move { setup_build(&cluster.build).await });
    for server in cluster.servers {
        tasks.spawn(async move { setup_storage(&server).await });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}

async fn setup_build(instance: &Instance) -> anyhow::Result<()> {
    let status = instance.ssh().arg([
        "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --profile minimal -y",
        "sudo apt-get update",
        "sudo apt-get install -y clang make"
    ].join(" && ")).status().await?;
    anyhow::ensure!(status.success());
    Ok(())
}

async fn setup_storage(instance: &Instance) -> anyhow::Result<()> {
    let status = instance.ssh().arg("mount | grep /tmp").status().await?;
    if !status.success() {
        let status = instance
            .ssh()
            .arg(
                [
                    "sudo mkfs.xfs -f /dev/nvme1n1",
                    "sudo mount -o discard /dev/nvme1n1 /tmp",
                    "sudo chmod 777 /tmp",
                ]
                .join(" && "),
            )
            .status()
            .await?;
        anyhow::ensure!(status.success());
    }
    Ok(())
}
