use big_control::{Cluster, Instance};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    let mut tasks = JoinSet::new();
    for instance in cluster.servers.into_iter().chain(cluster.clients) {
        tasks.spawn(async move { reset_instance(&instance).await });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}

async fn reset_instance(instance: &Instance) -> anyhow::Result<()> {
    instance.ssh().arg("pkill big").output().await?;
    Ok(())
}
