use big_control::{Cluster, Instance};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    let mut tasks = JoinSet::new();
    for instance in cluster
        .servers
        .into_iter()
        .chain(cluster.clients)
        .chain([cluster.server_prefill_big])
    {
        tasks.spawn(async move { reset_instance(&instance).await });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}

async fn reset_instance(instance: &Instance) -> anyhow::Result<()> {
    if instance.public_dns.is_empty() {
        println!(
            "Skipping reset for instance {} with no public DNS",
            instance.private_ip
        );
        return Ok(());
    }
    instance
        .ssh()
        .arg("pkill big; rm -r /tmp/.tmp*")
        .output()
        .await?;
    Ok(())
}
