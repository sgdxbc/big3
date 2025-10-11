use big_control::Cluster;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    println!("{cluster:?}");
    Ok(())
}
