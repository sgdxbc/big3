mod common;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = common::Cluster::from_terraform().await?;
    println!("{cluster:?}");
    Ok(())
}
