use big_control::Cluster;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    println!("Build");
    println!(
        "  {:15} {}",
        cluster.build.private_ip, cluster.build.public_dns
    );
    println!("Servers");
    for server in &cluster.servers {
        println!("  {:15} {}", server.private_ip, server.public_dns);
    }
    println!("Clients");
    for client in &cluster.clients {
        println!("  {:15} {}", client.private_ip, client.public_dns);
    }
    Ok(())
}
