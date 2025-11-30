use std::fmt::Write as _;

use big_control::{
    Cluster, Instance,
    configs::{APP, NETWORK, SHARDING, STORAGE, dump_all},
};
use big_schema::Storage;
use tokio::{
    fs::{File, create_dir_all},
    io::AsyncWriteExt,
    task::JoinSet,
};

#[derive(Debug, Clone, Copy)]
enum Setting {
    Full,
    Sharded,
    Big,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;

    let mut data = String::from("network,app,setting,num_nodes,storage,_notes,_ignore\n");
    writeln!(&mut data, ",,,,,\"{}\",true", dump_all())?;

    let setting = match STORAGE {
        Storage::Full if SHARDING.num_shards == 1 => Setting::Full,
        Storage::Full if SHARDING.num_shards > 1 => Setting::Sharded,
        Storage::Big if SHARDING.num_shards == 1 => Setting::Big,
        _ => panic!("invalid setting"),
    };
    for size in run(cluster).await? {
        writeln!(
            &mut data,
            "{:?},{:?},{:?},{},{},",
            NETWORK,
            APP,
            setting,
            SHARDING.num_nodes(),
            size
        )?;
    }

    create_dir_all("data").await?;
    let mut data_file = File::create("data/nodes-storage-scratch.csv").await?;
    data_file.write_all(data.as_bytes()).await?;
    Ok(())
}

async fn run(cluster: Cluster) -> anyhow::Result<Vec<u64>> {
    let num_running_nodes = SHARDING.num_running_nodes();

    let mut tasks = JoinSet::new();
    for instance in cluster.servers.into_iter().take(num_running_nodes as _) {
        tasks.spawn(async move { run_instance(&instance).await });
    }
    let mut run = Vec::new();
    while let Some(result) = tasks.join_next().await {
        run.push(result??);
    }

    Ok(run)
}

async fn run_instance(instance: &Instance) -> anyhow::Result<u64> {
    let output = instance
        .ssh()
        .arg("du -s /tmp/big-prefill")
        .output()
        .await?;
    anyhow::ensure!(output.status.success());
    let size = str::from_utf8(&output.stdout)?
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow::anyhow!("Failed to get size from output"))?
        .parse()?;
    Ok(size)
}
