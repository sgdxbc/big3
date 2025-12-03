use std::{fmt::Write as _, time::Duration};

use big_control::{
    Cluster, Instance,
    configs::{
        APP, CACHE_SIZE, NETWORK, NUM_KEYS, READ_RATIO, STORAGE, STRIPE_INTERVAL, Sharding,
        dump_all,
    },
    load_all, run_endpoints, scrape_all, start_all, stop_all,
};
use big_schema::{Stopped, StoppedReplicaBig, Storage, Task, YcsbWorkloadConfig};
use reqwest::Client;
use tokio::{
    fs::{File, create_dir_all},
    io::AsyncWriteExt,
    time::sleep,
    try_join,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cluster = Cluster::from_terraform().await?;
    let mut run = Run {
        cluster,
        data: String::new(),
    };
    run.init();
    run.perform(
        Sharding {
            num_shards: 1,
            num_shard_faulty_nodes: 33,
        },
        1,
        1.24,
    )
    .await?;

    create_dir_all("data").await?;
    let mut data_file = File::create("data/nodes-network-scratch.csv").await?;
    data_file.write_all(run.data.as_bytes()).await?;
    Ok(())
}

struct Run {
    cluster: Cluster,
    data: String,
}

impl Run {
    fn init(&mut self) {
        self.data = String::from("skewness,num_nodes,consensus,fetch,checkpoint,_notes,_ignore\n");
        writeln!(&mut self.data, ",,,,,,,\"{}\",true", dump_all()).unwrap();
    }

    async fn perform(
        &mut self,
        sharding: Sharding,
        num_concurrent: u32,
        skewness: f64,
    ) -> anyhow::Result<()> {
        assert_eq!(sharding.num_shards, 1);
        assert!(matches!(STORAGE, Storage::Big));
        let run = run(&self.cluster, sharding, num_concurrent, skewness).await?;
        for stopped in run {
            writeln!(
                &mut self.data,
                "{},{},{},{},{},\"num_concurrent = {}\"",
                skewness,
                sharding.num_nodes(),
                stopped.replica_egress,
                stopped.retrieve_egress,
                stopped.checkpoint_egress,
                num_concurrent
            )?;
        }
        Ok(())
    }
}

async fn run(
    cluster: &Cluster,
    sharding: Sharding,
    num_concurrent: u32,
    skewness: f64,
) -> anyhow::Result<Vec<StoppedReplicaBig>> {
    let num_running_nodes = sharding.num_running_nodes();
    let endpoints = run_endpoints(
        [
            &cluster.servers[..num_running_nodes as usize],
            &cluster.clients,
        ]
        .concat(),
    );
    let workload = run_workload(
        &cluster.servers[..num_running_nodes as usize],
        &cluster.clients,
        sharding,
        num_concurrent,
        skewness,
    );
    let workload = async {
        let result = workload.await;
        sleep(Duration::from_millis(1000)).await;
        result
    };
    let ((), run) = try_join!(endpoints, workload)?;
    Ok(run)
}

async fn run_workload(
    server_instances: &[Instance],
    client_instances: &[Instance],
    sharding: Sharding,
    num_concurrent: u32,
    skewness: f64,
) -> anyhow::Result<Vec<StoppedReplicaBig>> {
    assert!(STRIPE_INTERVAL >= Duration::from_hours(1));

    let control_client = Client::new();
    println!("wait for servers to boot");
    sleep(Duration::from_millis(2000)).await;

    let ips = server_instances
        .iter()
        .map(|instance| instance.private_ip)
        .collect::<Vec<_>>();

    let num_shard_running_nodes = 2 * sharding.num_shard_faulty_nodes + 1;
    let num_shards = sharding.num_shards as _;

    println!("load servers");
    let replica_items = server_instances.iter().enumerate().map(|(i, instance)| {
        let shard_index = (i / num_shard_running_nodes as usize) as _;
        let schema = big_schema::ReplicaTask {
            num_shards,
            shard_index,
            shard_node_index: (i % num_shard_running_nodes as usize) as _,
            num_shard_faulty_nodes: sharding.num_shard_faulty_nodes,
            ips: ips.clone(),
            latencies: NETWORK.to_latencies(),
            config: big_schema::ReplicaConfig {
                node_index: i as _,
                num_nodes: sharding.num_nodes(),
                num_faulty_nodes: sharding.num_faulty_nodes(),
            },
            cache_size: CACHE_SIZE,
            max_concurrent_executing: NETWORK.max_concurrent_executing(),
            storage: STORAGE,
            app: APP.to_schema_app(),
            stripe_interval: STRIPE_INTERVAL,
            checkpoint: true,
        };
        (instance, Task::Replica(schema))
    });
    load_all(replica_items, control_client.clone()).await?;

    println!("start servers");
    start_all(server_instances, control_client.clone()).await?;

    println!("load clients");
    let client_items = client_instances.iter().enumerate().map(|(i, instance)| {
        let client_task = big_schema::ClientTask {
            ips: vec![ips.clone()],
            config: big_schema::ClientConfig {
                // num_nodes: num_nodes(),
                num_faulty_nodes: sharding.num_shard_faulty_nodes,
            },
            workload_config: big_schema::ClientWorkloadConfig {
                num_concurrent,
                num_shards: sharding.num_shards,
                app: big_schema::WorkloadConfig::Ycsb(YcsbWorkloadConfig {
                    num_keys: NUM_KEYS,
                    read_ratio: READ_RATIO,
                    theta: skewness,
                }),
            },
            node_index: i as _,
        };
        (instance, Task::Client(client_task))
    });
    load_all(client_items, control_client.clone()).await?;
    println!("start clients");
    start_all(client_instances, control_client.clone()).await?;

    sleep(Duration::from_secs(30)).await;
    println!("scrape measured data");
    scrape_all(client_instances, control_client.clone()).await?;

    println!("stop clients");
    stop_all(client_instances, control_client.clone()).await?;
    println!("stop servers");
    let stopped = stop_all(server_instances, control_client.clone()).await?;
    println!("done");

    let mut stopped_big = Vec::new();
    for stopped in stopped {
        let Stopped::ReplicaBig(stopped) = stopped else {
            anyhow::bail!("expected big replica stopped");
        };
        stopped_big.push(stopped);
    }
    Ok(stopped_big)
}
