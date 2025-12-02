use std::time::Duration;

use big_control::{
    Cluster, Instance,
    configs::{APP, CACHE_SIZE, NETWORK, NUM_KEYS, SHARDING, STORAGE, STRIPE_INTERVAL, Sharding},
    load_all, run_endpoints, start_all, wait_all,
};
use big_schema::{Stopped, StoppedReplicaBig, Task};
use reqwest::Client;
use tokio::{time::sleep, try_join};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    assert!(matches!(STORAGE, big_schema::Storage::Big));
    #[allow(clippy::assertions_on_constants)]
    {
        assert!(SHARDING.num_shards == 1);
    }

    let cluster = Cluster::from_terraform().await?;

    // let sharding = SHARDING;
    // for f in [23, 13, 3] {
    let f = 3;
    let sharding = Sharding {
        num_shards: 1,
        num_shard_faulty_nodes: f,
    };

    let num_running_nodes = sharding.num_running_nodes();
    let servers = &cluster.servers[..num_running_nodes as usize];
    let endpoints = run_endpoints(servers.to_vec());
    let workload = run_workload(servers.to_vec(), sharding);
    let workload = async {
        let result = workload.await;
        if result.is_err() {
            sleep(Duration::from_millis(3000)).await;
        }
        result
    };
    let ((), stopped_big_list) = try_join!(endpoints, workload)?;

    println!(
        "num_nodes,num_keys,checkpoint,checkpoint_scan,checkpoint_network,checkpoint_verify,checkpoint_update"
    );
    for stopped in stopped_big_list {
        println!(
            "{},{},{},{},{},{},{}",
            sharding.num_nodes(),
            NUM_KEYS,
            stopped.checkpoint,
            stopped.checkpoint_scan,
            stopped.checkpoint_network,
            stopped.checkpoint_verify,
            stopped.checkpoint_update,
        );
    }
    // }
    Ok(())
}

async fn run_workload(
    server_instances: Vec<Instance>,
    sharding: Sharding,
) -> anyhow::Result<Vec<StoppedReplicaBig>> {
    let control_client = Client::new();
    sleep(Duration::from_millis(2000)).await;

    let ips = server_instances
        .iter()
        .map(|instance| instance.private_ip)
        .collect::<Vec<_>>();

    println!("load servers");
    let replica_items = server_instances.iter().enumerate().map(|(i, instance)| {
        let schema = big_schema::ReplicaTask {
            num_shards: 1,
            shard_index: 0,
            shard_node_index: i as _,
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
    start_all(&server_instances, control_client.clone()).await?;

    println!("wait servers");
    let stopped_list = wait_all(&server_instances, control_client.clone()).await?;
    let mut stopped_big_list = Vec::new();
    for stopped in stopped_list {
        let Stopped::ReplicaBig(stopped) = stopped else {
            anyhow::bail!("unexpected stopped variant");
        };
        stopped_big_list.push(stopped);
    }
    Ok(stopped_big_list)
}
