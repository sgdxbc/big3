use std::io::{Write as _, stdout};

use big::{execute::ycsb, storage3::BigStorageConfig, workload::zipfian::ScrambledZipfian};
use lru::LruCache;
use rand::{Rng, SeedableRng as _, rngs::StdRng};
use tokio::task::JoinSet;

#[tokio::main]
async fn main() {
    println!("num_nodes,consensus,fetch,checkpoint");
    let mut join_set = JoinSet::new();
    for f in [1, 3, 13, 23, 33] {
        join_set.spawn(async move { fun_name(f, 0.99) });
        join_set.spawn(async move { fun_name(f, 1.24) });
    }
    while let Some(res) = join_set.join_next().await {
        res.unwrap();
    }
}

fn fun_name(num_faulty_nodes: usize, theta: f64) {
    let num_ops = 1_000_000_000;
    let num_nodes = 3 * num_faulty_nodes + 1;
    let num_keys = 100_000_000;
    let cache_size = 4_000_000;
    let read_ratio = 0.95;

    let config = BigStorageConfig::from(&big_schema::ReplicaConfig {
        num_nodes: num_nodes as _,
        // num_faulty_nodes: num_faulty_nodes as _,
        num_faulty_nodes: 0,
        node_index: 0,
    });
    let proof_size = 32
        * ((num_keys / config.num_shards() as u64) as f64)
            .log2()
            .floor() as u64;

    let mut rng = StdRng::seed_from_u64(117418);
    let zipfian = ScrambledZipfian::new_range_exact(0, num_keys - 1, theta);

    #[derive(Clone, Default)]
    struct Egress {
        consensus: u64,
        fetch: u64,
        checkpoint: u64,
    }
    let mut nodes_egress = vec![Egress::default(); num_nodes];
    let mut lru = LruCache::new(cache_size.try_into().unwrap());
    for i in 0..1_000_000 {
        let key_index = zipfian.scramble(i);
        lru.put(key_index, ());
    }
    for _ in 0..num_ops {
        let is_read = rng.random_bool(read_ratio);
        let key_index = zipfian.next_u64(&mut rng);
        let key_size = 16;
        let op_size = if is_read {
            key_size
        } else {
            key_size + 8 + 100
        };
        let res_size = if is_read { 1000 } else { 0 };

        let proposer = rng.random_range(0..num_nodes);
        nodes_egress[proposer].consensus += op_size * (num_nodes - 1) as u64;

        if lru.get(&key_index).is_none() {
            let pushing_node =
                config.pushing_node_of_shard(config.shard_of_key(ycsb::key(key_index).as_bytes()));
            nodes_egress[pushing_node as usize].fetch +=
                (16 + 1000 + proof_size) * (num_nodes - 1) as u64;
            lru.put(key_index, ());
        }

        for egress in &mut nodes_egress[..] {
            egress.consensus += res_size;
        }
    }

    for shard in 0..config.num_shards() {
        let pushing_node = config.pushing_node_of_shard(shard);
        nodes_egress[pushing_node as usize].checkpoint +=
            (num_keys / config.num_shards() as u64) * (16 + 1000);
    }

    let mut stdout = stdout().lock();
    for egress in nodes_egress {
        writeln!(
            &mut stdout,
            "{num_nodes},{},{},{}",
            egress.consensus, egress.fetch, egress.checkpoint
        )
        .unwrap();
    }
}
