use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use big_schema::NodeIndex;
use log::info;
use rocksdb::DB;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    time::sleep,
};
use tokio_util::sync::CancellationToken;

use crate::{
    metrics::Latency,
    network::interconnect::{NetworkInterconnectHandle, ReceiveHandle},
    storage::BigStorageConfig,
};

pub use message::Message;

pub struct ArchiveChannels {
    tx_message: UnboundedSender<Message>,
    rx_message: UnboundedReceiver<Message>,
}

impl ArchiveChannels {
    pub fn receive_handle(&self) -> ReceiveHandle<Message> {
        ReceiveHandle::new(self.tx_message.clone())
    }
}

pub struct ArchiveConfig {
    stripe_interval: Duration,
}

pub struct ArchiveTask {
    channels: ArchiveChannels,
    network_interconnect: NetworkInterconnectHandle,

    config: ArchiveConfig,
    storage_config: BigStorageConfig,
    shards: HashSet<u32>,
    db: Arc<DB>,
    node_index: NodeIndex,

    current_round: u64,
    node_rounds: HashMap<NodeIndex, u64>,
    // round -> stripe -> shard -> data
    reorder_push_shards: HashMap<u64, HashMap<u32, HashMap<u32, Vec<(Vec<u8>, Vec<u8>)>>>>,

    metrics: ArchiveMetrics,
}

struct ArchiveMetrics {
    round_start: Instant,
    round: Latency,
}

impl ArchiveTask {
    pub fn new(
        channels: ArchiveChannels,
        network_interconnect: NetworkInterconnectHandle,
        config: ArchiveConfig,
        storage_config: BigStorageConfig,
        db: Arc<DB>,
        node_index: NodeIndex,
    ) -> Self {
        let shards = (0..storage_config.num_shards())
            .filter(|shard| {
                storage_config.primary_node_of_shard(*shard) == node_index
                    || storage_config
                        .secondary_nodes_of_shard(*shard)
                        .any(|n| n == node_index)
            })
            .collect();
        Self {
            channels,
            network_interconnect,
            config,
            storage_config,
            shards,
            db,
            node_index,
            current_round: 0,
            node_rounds: HashMap::new(),
            reorder_push_shards: HashMap::new(),
            metrics: ArchiveMetrics {
                round_start: Instant::now(),
                round: Latency::new(),
            },
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        cancel
            .run_until_cancelled(self.run_inner())
            .await
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            info!("Voting to start archive for round {}", self.current_round);
            let vote = message::Vote {
                round: self.current_round,
                node_index: self.node_index,
            };
            // TODO send vote through consensus
            self.network_interconnect.send_to_all(Message::Vote(vote));
            self.node_rounds.insert(self.node_index, self.current_round);
            let mut round = self.current_round;
            while self.node_rounds.values().filter(|&&r| r == round).count()
                < (self.storage_config.num_nodes - self.storage_config.num_faulty_nodes) as usize
            {
                let Some(message) = self.channels.rx_message.recv().await else {
                    return Ok(());
                };
                match message {
                    Message::Vote(vote) => {
                        round = vote.round;
                        let node_round = self.node_rounds.entry(vote.node_index).or_insert(round);
                        if *node_round < round {
                            *node_round = round;
                        }
                    }
                    Message::PushShard(push_shard) => {
                        if push_shard.round >= self.current_round {
                            self.reorder_push_shards
                                .entry(push_shard.round)
                                .or_default()
                                .entry(self.storage_config.stripe_of_shard(push_shard.shard))
                                .or_default()
                                .insert(push_shard.shard, push_shard.data);
                        }
                    }
                }
            }
            assert_eq!(round, self.current_round); // otherwise unimplemented yet
            self.current_round = round;
            info!("Starting archive for round {}", self.current_round);
            self.metrics.round_start = Instant::now();

            let mut iter = self.db.raw_iterator();
            for stripe in 0..self.storage_config.num_stripes {
                sleep(self.config.stripe_interval).await;
                let mut stripe_shards = HashMap::new();
                for &shard in &self.shards {
                    if !self.storage_config.stripe_of_shard(shard) == stripe {
                        continue;
                    }

                    let prefix = [b"shard.", &shard.to_be_bytes()[..]].concat();
                    iter.seek(&prefix);
                    iter.status()?;
                    let mut data = Vec::new();
                    while let Some((key, value)) = iter.item() {
                        if !key.starts_with(&prefix) {
                            break;
                        }
                        data.push((key.to_vec(), value.to_vec()));
                        iter.next();
                        iter.status()?;
                    }

                    stripe_shards.insert(shard, data.clone());
                    let push_shard = message::PushShard {
                        round: self.current_round,
                        shard,
                        data,
                    };
                    self.network_interconnect
                        .send_to_all(Message::PushShard(push_shard));
                }

                if let Some(reorder_stripe) = self
                    .reorder_push_shards
                    .get_mut(&self.current_round)
                    .and_then(|stripes| stripes.remove(&stripe))
                {
                    stripe_shards.extend(reorder_stripe);
                }

                loop {
                    let Some(message) = self.channels.rx_message.recv().await else {
                        return Ok(());
                    };
                    match message {
                        Message::PushShard(push_shard) => {
                            assert!(push_shard.round >= self.current_round);
                            if push_shard.round > self.current_round
                                || self.storage_config.stripe_of_shard(push_shard.shard) > stripe
                            {
                                self.reorder_push_shards
                                    .entry(push_shard.round)
                                    .or_default()
                                    .entry(self.storage_config.stripe_of_shard(push_shard.shard))
                                    .or_default()
                                    .insert(push_shard.shard, push_shard.data);
                                continue;
                            }
                            if self.storage_config.stripe_of_shard(push_shard.shard) == stripe {
                                stripe_shards.insert(push_shard.shard, push_shard.data);
                                assert!(
                                    stripe_shards.len()
                                        <= self.storage_config.num_shards_per_stripe() as usize
                                );
                                if stripe_shards.len()
                                    == self.storage_config.num_shards_per_stripe() as usize
                                {
                                    break;
                                }
                            }
                        }
                        Message::Vote(start_round) => {
                            let round = start_round.round;
                            let node_round = self
                                .node_rounds
                                .entry(start_round.node_index)
                                .or_insert(round);
                            if *node_round < round {
                                *node_round = round;
                            }
                        }
                    }
                }

                let mut stripe_data = stripe_shards
                    .into_iter()
                    .flat_map(|(_, data)| data)
                    .collect::<Vec<_>>();
                stripe_data.sort_unstable_by(|(k1, _), (k2, _)| k1.cmp(k2));
                let mut stripe_data =
                    bincode::encode_to_vec(stripe_data, bincode::config::standard())?;
                let k = (self.storage_config.num_faulty_nodes + 1) as usize;
                let stripe_data_len =
                    stripe_data
                        .len()
                        .next_multiple_of(if k.is_multiple_of(2) { k } else { k * 2 });
                info!(
                    "Archiving round {}, stripe {}, data length {} -> {}",
                    self.current_round,
                    stripe,
                    stripe_data.len(),
                    stripe_data_len
                );
                stripe_data.resize(stripe_data_len, 0);
                let encoded_chunks = reed_solomon_simd::encode(
                    k,
                    (self.storage_config.num_faulty_nodes * 2) as _,
                    stripe_data.chunks_exact(stripe_data_len / k),
                )?;
                // TODO do not directly overwrite last round
                self.db.put(
                    format!("archive.{stripe}.{}", self.node_index),
                    &encoded_chunks[self.node_index as usize],
                )?;
                info!(
                    "Archived round {}, stripe {}, data size {}",
                    self.current_round,
                    stripe,
                    encoded_chunks[self.node_index as usize].len()
                );
            }

            self.metrics.round += self.metrics.round_start.elapsed();
            self.current_round += 1;
        }
    }
}

mod message {
    use big_schema::NodeIndex;
    use bincode::{Decode, Encode};

    #[derive(Decode, Encode)]
    pub enum Message {
        PushShard(PushShard),
        Vote(Vote),
    }

    #[derive(Decode, Encode)]
    pub struct PushShard {
        pub round: u64,
        pub shard: u32,
        pub data: Vec<(Vec<u8>, Vec<u8>)>,
    }

    #[derive(Decode, Encode)]
    pub struct Vote {
        pub round: u64,
        pub node_index: NodeIndex,
    }
}
