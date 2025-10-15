use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use hdrhistogram::serialization::{Serializer as _, V2Serializer};
use tokio_util::sync::CancellationToken;

use crate::{
    client::Records,
    schema::{self, Stopped},
};

use self::{client::ClientNodeTask, prefill::PrefillTask, replica::ReplicaNodeTask};

pub mod client;
pub mod prefill;
pub mod replica;

const PREFILL_PATH: &str = "/tmp/big-prefill";

pub enum Task {
    Replica(ReplicaNodeTask),
    Client(ClientNodeTask),
    Prefill,
}

pub enum ScrapeState {
    Replica,
    Client(Arc<Mutex<Records>>),
}

impl Task {
    pub async fn load(schema: schema::Task) -> anyhow::Result<Self> {
        let task = match schema {
            schema::Task::Replica => Self::Replica(ReplicaNodeTask::load().await?),
            schema::Task::Client(task) => Self::Client(ClientNodeTask::load(task).await?),
            schema::Task::Prefill(task) => {
                PrefillTask::load(&task).await?;
                Self::Prefill
            }
        };
        Ok(task)
    }

    pub fn scrape_state(&self) -> ScrapeState {
        match self {
            Self::Replica(_) => ScrapeState::Replica,
            Self::Client(task) => ScrapeState::Client(task.scrape_state()),
            Self::Prefill => panic!("prefill has no scrape state"),
        }
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<Stopped> {
        let stopped = match self {
            Self::Replica(task) => {
                task.run(stop).await?;
                Stopped::Replica
            }
            Self::Client(task) => {
                task.run(stop).await?;
                Stopped::Client
            }
            Self::Prefill => anyhow::bail!("prefill has no run method"),
        };
        Ok(stopped)
    }
}

impl ScrapeState {
    pub fn scrape(&self) -> anyhow::Result<schema::Scrape> {
        let scrape = match self {
            Self::Replica => anyhow::bail!("replica has no scrape state"),
            Self::Client(latency_histogram) => {
                let mut records = latency_histogram.lock().unwrap();
                let interval = records.start.elapsed();
                records.start = Instant::now();

                let mut buf = Vec::new();
                V2Serializer::new().serialize(&records.latency_histogram, &mut buf)?;
                records.latency_histogram.reset();

                schema::Scrape {
                    interval,
                    latency_histogram: buf,
                }
            }
        };
        Ok(scrape)
    }
}
