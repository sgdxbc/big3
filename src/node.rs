use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use crate::prefill::PrefillTask;
use crate::schema::{self, Stopped};
use crate::workload::ClientScrapeState;

use self::{big3::BigReplicaNodeTask, client::ClientNodeTask, full::FullReplicaNodeTask};

#[allow(dead_code)]
mod big;
// mod big2;
mod big3;
mod client;
mod full;

#[allow(clippy::large_enum_variant)]
pub enum Task {
    Full(FullReplicaNodeTask),
    Big(BigReplicaNodeTask),
    Client(ClientNodeTask),
    Prefill,
}

pub enum ScrapeState {
    Replica,
    Client(Arc<Mutex<ClientScrapeState>>),
}

impl Task {
    pub async fn load(schema: schema::Task) -> anyhow::Result<Self> {
        let task = match schema {
            schema::Task::Replica(task) => match task.storage {
                schema::Storage::Full => Self::Full(FullReplicaNodeTask::load(task).await?),
                schema::Storage::Big => Self::Big(BigReplicaNodeTask::load(task).await?),
            },
            schema::Task::Client(task) => Self::Client(ClientNodeTask::load(task).await?),
            schema::Task::Prefill(task) => {
                PrefillTask::load(task).await?;
                Self::Prefill
            }
        };
        Ok(task)
    }

    pub fn scrape_state(&self) -> ScrapeState {
        match self {
            Self::Full(_) => ScrapeState::Replica,
            Self::Big(_) => ScrapeState::Replica,
            Self::Client(task) => ScrapeState::Client(task.scrape_state()),
            Self::Prefill => panic!("prefill has no scrape state"),
        }
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<Stopped> {
        let stopped = match self {
            Self::Full(task) => {
                task.run(stop).await?;
                Stopped::Replica
            }
            Self::Big(task) => {
                task.run(stop).await?;
                Stopped::Replica
            }
            Self::Client(task) => {
                task.run(stop).await?;
                Stopped::Client
            }
            Self::Prefill => {
                stop.cancelled().await;
                Stopped::Replica
            }
        };
        Ok(stopped)
    }
}

impl ScrapeState {
    pub fn scrape(&self) -> anyhow::Result<schema::Scrape> {
        let scrape = match self {
            Self::Replica => anyhow::bail!("replica has no scrape state"),
            Self::Client(state) => state.lock().unwrap().scrape(),
        };
        Ok(scrape)
    }
}
