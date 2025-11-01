use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

use crate::schema::{self, Stopped};

use self::{
    big::BigReplicaNodeTask,
    client::{ClientNodeTask, ClientScrapeState},
    full::FullReplicaNodeTask,
    prefill::PrefillTask,
};

pub mod big;
pub mod client;
pub mod consensus;
pub mod execute;
pub mod full;
pub mod network;
pub mod prefill;
pub mod storage;

const PREFILL_PATH: &str = "/tmp/big-prefill";

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
            schema::Task::Full(task) => Self::Full(FullReplicaNodeTask::load(task).await?),
            schema::Task::Big(task) => Self::Big(BigReplicaNodeTask::load(task).await?),
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
            Self::Prefill => anyhow::bail!("prefill has no run method"),
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

pub type RequestId = u64;

pub struct RequestContext<R, P> {
    id: RequestId,
    tx_request: UnboundedSender<(R, ResponseContext<P>)>,
    tx_response: UnboundedSender<(RequestId, P)>,
}

impl<R, P> RequestContext<R, P> {
    pub fn new(
        tx_request: UnboundedSender<(R, ResponseContext<P>)>,
        tx_response: UnboundedSender<(RequestId, P)>,
    ) -> Self {
        Self {
            id: 0,
            tx_request,
            tx_response,
        }
    }

    pub fn request(&mut self, request: R) -> RequestId {
        self.id += 1;
        let ctx = ResponseContext {
            id: self.id,
            tx: self.tx_response.clone(),
        };
        let _ = self.tx_request.send((request, ctx));
        self.id
    }
}

pub struct ResponseContext<T> {
    id: RequestId,
    tx: UnboundedSender<(RequestId, T)>,
}

impl<T> ResponseContext<T> {
    pub fn new(id: RequestId, tx: UnboundedSender<(RequestId, T)>) -> Self {
        Self { id, tx }
    }

    pub fn respond(self, response: T) {
        let _ = self.tx.send((self.id, response));
    }
}
