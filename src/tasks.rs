use tokio_util::sync::CancellationToken;

use crate::schema::{self, Stopped};

use self::{client::ClientNodeTask, replica::ReplicaNodeTask};

pub mod client;
pub mod replica;

pub enum Task {
    Replica(ReplicaNodeTask),
    Client(ClientNodeTask),
}

impl Task {
    pub async fn load(schema: schema::Task) -> anyhow::Result<Self> {
        let task = match schema {
            schema::Task::Replica => Self::Replica(ReplicaNodeTask::load().await?),
            schema::Task::Client(task) => Self::Client(ClientNodeTask::load(task).await?),
        };
        Ok(task)
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<Stopped> {
        let metrics = match self {
            Self::Replica(task) => {
                task.run(stop).await?;
                Stopped::Replica
            }
            Self::Client(task) => {
                task.run(stop).await?;
                Stopped::Client
            }
        };
        Ok(metrics)
    }
}
