use big_schema::TaskMetrics;
use tokio_util::sync::CancellationToken;

use crate::schema;

use self::replica::ReplicaTask;

pub mod replica;

pub enum Task {
    Replica(ReplicaTask),
}

impl Task {
    pub async fn load(schema: schema::Task) -> anyhow::Result<Self> {
        let task = match schema {
            schema::Task::Replica => Self::Replica(ReplicaTask::load().await?),
        };
        Ok(task)
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<TaskMetrics> {
        let metrics = match self {
            Self::Replica(task) => {
                task.run(stop).await?;
                TaskMetrics::Replica
            }
        };
        Ok(metrics)
    }
}
