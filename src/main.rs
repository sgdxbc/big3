use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse, routing::post};
use big::{schema, tasks::Task};
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    try_join,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx_command, rx_command) = channel(1);
    let run_task = run(rx_command);

    let router = axum::Router::new()
        .route("/load", post(load))
        .route("/start", post(start))
        .route("/stop", post(stop));
    let shutdown = CancellationToken::new();
    let state = AppState {
        shutdown: shutdown.clone(),
        tx_command,
    };
    let router = router.with_state(Arc::new(state));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    let serve = async {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await?;
        Ok(())
    };

    try_join!(run_task, serve)?;
    Ok(())
}

enum Command {
    Load(schema::Task, oneshot::Sender<()>),
    Start,
    Stop(oneshot::Sender<schema::Stopped>),
}

async fn run(mut rx_command: Receiver<Command>) -> anyhow::Result<()> {
    let Some(Command::Load(task, tx)) = rx_command.recv().await else {
        anyhow::bail!("first command must be load");
    };
    let task = Task::load(task).await?;
    let _ = tx.send(());

    let Some(Command::Start) = rx_command.recv().await else {
        anyhow::bail!("second command must be start");
    };
    let stop = CancellationToken::new();
    let mut task_handle = tokio::spawn(task.run(stop.clone()));

    let tx_metrics = select! {
        Some(Command::Stop(tx)) = rx_command.recv() => tx,
        result = &mut task_handle => {
            result??;
            unimplemented!()
        }
    };
    stop.cancel();
    let _ = tx_metrics.send(task_handle.await??);
    Ok(())
}

struct AppState {
    shutdown: CancellationToken,
    tx_command: Sender<Command>,
}

async fn load(State(state): State<Arc<AppState>>, Json(task): Json<schema::Task>) {
    let (tx, rx) = oneshot::channel();
    state
        .tx_command
        .send(Command::Load(task, tx))
        .await
        .unwrap();
    rx.await.unwrap();
}

async fn start(State(state): State<Arc<AppState>>) {
    state.tx_command.send(Command::Start).await.unwrap();
}

async fn stop(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state.shutdown.cancel();
    let (tx, rx) = oneshot::channel();
    state.tx_command.send(Command::Stop(tx)).await.unwrap();
    Json(rx.await.unwrap())
}
