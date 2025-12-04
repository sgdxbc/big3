use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse, routing::post};
use big::{node::Task, schema};
use log::info;
use rustix::process::{Resource, getrlimit, setrlimit};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    try_join,
};
use tokio_util::sync::CancellationToken;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::builder()
        .target(env_logger::Target::Stdout)
        .try_init()?;
    info!("logger initialized");
    let mut rlimit = getrlimit(Resource::Nofile);
    rlimit.current = rlimit.maximum;
    setrlimit(Resource::Nofile, rlimit)?;

    let (tx_command, rx_command) = channel(1);
    let shutdown = CancellationToken::new();
    let run_task = run(rx_command, shutdown.clone());

    let state = AppState { tx_command };
    let router = axum::Router::new()
        .route("/load", post(load))
        .route("/start", post(start))
        .route("/scrape", post(scrape))
        .route("/stop", post(stop))
        .route("/wait", post(wait))
        .with_state(Arc::new(state));
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
    Scrape(oneshot::Sender<schema::Scrape>),
    Stop(oneshot::Sender<schema::Stopped>),
    Wait(oneshot::Sender<()>),
}

async fn run(mut rx_command: Receiver<Command>, shutdown: CancellationToken) -> anyhow::Result<()> {
    let Some(Command::Load(task, tx)) = rx_command.recv().await else {
        anyhow::bail!("first command must be load");
    };
    let task = Task::load(task).await?;
    let _ = tx.send(());

    match rx_command.recv().await {
        Some(Command::Stop(tx)) => {
            shutdown.cancel();
            let stopped = task.run(shutdown.clone(), CancellationToken::new()).await?;
            let _ = tx.send(stopped);
            return Ok(());
        }
        Some(Command::Start) => {}
        _ => anyhow::bail!("second command must be start"),
    }
    let scrape_state = task.scrape_state();
    let wait = CancellationToken::new();
    let watch = async {
        loop {
            match rx_command.recv().await {
                Some(Command::Scrape(tx_scrape)) => {
                    let _ = tx_scrape.send(scrape_state.scrape()?);
                }
                Some(Command::Stop(tx_stopped)) => {
                    shutdown.cancel();
                    break Ok(tx_stopped);
                }
                Some(Command::Wait(tx_stopped)) => {
                    wait.cancelled().await;
                    let _ = tx_stopped.send(());
                }
                _ => anyhow::bail!("unexpected command"),
            }
        }
    };
    let (tx_stopped, stopped) = try_join!(watch, task.run(shutdown.clone(), wait.clone()))?;
    let _ = tx_stopped.send(stopped);
    Ok(())
}

struct AppState {
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

async fn scrape(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let (tx, rx) = oneshot::channel();
    state.tx_command.send(Command::Scrape(tx)).await.unwrap();
    Json(rx.await.unwrap())
}

async fn stop(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let (tx, rx) = oneshot::channel();
    state.tx_command.send(Command::Stop(tx)).await.unwrap();
    Json(rx.await.unwrap())
}

async fn wait(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let (tx, rx) = oneshot::channel();
    state.tx_command.send(Command::Wait(tx)).await.unwrap();
    rx.await.unwrap();
    ()
}
