use std::sync::Arc;

use axum::{extract::State, routing::post};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let shutdown = CancellationToken::new();
    let router = axum::Router::new()
        .route("/load", post(load))
        .route("/start", post(start))
        .route("/stop", post(stop));
    let state = AppState {
        shutdown: shutdown.clone(),
    };
    let router = router.with_state(Arc::new(state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await?;
    Ok(())
}

struct AppState {
    shutdown: CancellationToken,
}

async fn load() {}

async fn start() {}

async fn stop(State(state): State<Arc<AppState>>) {
    //
    state.shutdown.cancel();
}
