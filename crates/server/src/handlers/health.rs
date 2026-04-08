use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use crate::state::AppState;

pub async fn handle_health() -> impl IntoResponse {
    Json(json!({
        "status": "healthy",
        "timestamp": unix_now(),
    }))
}

pub async fn handle_ready(State(state): State<AppState>) -> impl IntoResponse {
    let connections = state.coordinator.server().connection_count().await;
    if connections == 0 {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "no_connections", "connections": 0 })),
        );
    }
    (
        StatusCode::OK,
        Json(json!({ "status": "ready", "connections": connections })),
    )
}

pub async fn handle_stats(State(state): State<AppState>) -> impl IntoResponse {
    let stats = state.coordinator.stats().await;
    Json(stats)
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
