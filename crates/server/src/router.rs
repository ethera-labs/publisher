//! HTTP router assembly.

use std::time::Duration;

use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::handlers;
use crate::state::AppState;

pub fn build_router(state: AppState, request_timeout: Duration) -> Router {
    Router::new()
        .route("/health", get(handlers::health::handle_health))
        .route("/ready", get(handlers::health::handle_ready))
        .route("/stats", get(handlers::health::handle_stats))
        .route("/metrics", get(handlers::metrics::handle_metrics))
        .route(
            "/v1/proofs/op-succinct",
            post(handlers::proofs::handle_submit_proof),
        )
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            request_timeout,
        ))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}
