//! HTTP router assembly.

use axum::routing::get;
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::handlers;
use crate::state::AppState;

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(handlers::health::handle_health))
        .route("/ready", get(handlers::health::handle_ready))
        .route("/stats", get(handlers::health::handle_stats))
        .route("/metrics", get(handlers::metrics::handle_metrics))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}
