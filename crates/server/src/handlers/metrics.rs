//! Prometheus metrics endpoint.

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use prometheus_client::encoding::text::encode;
use tracing::error;

use crate::state::AppState;

pub async fn handle_metrics(State(state): State<AppState>) -> impl IntoResponse {
    let Some(registry) = &state.registry else {
        return (StatusCode::NOT_FOUND, "metrics not enabled".to_string()).into_response();
    };

    let registry = registry.lock().await;
    let mut body = String::new();
    if let Err(e) = encode(&mut body, &registry) {
        error!(error = %e, "Failed to encode metrics");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encode metrics".to_string(),
        )
            .into_response();
    }

    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        body,
    )
        .into_response()
}
