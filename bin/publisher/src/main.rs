//! Ethera Shared Publisher.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use compose_spec::{PeriodId, SuperblockNumber};
use prometheus_client::registry::Registry;
use tokio::net::TcpListener;
use tracing::{error, info};

use publisher_config::{Cli, Config};
use publisher_coordinator::coordinator::Coordinator;
use publisher_coordinator::handlers;
use publisher_metrics::PublisherMetrics;
use publisher_server::router::build_router;
use publisher_server::state::AppState;
use publisher_transport::server::QuicServer;

const PERIOD_DURATION: Duration = Duration::from_secs(12);

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = Config::load(&cli.config)?;

    publisher_tracing::init(&cfg.log.level, cfg.log.pretty);
    info!("Starting Ethera Shared Publisher");

    let mut registry = Registry::default();
    let metrics = if cfg.metrics.enabled {
        Some(Arc::new(PublisherMetrics::new(&mut registry)))
    } else {
        None
    };

    let server = Arc::new(QuicServer::new(
        cfg.server.listen_addr.clone(),
        cfg.server.max_message_size,
    ));
    let coordinator = Arc::new(Coordinator::new(
        server.clone(),
        metrics.clone(),
        cfg.consensus.timeout,
    ));

    let coord_for_handler = coordinator.clone();
    let on_message = Arc::new(move |client_id: String, data: Vec<u8>| {
        let coord = coord_for_handler.clone();
        tokio::spawn(async move {
            handlers::dispatch(coord, client_id, data).await;
        });
    });

    let metrics_connect = metrics.clone();
    let on_connect = Arc::new(move |_client_id: String| {
        if let Some(m) = &metrics_connect {
            m.connections_active.inc();
        }
    });

    let metrics_disconnect = metrics.clone();
    let on_disconnect = Arc::new(move |_client_id: String| {
        if let Some(m) = &metrics_disconnect {
            m.connections_active.dec();
        }
    });

    let _quic_handle = server.start(on_message, Some(on_connect), Some(on_disconnect))?;

    let coord_for_period = coordinator.clone();
    tokio::spawn(async move { period_loop(coord_for_period).await });

    let coord_for_timeout = coordinator.clone();
    tokio::spawn(async move { timeout_loop(coord_for_timeout).await });

    let state = if cfg.metrics.enabled {
        AppState::new(coordinator.clone()).with_registry(registry)
    } else {
        AppState::new(coordinator.clone())
    };
    let router = build_router(state, cfg.api.request_timeout);
    let listener = TcpListener::bind(&cfg.api.listen_addr).await?;
    info!(addr = %cfg.api.listen_addr, "HTTP API listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Shutting down");
    coordinator.server().close();
    Ok(())
}

async fn period_loop(coordinator: Arc<Coordinator>) {
    let mut interval = tokio::time::interval(PERIOD_DURATION);
    let period_counter = AtomicU64::new(0);
    let superblock_counter = AtomicU64::new(0);

    loop {
        interval.tick().await;
        let pid = period_counter.fetch_add(1, Ordering::Relaxed) + 1;
        let sb = superblock_counter.fetch_add(1, Ordering::Relaxed) + 1;

        if let Err(e) = coordinator
            .start_period(PeriodId::new(pid), SuperblockNumber::new(sb))
            .await
        {
            error!(period_id = pid, error = %e, "Failed to broadcast period");
        }
    }
}

async fn timeout_loop(coordinator: Arc<Coordinator>) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        coordinator.cleanup_expired_xts().await;
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
    info!("Received shutdown signal");
}
