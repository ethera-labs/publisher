//! Ethera Shared Publisher.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Parser;
use prometheus_client::registry::Registry;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use publisher_config::Cli;
use publisher_coordinator::coordinator::Coordinator;
use publisher_coordinator::handlers::{self, parse_chain_id};
use publisher_coordinator::l1_submit::L1Submitter;
use publisher_metrics::PublisherMetrics;
use publisher_server::router::build_router;
use publisher_server::state::AppState;
use publisher_transport::server::QuicServer;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls crypto provider"))?;

    let cli = Cli::parse();
    let cfg = cli.load_config()?;

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

    let s = &cfg.settlement;
    let l1_submitter = if !s.l1_rpc_url.is_empty()
        && !s.dispute_game_factory.is_empty()
        && !s.proposer_key.is_empty()
    {
        match L1Submitter::new(
            &s.dispute_game_factory,
            &s.anchor_state_registry,
            s.l1_rpc_url.clone(),
            s.proposer_key.clone(),
        ) {
            Ok(sub) => {
                info!(
                    proving_mode = ?cfg.proofs.proving_mode,
                    factory = %s.dispute_game_factory,
                    "L1 submitter configured"
                );
                Some(sub)
            }
            Err(e) => {
                error!(error = %e, "Failed to create L1 submitter - running without L1 settlement");
                None
            }
        }
    } else {
        info!("No settlement config - running without L1 settlement");
        None
    };

    let coordinator_builder = Coordinator::new(
        server.clone(),
        metrics.clone(),
        cfg.consensus.timeout,
        cfg.consensus.proof_window,
    );
    let coordinator_builder = if cfg.proofs.proving_mode.is_mock() {
        warn!("Proof generation set to mock mode");
        coordinator_builder.with_mock_proofs()
    } else {
        coordinator_builder
    };
    let coordinator = Arc::new(if let Some(sub) = l1_submitter {
        coordinator_builder.with_l1_submitter(sub)
    } else {
        coordinator_builder
    });

    coordinator.init_from_l1().await?;

    let period_schedule = PeriodSchedule::new(
        cfg.consensus
            .genesis_unix_seconds
            .context("consensus.genesis_unix_seconds is required")?,
        cfg.consensus.period_duration,
    )?;

    let coord_for_handler = coordinator.clone();
    let on_message = Arc::new(move |client_id: String, data: Vec<u8>| {
        let coord = coord_for_handler.clone();
        tokio::spawn(async move {
            handlers::dispatch(coord, client_id, data).await;
        });
    });

    let coord_for_connect = coordinator.clone();
    let metrics_connect = metrics.clone();
    let on_connect = Arc::new(move |client_id: String| {
        if let Some(m) = &metrics_connect {
            m.connections_active.inc();
        }

        let coord = coord_for_connect.clone();
        tokio::spawn(async move {
            match parse_chain_id(&client_id) {
                Ok(chain_id) => coord.register_chain(&client_id, chain_id).await,
                Err(e) => {
                    warn!(client_id, error = %e, "Ignoring connection with unparseable chain ID");
                }
            }
        });
    });

    let metrics_disconnect = metrics.clone();
    let on_disconnect = Arc::new(move |_client_id: String| {
        if let Some(m) = &metrics_disconnect {
            m.connections_active.dec();
        }
    });

    let _quic_handle = server.start(on_message, Some(on_connect), Some(on_disconnect))?;

    let recovery_period = period_schedule.period_at(SystemTime::now())?;
    coordinator
        .wait_for_chains(&cfg.proofs.required_chain_ids, cfg.consensus.timeout)
        .await?;
    coordinator
        .broadcast_recovery_rollback(recovery_period)
        .await?;
    coordinator.start_period(recovery_period).await?;
    coordinator.activate_protocol();

    let coord_for_period = coordinator.clone();
    tokio::spawn(async move { period_loop(coord_for_period, period_schedule).await });

    let coord_for_reaper = coordinator.clone();
    tokio::spawn(async move { reaper_loop(coord_for_reaper).await });

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

async fn period_loop(coordinator: Arc<Coordinator>, schedule: PeriodSchedule) {
    loop {
        let now = SystemTime::now();
        let period_id = match schedule.period_at(now) {
            Ok(period_id) => period_id,
            Err(e) => {
                error!(error = %e, "Failed to derive protocol period");
                return;
            }
        };
        if let Err(e) = coordinator.start_period(period_id).await {
            error!(error = %e, "Failed to broadcast period");
        }
        tokio::time::sleep(schedule.until_next_period(now)).await;
    }
}

#[derive(Debug, Clone, Copy)]
struct PeriodSchedule {
    genesis_unix_seconds: u64,
    period_duration_seconds: u64,
}

impl PeriodSchedule {
    fn new(genesis_unix_seconds: u64, period_duration: Duration) -> Result<Self> {
        let period_duration_seconds = period_duration.as_secs();
        anyhow::ensure!(
            period_duration_seconds > 0,
            "period duration must be at least one second"
        );
        Ok(Self {
            genesis_unix_seconds,
            period_duration_seconds,
        })
    }

    fn period_at(self, now: SystemTime) -> Result<ethera_spec::PeriodId> {
        let now = now
            .duration_since(UNIX_EPOCH)
            .context("system clock is before Unix epoch")?
            .as_secs();
        anyhow::ensure!(
            now >= self.genesis_unix_seconds,
            "system clock is before protocol genesis"
        );
        let elapsed = now - self.genesis_unix_seconds;
        // FIXME(spec): SBCP's PeriodStart(k) formula implies period 0 starts at genesis.
        // Keep the deployed one-based numbering until period indexing is resolved explicitly.
        Ok(ethera_spec::PeriodId(
            elapsed / self.period_duration_seconds + 1,
        ))
    }

    fn until_next_period(self, now: SystemTime) -> Duration {
        let now = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let elapsed = now.saturating_sub(self.genesis_unix_seconds);
        let remainder = elapsed % self.period_duration_seconds;
        Duration::from_secs(self.period_duration_seconds - remainder)
    }
}

/// Runs periodic cleanup: times out stale consensus instances and triggers rollback
/// for proof sets that exceed the proof window.
async fn reaper_loop(coordinator: Arc<Coordinator>) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        interval.tick().await;
        coordinator.reap_timed_out_xts().await;
        coordinator.reap_expired_proofs().await;
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
    info!("Received shutdown signal");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn period_schedule_is_derived_from_genesis() {
        let schedule = PeriodSchedule::new(1_000, Duration::from_secs(100)).unwrap();

        assert_eq!(
            schedule
                .period_at(UNIX_EPOCH + Duration::from_secs(1_000))
                .unwrap(),
            ethera_spec::PeriodId(1)
        );
        assert_eq!(
            schedule
                .period_at(UNIX_EPOCH + Duration::from_secs(1_299))
                .unwrap(),
            ethera_spec::PeriodId(3)
        );
    }

    #[test]
    fn period_schedule_waits_for_the_next_boundary() {
        let schedule = PeriodSchedule::new(1_000, Duration::from_secs(100)).unwrap();

        assert_eq!(
            schedule.until_next_period(UNIX_EPOCH + Duration::from_secs(1_250)),
            Duration::from_secs(50)
        );
        assert_eq!(
            schedule.until_next_period(UNIX_EPOCH + Duration::from_secs(1_300)),
            Duration::from_secs(100)
        );
    }
}
