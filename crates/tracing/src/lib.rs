//! Tracing bootstrap for the shared publisher.

use tracing_subscriber::{fmt, EnvFilter};

pub fn init(level: &str, format: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    match format {
        "pretty" => {
            fmt::Subscriber::builder()
                .with_env_filter(filter)
                .with_target(true)
                .pretty()
                .init();
        }
        _ => {
            fmt::Subscriber::builder()
                .with_env_filter(filter)
                .with_target(true)
                .json()
                .init();
        }
    }
}
