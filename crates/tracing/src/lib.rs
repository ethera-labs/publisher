//! Tracing bootstrap for the shared publisher.

use tracing_subscriber::{fmt, EnvFilter};

pub fn init(level: &str, pretty: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    if pretty {
        fmt::Subscriber::builder()
            .with_env_filter(filter)
            .with_target(true)
            .pretty()
            .init();
    } else {
        fmt::Subscriber::builder()
            .with_env_filter(filter)
            .with_target(true)
            .json()
            .init();
    }
}
