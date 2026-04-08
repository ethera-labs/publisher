# Ethera Shared Publisher

Cross-chain transaction coordinator for Ethera rollups. Accepts QUIC connections from sidecars, coordinates two-phase
commit consensus for cross-chain transactions, and exposes an HTTP API for observability.

## Quick Start

```bash
# Build
cargo build --release

# Run (development mode with pretty logs)
just dev

# Run with custom flags
./target/release/publisher --quic.listen-addr 0.0.0.0:8080 --log.level debug --log.format pretty
```

## Architecture

```
┌──────────────────────────────────┐
│      Shared Publisher            │
│  (2PC Coordinator, QUIC Server)  │
└─────────────────┬────────────────┘
                  │ QUIC (length-prefixed protobuf)
          ┌───────┴───────────┐
          │                   │
┌─────────▼───────────┐ ┌────▼──────────────┐
│ Rollup A Sidecar    │ │ Rollup B Sidecar  │
└─────────────────────┘ └───────────────────┘
```

### Crates

| Crate                | Description                                   |
|----------------------|-----------------------------------------------|
| `bin/publisher`      | Binary entrypoint                             |
| `crates/config`      | CLI args and env-var configuration            |
| `crates/coordinator` | 2PC consensus state machine, message dispatch |
| `crates/metrics`     | Prometheus metrics                            |
| `crates/server`      | HTTP API (health, readiness, stats, metrics)  |
| `crates/tracing`     | Structured logging bootstrap                  |
| `crates/transport`   | QUIC server, length-prefixed framing, TLS     |

Protocol types and wire format come from the shared `specs/compose/` crates.

## Configuration

All settings are exposed as CLI flags with `PUBLISHER_*` env-var overrides:

| Flag                       | Env                                | Default        |
|----------------------------|------------------------------------|----------------|
| `--quic.listen-addr`       | `PUBLISHER_QUIC_LISTEN_ADDR`       | `0.0.0.0:8080` |
| `--quic.max-message-size`  | `PUBLISHER_QUIC_MAX_MESSAGE_SIZE`  | `4194304`      |
| `--api.listen-addr`        | `PUBLISHER_API_LISTEN_ADDR`        | `0.0.0.0:8081` |
| `--consensus.timeout-secs` | `PUBLISHER_CONSENSUS_TIMEOUT_SECS` | `60`           |
| `--log.level`              | `PUBLISHER_LOG_LEVEL`              | `info`         |
| `--log.format`             | `PUBLISHER_LOG_FORMAT`             | `json`         |

## HTTP API

| Endpoint       | Description                                    |
|----------------|------------------------------------------------|
| `GET /health`  | Liveness probe                                 |
| `GET /ready`   | Readiness probe (503 until a sidecar connects) |
| `GET /stats`   | Application statistics                         |
| `GET /metrics` | Prometheus metrics                             |

## Development

```bash
just build       # cargo build --release
just check       # cargo check --all-targets
just test        # cargo test --all
just lint        # cargo clippy
just fmt         # check formatting
just fmt-fix     # fix formatting
just dev         # run with pretty logs + debug level
just docker      # build Docker image
```

## Docker

```bash
docker build -t publisher .
docker run -p 8080:8080/udp -p 8081:8081 publisher
```

## License

MIT
