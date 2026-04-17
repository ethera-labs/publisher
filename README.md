# Ethera Shared Publisher

Cross-chain transaction coordinator for Ethera rollups. Accepts QUIC connections from sidecars, coordinates two-phase
commit consensus for cross-chain transactions, and exposes an HTTP API for observability.

## Quick Start

```bash
# Build
cargo build --release

# Run (development mode with pretty logs)
just dev

# Run with custom config
./target/release/publisher --config config.yaml
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
┌─────────▼───────────┐ ┌─────▼─────────────┐
│ Rollup A Sidecar    │ │ Rollup B Sidecar  │
└─────────────────────┘ └───────────────────┘
```

### Crates

| Crate                | Description                                               |
|----------------------|-----------------------------------------------------------|
| `bin/publisher`      | Binary entrypoint, period loop, shutdown                  |
| `crates/config`      | YAML config + env-var overrides                           |
| `crates/coordinator` | 2PC consensus state machine, message dispatch             |
| `crates/metrics`     | Prometheus metrics                                        |
| `crates/server`      | Axum HTTP API (`/health`, `/ready`, `/stats`, `/metrics`) |
| `crates/tracing`     | Structured logging bootstrap                              |
| `crates/transport`   | QUIC server, length-prefixed framing, TLS                 |
| `crates/spec`        | Vendored domain types (ChainId, PeriodId, etc.)           |
| `crates/spec-proto`  | Vendored protobuf message types + conversions             |
| `crates/spec-sbcp`   | Vendored SBCP types and instance ID generation            |

Protocol types and wire format come from the shared `specs/compose/` crates.
The three `spec-*` crates are temporary vendored copies pending native integration.

## Configuration

Configuration is loaded from a YAML file (`config.yaml` by default) with environment variable overrides.
Use `--config <path>` to specify a custom config file.

| YAML Key                  | Env Override              | Default        |
|---------------------------|---------------------------|----------------|
| `server.listen_addr`      | `SERVER_LISTEN_ADDR`      | `0.0.0.0:8080` |
| `server.max_message_size` | `SERVER_MAX_MESSAGE_SIZE` | `4194304`      |
| `api.listen_addr`         | `API_LISTEN_ADDR`         | `0.0.0.0:8081` |
| `api.request_timeout`     | `API_REQUEST_TIMEOUT`     | `15s`          |
| `consensus.timeout`       | `CONSENSUS_TIMEOUT`       | `60s`          |
| `metrics.enabled`         | `METRICS_ENABLED`         | `true`         |
| `log.level`               | `LOG_LEVEL`               | `info`         |
| `log.pretty`              | `LOG_PRETTY`              | `false`        |

## HTTP API

| Endpoint       | Description                                    |
|----------------|------------------------------------------------|
| `GET /health`  | Liveness probe                                 |
| `GET /ready`   | Readiness probe (503 until a sidecar connects) |
| `GET /stats`   | Application statistics                         |
| `GET /metrics` | Prometheus metrics (404 when disabled)         |

## Development

```bash
just build       # cargo build --workspace
just test        # cargo test --workspace
just lint        # cargo clippy --workspace --all-targets -- -D warnings
just lint-fix    # clippy with auto-fix
just fmt         # cargo fmt --all
just fmt-check   # check formatting without modifying
just ci          # fmt-check + lint + test (full CI gate)
just dev         # run with pretty logs + debug level
just docker      # build Docker image
```

## Docker

```bash
docker build -t publisher .
docker run -p 8080:8080/udp -p 8081:8081 publisher
```

## License

Distributed under the GNU General Public License v3.0. See [`COPYING`](./COPYING) for the full text.
