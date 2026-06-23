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

## Configuration

Configuration is loaded from a YAML file (`config.yaml` by default) with environment variable overrides.
Use `--config <path>` to specify a custom config file.

| YAML Key                           | Env Override                          | Default        |
|------------------------------------|---------------------------------------|----------------|
| `server.listen_addr`               | `SERVER_LISTEN_ADDR`                  | `0.0.0.0:8080` |
| `server.max_message_size`          | `SERVER_MAX_MESSAGE_SIZE`             | `4194304`      |
| `api.listen_addr`                  | `API_LISTEN_ADDR`                     | `0.0.0.0:8081` |
| `api.request_timeout`              | `API_REQUEST_TIMEOUT`                 | `15s`          |
| `consensus.timeout`                | `CONSENSUS_TIMEOUT`                   | `60s`          |
| `consensus.period_duration`        | `CONSENSUS_PERIOD_DURATION`           | `3840s`        |
| `consensus.proof_window`           | `CONSENSUS_PROOF_WINDOW`              | `7200s`        |
| `metrics.enabled`                  | `METRICS_ENABLED`                     | `true`         |
| `log.level`                        | `LOG_LEVEL`                           | `info`         |
| `log.pretty`                       | `LOG_PRETTY`                          | `false`        |
| `settlement.l1_rpc_url`            | `SETTLEMENT_L1_RPC_URL`               | empty          |
| `settlement.dispute_game_factory`  | `SETTLEMENT_DISPUTE_GAME_FACTORY`     | empty          |
| `settlement.anchor_state_registry` | `SETTLEMENT_ANCHOR_STATE_REGISTRY`    | empty          |
| `settlement.proposer_key`          | `SETTLEMENT_PROPOSER_KEY`             | empty          |
| `settlement.mock`                  | `SETTLEMENT_MOCK`                     | `false`        |
| `proofs.proving_mode`              | `PROOFS_PROVING_MODE`                 | `real`         |

## HTTP API

| Endpoint                      | Description                                    |
|-------------------------------|------------------------------------------------|
| `GET /health`                 | Liveness probe                                 |
| `GET /ready`                  | Readiness probe (503 until a sidecar connects) |
| `GET /stats`                  | Application statistics                         |
| `GET /metrics`                | Prometheus metrics (404 when disabled)         |
| `POST /v1/proofs/op-succinct` | Submit a proof bundle for a chain/superblock   |

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
