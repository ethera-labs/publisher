# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
just build          # cargo build --workspace
just test           # cargo test --workspace
just lint           # cargo clippy --workspace --all-targets -- -D warnings
just lint-fix       # clippy with auto-fix
just fmt            # cargo fmt --all (formats in place)
just fmt-check      # check formatting without modifying
just ci             # fmt-check + lint + test (full CI gate)
just ci-full        # ci + cargo deny + cargo machete
just dev            # run with LOG_PRETTY=true and LOG_LEVEL=debug
just run -- <args>  # cargo run -p publisher -- <args>
```

Run a single test:

```bash
cargo test -p <crate-name> <test_name>
```

Pre-commit hooks (fmt, clippy, deny, machete) run automatically via `pre-commit`. Install with `just install-hooks`.
Requires `cargo-deny` and `cargo-machete` (`just install-tools`).

Toolchain is pinned to Rust **1.91** via `rust-toolchain.toml`.

## Architecture

The publisher is the **coordinator** side of a 2-Phase Commit (2PC) protocol for synchronous composability across
rollups. Sidecars (one per rollup sequencer) connect to the publisher over QUIC; the publisher orchestrates cross-rollup
transactions (xTs) so that every involved chain either commits or aborts together.

Protocol state lives in the spec crates: `ethera_spec_sbcp::Publisher` owns periods, superblock numbering, chain
reservation, proof aggregation, and one `ethera_spec_scp::PublisherInstance` per in-flight xT (created by
`start_instance`, fed by `process_vote`/`timeout_instance`). The coordinator owns only transport, scheduling
(timers, pending-xT queue), and L1 infrastructure. The spec state machines are synchronous; their effect traits are
implemented by `bridge::OutboundSink` as non-blocking mpsc enqueues drained by an async task
(`Coordinator::run_outbound`) that performs the QUIC broadcasts and L1 submissions.

### Request lifecycle

1. A sidecar sends an `XtRequest` (length-prefixed protobuf over QUIC).
2. `Coordinator::handle_xt_request` calls `sbcp::Publisher::start_instance`. If any target chain is reserved by an
   active xT the request is queued (max 100 entries); otherwise the spec assigns `PeriodId`/`SequenceNumber`, computes
   the `instance_id`, creates the SCP instance, and broadcasts `StartInstance` to all connected sidecars.
3. Each sidecar votes via a `Vote` message routed to `sbcp::Publisher::process_vote`: one `false` vote triggers
   immediate `Decided(false)`; unanimous `true` votes produce `Decided(true)`. On decision the spec releases the
   chains; the coordinator records metrics and `drain_queue` starts the next queued xT whose chains are free.
4. A background `reaper_loop` (1 s tick) calls `reap_timed_out_xts` (`sbcp` `timeout_instance()`) to abort stale xTs
   that exceed `consensus.timeout`, and `reap_expired_proofs`, which arms a `consensus.proof_window` timer while
   `settling_superblock()` reports a terminated superblock awaiting proofs and triggers `proof_timeout()` (rollback)
   on expiry.
5. `period_loop` calls `sbcp::Publisher::start_period` on the `consensus.period_duration` cadence: it advances the
   target superblock, broadcasts `StartPeriod`, and refuses (backpressure) once more than
   `consensus.proof_window_periods` superblocks are unfinalized.
6. Proofs (`ProofData` is the spec's generic `ChainProof` type) are validated and collected by
   `sbcp::Publisher::receive_proof` (chain membership, ordering, period, duplicates — rejections are returned as
   `PublisherError`). When all registered chains have reported, the spec requests superblock proof generation
   (`PublisherProver` is fire-and-forget since real proving is long-running); the result is fed back via
   `superblock_proof_ready` (→ L1 submission, success advances the settled state) or `superblock_proof_failed`
   (→ `rollback()`, which also abandons in-flight instances and invalidates the pending aggregation). In this
   deployment aggregation is the identity (op-succinct proofs are final), completed inline by the outbound task.

### Crate responsibilities

| Crate                | Role                                                                                    |
|----------------------|-----------------------------------------------------------------------------------------|
| `bin/publisher`      | `main`: wires QUIC server, coordinator, HTTP API; period loop, reaper loop, shutdown    |
| `crates/config`      | YAML config + env-var overrides (`SECTION_FIELD` convention, no prefix)                 |
| `crates/coordinator` | Bridges spec state machines to QUIC/L1 (`bridge`), message dispatch (`handlers`), queueing, reaping |
| `crates/transport`   | QUIC server (quinn), length-prefixed framing, self-signed TLS, per-connection callbacks |
| `crates/server`      | Axum HTTP API: `/health`, `/ready`, `/stats`, `/metrics`                                |
| `crates/metrics`     | Prometheus metrics via `prometheus-client`                                              |
| `crates/tracing`     | `tracing-subscriber` setup (json or pretty format)                                      |

### Key internal invariants

- **Spec effect-trait implementations never block**: `bridge::OutboundSink` only enqueues onto an unbounded mpsc
  channel because the spec invokes its traits while holding its internal `std::sync::Mutex`. Never `.await` (or do
  I/O) inside a spec callback.
- **Instance ID** is a deterministic hash of `(PeriodId, SequenceNumber, XtRequest)` computed by `generate_instance_id`
  in the SBCP spec crate.
- **Chain reservation** lives in `sbcp::Publisher`: chains are reserved by `start_instance` and released when the
  instance decides (`process_vote`/`timeout_instance`), or cleared wholesale on rollback together with all in-flight
  instances.
- **Chain membership is dynamic**: handshakes update the registry and push the full set into
  `sbcp::Publisher::update_chains`, which defines when "all proofs" are collected.
- **`/ready` returns 503** until at least one sidecar is connected (checked via `QuicServer::connection_count`).

### Wire protocol

Messages are length-prefixed protobuf (`ethera_spec_proto::Message`). Framing is in `crates/transport/src/framing.rs`.
TLS uses a self-signed ephemeral cert generated by `rcgen` at startup (no mTLS/auth beyond QUIC's transport-layer
encryption).

## Configuration

Configuration is loaded from a YAML file (`config.yaml` by default, override with `--config <path>`).
Environment variables override YAML values (uppercase `SECTION_FIELD` convention, no prefix):

| YAML Key                    | Env Override                | Default        |
|-----------------------------|-----------------------------|----------------|
| `server.listen_addr`        | `SERVER_LISTEN_ADDR`        | `0.0.0.0:8080` |
| `server.max_message_size`   | `SERVER_MAX_MESSAGE_SIZE`   | `4194304`      |
| `api.listen_addr`           | `API_LISTEN_ADDR`           | `0.0.0.0:8081` |
| `api.request_timeout`       | `API_REQUEST_TIMEOUT`       | `15s`          |
| `consensus.timeout`         | `CONSENSUS_TIMEOUT`         | `60s`          |
| `consensus.period_duration` | `CONSENSUS_PERIOD_DURATION` | `3840s`        |
| `consensus.proof_window`    | `CONSENSUS_PROOF_WINDOW`    | `7200s`        |
| `consensus.proof_window_periods` | `CONSENSUS_PROOF_WINDOW_PERIODS` | `168`  |
| `metrics.enabled`           | `METRICS_ENABLED`           | `true`         |
| `log.level`                 | `LOG_LEVEL`                 | `info`         |
| `log.pretty`                | `LOG_PRETTY`                | `false`        |
| `settlement.l1_rpc_url`     | `SETTLEMENT_L1_RPC_URL`     | empty          |
| `settlement.l2oo_address`   | `SETTLEMENT_L2OO_ADDRESS`   | empty          |
| `settlement.proposer_key`   | `SETTLEMENT_PROPOSER_KEY`   | empty          |
| `settlement.mock`           | `SETTLEMENT_MOCK`           | `false`        |
