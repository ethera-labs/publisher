FROM rust:1.100-slim-bookworm@sha256:7c7d4eb4d05b7781187a327809466cfddf29753f6defdc3d19a0e291951b7a37 AS chef

WORKDIR /app
RUN cargo install cargo-chef --locked

FROM chef AS planner

COPY Cargo.lock Cargo.toml rust-toolchain.toml rustfmt.toml ./
COPY bin ./bin
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY Cargo.lock Cargo.toml rust-toolchain.toml rustfmt.toml ./
COPY bin ./bin
COPY crates ./crates
RUN cargo build --locked --release --bin publisher

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/publisher /usr/local/bin/publisher

ENTRYPOINT ["/usr/local/bin/publisher"]
