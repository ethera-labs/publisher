FROM rust:1.99-slim-bookworm@sha256:24707e70a4f1e23d9a7b25202ec43126d28a52294b96aad5c670df1fd1f1d400 AS chef

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
