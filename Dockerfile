FROM rust:1.91-slim-bookworm@sha256:8514999d4786ef12efe89239e86b3d0a021b94b9d35108c8efe6c79ca7dc1a65 AS chef

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

FROM debian:bookworm-slim@sha256:96e378d7e6531ac9a15ad505478fcc2e69f371b10f5cdf87857c4b8188404716 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/publisher /usr/local/bin/publisher

ENTRYPOINT ["/usr/local/bin/publisher"]
