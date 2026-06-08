set dotenv-load

build:
    cargo build --workspace

release:
    cargo build --release -p publisher

test:
    cargo test --workspace

test-verbose:
    cargo test --workspace -- --nocapture

lint:
    cargo clippy --workspace --all-targets -- -D warnings

lint-fix:
    cargo clippy --workspace --all-targets --fix --allow-staged -- -D warnings

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

machete:
    cargo machete

deny:
    cargo deny check

doc:
    cargo doc --workspace --no-deps --open

ci: fmt-check lint test

ci-full: fmt-check lint test deny machete

run *ARGS:
    cargo run -p publisher -- {{ARGS}}

dev:
    LOG_PRETTY=true LOG_LEVEL=debug cargo run -p publisher

install-hooks:
    pre-commit install

pre-commit:
    pre-commit run --all-files

install-tools:
    cargo install cargo-deny --locked
    cargo install cargo-machete --locked

docker tag="publisher:latest":
    docker build -t {{tag}} .

clean:
    cargo clean
