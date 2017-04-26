FROM debian:jessie-slim

RUN apt-get update && apt-get -y upgrade && \
    apt-get install -y build-essential git curl openssl pkg-config libssl-dev

ARG channel="stable"
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain "${channel}"

ENV PATH=/root/.cargo/bin:"$PATH"
RUN if [ "$channel" = "nightly" ]; then \
    cargo install clippy && \
    cargo install cargo-fuzz; \
fi
RUN cargo install rustfmt
