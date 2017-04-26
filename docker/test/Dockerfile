FROM debian:jessie-slim

RUN apt-get update && apt-get -y upgrade && \
    apt-get install -y --no-install-recommends build-essential git curl openssl pkg-config libssl-dev ca-certificates

ARG channel="stable"
ENV PATH=/root/.cargo/bin:"$PATH"
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain "${channel}"

# shrinky dink
RUN apt-get remove -y cmake libcurl4-openssl-dev libelf-dev python && \
    apt-get autoremove -y && \ 
    rm -rf /var/lib/apt/lists/*
