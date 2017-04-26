FROM debian:jessie-slim

RUN apt-get update && apt-get -y upgrade && \
    apt-get install -y --no-install-recommends build-essential git curl openssl pkg-config \
      libssl-dev cmake libcurl4-openssl-dev libelf-dev libdw-dev binutils-dev python ca-certificates 

RUN curl -L https://github.com/SimonKagstrom/kcov/archive/v33.tar.gz | tar xvz && \
  cd kcov-33 && \
  mkdir build && \
  cd build && \
  cmake .. && \
  make && \
  make install && \
  cd ../.. && \
  rm -rf kcov-master 

ARG channel="stable"
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain "${channel}"

ENV PATH=/root/.cargo/bin:"$PATH"
RUN if [ "$channel" = "nightly" ]; then \
    cargo install clippy && \
    cargo install cargo-fuzz; \
fi
RUN cargo install rustfmt

# shrinky dink
RUN apt-get remove -y cmake libcurl4-openssl-dev libelf-dev libdw-dev binutils-dev python && \
    rm -rf /var/lib/apt/lists/*
