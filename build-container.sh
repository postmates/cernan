#!/usr/bin/env bash

function usage() {
  echo "$0 <version>"
}

if [ -z "$1" ]; then
  usage
  exit 1
fi

VERSION="${1}"

docker run \
  -e 'RUST_BACKTRACE=1' \
  -e 'SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt' \
  --rm \
  -it \
  -v "$(pwd)":/home/rust/src ekidd/rust-musl-builder:1.13 /bin/bash \
  -c "sudo apt-get update && sudo apt-get install -y make wget libssl-dev libreadline-dev && cargo build --verbose --release --target=x86_64-unknown-linux-musl"

docker build -t quay.io/postmates/cernan:${VERSION} .
