#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

usage() {
    echo "$0 <version>"
}

if [[ -z "$1" ]]; then
    usage
    exit 1
fi

cargo clean || true

VERSION=$1

mkdir -p target/bindir
docker build -t cernan-build -f docker/build/Dockerfile .
docker run -v "$PWD:/source" -w /source -it cernan-build cargo build --release
docker build -t quay.io/postmates/cernan:latest -t "quay.io/postmates/cernan:$VERSION" -f docker/release/Dockerfile .
