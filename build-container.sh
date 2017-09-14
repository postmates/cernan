#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

function usage() {
    echo "$0 <version>"
}

if [ -z "$1" ]; then
    usage
    exit 1
fi

cargo clean

VERSION="${1}"

docker build -t cernan-build -f docker/build/Dockerfile .
docker run -v $(pwd):/source -w /source -it cernan-build cargo build --release
cp target/x86_64-unknown-linux-musl/release/cernan target/bindir/
docker build -t quay.io/postmates/cernan:latest -t "quay.io/postmates/cernan:$VERSION" -f docker/release/Dockerfile .
docker push "quay.io/postmates/cernan:$VERSION"
docker push "quay.io/postmates/cernan:latest"
