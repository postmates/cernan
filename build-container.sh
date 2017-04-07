#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace

function usage() {
    echo "$0 <version>"
}

if [ -z "$1" ]; then
    usage
    exit 1
fi

VERSION="${1}"

docker build -t cernan-build -f docker/build/Dockerfile .
CONTAINER_ID=$(docker create cernan-build)
# Remove 'cernan' from the build image 
docker container cp ${CONTAINER_ID}:/source/target/x86_64-unknown-linux-musl/release/cernan docker/release/
docker rm ${CONTAINER_ID}
cp examples/configs/basic.toml docker/release/cernan.toml
docker build -t cernan:latest -t cernan:${VERSION} -f docker/release/Dockerfile .
rm docker/release/cernan.toml docker/release/cernan
