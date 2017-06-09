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
confd_version=0.11.0
confd_url="https://github.com/kelseyhightower/confd/releases/download/v${confd_version}/confd-${confd_version}-linux-amd64"
mkdir -p target/bindir/
[ -f target/bindir/confd-bin ] || curl -L "$confd_url" > target/bindir/confd-bin
chmod +x target/bindir/confd-bin

docker build -t cernan-build -f docker/build/Dockerfile .
CONTAINER_ID=$(docker create cernan-build)
# Remove 'cernan' from the build image 
docker container cp ${CONTAINER_ID}:/source/target/release/cernan target/bindir/
docker rm ${CONTAINER_ID}
docker build --build-arg EXE_ROOT=target/bindir -t quay.io/postmates/cernan:latest -t quay.io/postmates/cernan:${VERSION} -f docker/release/Dockerfile .
rm target/bindir/cernan
