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
[ -f docker/release/confd-bin ] || curl -L "$confd_url" > docker/release/confd-bin
chmod +x docker/release/confd-bin

# docker build -t cernan-build -f docker/build/Dockerfile .
CONTAINER_ID=$(docker create cernan-build)
# Remove 'cernan' from the build image 
docker container cp ${CONTAINER_ID}:/source/target/release/cernan docker/release/
docker rm ${CONTAINER_ID}
# docker build -t quay.io/postmates/cernan:latest -t quay.io/postmates/cernan:${VERSION} -f docker/release/Dockerfile .
docker build -t cernan:latest -t cernan:${VERSION} -f docker/release/Dockerfile .
rm docker/release/cernan
