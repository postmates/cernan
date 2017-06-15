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
confd_version=0.11.0
confd_url="https://github.com/kelseyhightower/confd/releases/download/v${confd_version}/confd-${confd_version}-linux-amd64"
mkdir -p target/bindir/
[ -f target/bindir/confd-bin ] || curl -L "$confd_url" > target/bindir/confd-bin
chmod +x target/bindir/confd-bin

docker build -t cernan-build -f docker/build/Dockerfile .
docker run -v $(pwd):/source -w /source -it cernan-build cargo build --release
cp target/release/cernan target/bindir/
docker build -t quay.io/postmates/cernan:latest -t "quay.io/postmates/cernan:$VERSION" -f docker/release/Dockerfile .
docker push "quay.io/postmates/cernan:$VERSION"
docker push "quay.io/postmates/cernan:latest"
