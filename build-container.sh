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

docker build -t "quay.io/postmates/cernan:$VERSION" .
docker push "quay.io/postmates/cernan:$VERSION"
