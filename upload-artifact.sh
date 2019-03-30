#!/usr/bin/env bash

set -o errexit
set -o pipefail

function usage() {
    echo "$0 <version>"
}

if [ -z "$1" ]; then
    usage
    exit 1
fi

VERSION="${1}"

mkdir -p target/artifact
TARGET="target/artifact/cernan"

container_id=$(docker create quay.io/postmates/cernan:$VERSION)
# Output of docker cp is always a tar archive regardless of source
docker cp $container_id:/usr/bin/cernan - | tar x -C target/artifact
docker rm -v $container_id

DEST="s3://artifacts.postmates.com/binaries/cernan/cernan-$VERSION"

aws s3 cp $TARGET $DEST
