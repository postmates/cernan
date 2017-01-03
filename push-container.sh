#!/usr/bin/env bash

function usage() {
  echo "$0 <version>"
}

if [ -z "$1" ]; then
  usage
  exit 1
fi

VERSION="${1}"

docker login quay.io/postmates
docker push quay.io/postmates/cernan:${VERSION}
