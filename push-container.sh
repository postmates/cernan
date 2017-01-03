#!/usr/bin/env bash

function usage() {
  echo "$0 <version>"
}

if [ -z "$1" ]; then
  usage
  exit 1
fi

VERSION="${1}"

sudo docker login quay.io/postmates
sudo docker push quay.io/postmates/cernan:${VERSION}
