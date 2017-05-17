#!/usr/bin/env bash

set -e
set -u
set -x

if [[ "$channel" == "nightly" ]]; then
    cargo install clippy && \
    cargo install cargo-fuzz 
fi
