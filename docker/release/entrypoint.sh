#!/bin/sh

/usr/bin/confd -onetime -backend env
/usr/bin/cernan --config /etc/cernan.toml
