#!/bin/sh

export AWS_REGION="$(wget -q http://169.254.169.254/latest/meta-data/hostname -O - | cut -d'.' -f2)"
/usr/bin/confd -onetime -backend env
/usr/bin/cernan --config /etc/cernan.toml
