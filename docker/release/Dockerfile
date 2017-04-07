FROM alpine:3.5

COPY docker/release/confd /etc/confd
COPY docker/release/confd-bin /usr/bin/confd
COPY docker/release/entrypoint.sh /entrypoint.sh
COPY docker/release/cernan /usr/bin/cernan

ENTRYPOINT ["/entrypoint.sh"]
