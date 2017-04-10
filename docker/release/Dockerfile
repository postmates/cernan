FROM alpine:3.5

RUN apk update \
  && apk upgrade --no-cache

COPY docker/release/confd /etc/confd
COPY docker/release/confd-bin /usr/bin/confd
COPY docker/release/entrypoint.sh /entrypoint.sh
COPY docker/release/cernan /usr/bin/cernan

ENV STATSD_PORT 8125

ENTRYPOINT ["/entrypoint.sh"]
