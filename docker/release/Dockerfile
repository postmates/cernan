FROM alpine:edge

RUN echo '@testing http://dl-cdn.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories

RUN apk update \
  && apk upgrade --no-cache

RUN apk add --no-cache --update \
  ca-certificates \
  llvm-libunwind@testing \ 
  openssl && \
  update-ca-certificates && \
  rm -rf /var/cache/apk/*

COPY docker/release/confd /etc/confd
COPY docker/release/entrypoint.sh /entrypoint.sh
COPY target/bindir/confd-bin /usr/bin/confd
COPY target/release/cernan /usr/bin/cernan

ENV STATSD_PORT 8125

ENTRYPOINT ["/entrypoint.sh"]
