FROM alpine:edge

RUN echo '@testing http://dl-cdn.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories

RUN apk add --no-cache --update \
  llvm-libunwind@testing \ 
  openssl

RUN apk update \
  && apk upgrade --no-cache

ARG EXE_ROOT="target/bindir"

COPY docker/release/confd /etc/confd
COPY docker/release/entrypoint.sh /entrypoint.sh
COPY ${EXE_ROOT}/confd-bin /usr/bin/confd
COPY ${EXE_ROOT}/cernan /usr/bin/cernan

ENV STATSD_PORT 8125

ENTRYPOINT ["/entrypoint.sh"]
