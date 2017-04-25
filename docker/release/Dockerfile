FROM alpine:edge

RUN echo '@testing http://dl-cdn.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories

RUN apk add --no-cache --update \
  llvm-libunwind@testing \ 
  openssl

RUN apk update \
  && apk upgrade --no-cache

ARG DIST_ROOT="target/bindir"

COPY ${DIST_ROOT}/confd /etc/confd
COPY ${DIST_ROOT}/entrypoint.sh /entrypoint.sh
COPY ${DIST_ROOT}/confd-bin /usr/bin/confd
COPY ${DIST_ROOT}/cernan /usr/bin/cernan

ENV STATSD_PORT 8125

ENTRYPOINT ["/entrypoint.sh"]
