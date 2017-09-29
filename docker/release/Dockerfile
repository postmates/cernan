FROM alpine:edge

RUN echo '@testing http://dl-cdn.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories

RUN apk update \
  && apk upgrade --no-cache

RUN apk add --no-cache --update \
  ca-certificates \
  llvm-libunwind@testing \
  openssl && \
  update-ca-certificates && \
  rm -rf /var/cache/apk/* && \
  mkdir -p /etc/cernan/scripts

COPY docker/release/entrypoint.sh /entrypoint.sh
COPY target/x86_64-unknown-linux-musl/release/cernan /usr/bin/cernan
COPY examples/configs/quickstart.toml /etc/cernan/cernan.toml

ENV STATSD_PORT 8125

ENTRYPOINT /usr/bin/cernan
CMD --config /etc/cernan/cernan.toml
