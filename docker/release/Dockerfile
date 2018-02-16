FROM alpine:3.6

RUN apk update \
  && apk upgrade --no-cache

RUN apk add --no-cache --update \
  ca-certificates \
  llvm-libunwind \
  openssl && \
  update-ca-certificates && \
  rm -rf /var/cache/apk/* && \
  mkdir -p /etc/cernan/scripts

COPY target/x86_64-unknown-linux-musl/release/cernan /usr/bin/cernan
COPY examples/configs/quickstart.toml /etc/cernan/cernan.toml

ENV STATSD_PORT 8125

ENTRYPOINT /usr/bin/cernan
CMD ["--config", "/etc/cernan/cernan.toml"]
