FROM alpine:edge

COPY target/x86_64-unknown-linux-musl/release/cernan /usr/local/bin/cernan
RUN chmod +x /usr/local/bin/cernan
