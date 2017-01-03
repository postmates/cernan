FROM alpine:edge

COPY target/x86_64-unknown-linux-musl/release/cernan /usr/local/bin/cernan
RUN mkdir -p /etc/cernan
COPY examples/configs/basic.toml /etc/cernan/cernan.conf
RUN chmod +x /usr/local/bin/cernan
ENTRYPOINT ["/usr/local/bin/cernan"]
CMD ["-C", "/etc/cernan/cernan.conf"]
