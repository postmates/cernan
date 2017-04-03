FROM busybox:glibc

COPY docker/release/cernan /usr/bin/cernan
COPY docker/release/cernan.toml /etc/cernan.toml 

ENTRYPOINT ["/usr/bin/cernan", "--config", "/etc/cernan.toml"]
