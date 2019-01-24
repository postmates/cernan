FROM ekidd/rust-musl-builder:1.32.0 as builder

RUN VERS=1.2.11 && \
    cd /home/rust/libs && \
    curl -LO http://zlib.net/zlib-$VERS.tar.gz && \
    tar xzf zlib-$VERS.tar.gz && cd zlib-$VERS && \
    CC=musl-gcc CFLAGS=-fPIC ./configure --static --prefix=/usr/local/musl && \
    make && sudo make install && \
    cd .. && rm -rf zlib-$VERS.tar.gz zlib-$VERS

RUN cd /home/rust/libs && \
    curl -LO https://github.com/lz4/lz4/archive/master.tar.gz && \
    tar xfz master.tar.gz && \
    ls && \
    cd lz4-master && \
    CC=musl-gcc CFLAGS=-fPIC make prefix=/usr/local/musl && \
    sudo make install prefix=/usr/local/musl && \
    cd .. && \
    rm -rf master.tar.gz lz4-master

RUN sudo apt-get update && \
    sudo apt-get install -y python2.7-minimal && \
    sudo ln -sf /usr/bin/python2.7 /usr/bin/python

ENV CC=musl-gcc \
    CFLAGS=-I/usr/local/musl/include \
    LDFLAGS=-L/usr/local/musl/lib

COPY --chown=rust:rust . /source
RUN cd /source && cargo build --release

FROM alpine:3.8

RUN apk update \
  && apk upgrade --no-cache

RUN apk add --no-cache --update \
  ca-certificates \
  llvm-libunwind \
  openssl && \
  update-ca-certificates && \
  rm -rf /var/cache/apk/* && \
  mkdir -p /etc/cernan/scripts

COPY --from=builder /source/target/x86_64-unknown-linux-musl/release/cernan /usr/bin/cernan
COPY examples/configs/quickstart.toml /etc/cernan/cernan.toml

ENV STATSD_PORT 8125

ENTRYPOINT /usr/bin/cernan
CMD ["--config", "/etc/cernan/cernan.toml"]
