FROM quay.io/postmates/postmates-rust:stable

COPY . /source
WORKDIR /source
ENV SSL_DIR=/opt/openssl \
    PATH=/usr/local/bin:$SSL_DIR/bin:$PATH \
    PKG_CONFIG_PATH=$SSL_DIR/lib/pkgconfig \ 
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    SSL_CERT_DIR=/etc/ssl/certs \
    OPENSSL_LIB_DIR=$SSL_DIR/lib \
    OPENSSL_INCLUDE_DIR=$SSL_DIR/include \
    OPENSSL_DIR=$SSL_DIR \
    OPENSSL_STATIC=1
RUN cargo build --release && \
    strip /source/target/x86_64-unknown-linux-musl/release/cernan 
