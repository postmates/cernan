FROM quay.io/postmates/postmates-rust:latest

COPY . /source
WORKDIR /source
ENV OPENSSL_STATIC=1
ENV OPENSSL_DIR=/opt/libressl-dev-musl
ENV OPENSSL_INCLUDE_DIR=/opt/libressl-dev-musl/usr/include
ENV OPENSSL_LIB_DIR=/opt/libressl-dev-musl/usr/lib
ENV PATH="/opt/libressl-dev-musl/bin:${PATH}"
RUN cargo build --release && \
    strip /source/target/x86_64-unknown-linux-musl/release/cernan 
