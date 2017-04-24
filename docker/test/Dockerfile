FROM alpine:edge

RUN echo '@testing http://dl-cdn.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories

RUN apk add --no-cache --update \
  alpine-sdk \
  perl \
  openssl-dev \
  cargo@testing \
  rust@testing

ENV PATH=/root/.cargo/bin:$PATH
RUN cargo install rustfmt
