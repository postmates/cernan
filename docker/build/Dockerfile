FROM alpine:edge

RUN echo '@testing http://dl-cdn.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories

RUN apk add --no-cache --update \
  git \
  alpine-sdk \
  perl \
  openssl-dev \
  cargo@testing \
  rust@testing
