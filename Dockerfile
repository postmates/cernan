FROM quay.io/postmates/postmates-rust:stable

ADD . /source
WORKDIR /source
RUN cargo build --release 

WORKDIR /source/target/release

ADD Dockerfile.prod Dockerfile 
ADD examples/configs/basic.toml cernan.toml

CMD docker build -t quay.io/postmates/cernan:latest . && \
    docker push quay.io/postmates/cernan:latest 
