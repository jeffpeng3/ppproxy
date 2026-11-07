FROM rust AS builder

RUN apt-get update && apt-get install -y musl-tools && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN rustup target add x86_64-unknown-linux-musl

RUN cargo init ppproxy
WORKDIR /app/ppproxy
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release --target x86_64-unknown-linux-musl

COPY src ./src
RUN touch -a -m ./src/main.rs
RUN cargo build --release --target x86_64-unknown-linux-musl


FROM alpine:latest

RUN apk add --no-cache rp-pppoe
RUN apk add --no-cache ppp-pppoe
RUN apk add --no-cache nftables
RUN apk add --no-cache tzdata
RUN apk add --no-cache iproute2
RUN apk add --no-cache curl iputils-ping

WORKDIR /app
ADD rt_tables /etc/iproute2/rt_tables
ADD nftables.conf /etc/nftables.conf
COPY gost ./gost
COPY --from=builder /app/ppproxy/target/x86_64-unknown-linux-musl/release/ppproxy .

ENV TZ=Asia/Taipei
ENV docker=true

CMD ["./ppproxy"]