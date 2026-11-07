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
RUN apk add --no-cache dhcpcd

WORKDIR /app
ADD rt_tables /etc/iproute2/rt_tables
ADD nftables.conf /etc/nftables.conf
COPY dhcpcd-hooks/20-ppproxy-ipv6 /usr/lib/dhcpcd/dhcpcd-hooks/20-ppproxy-ipv6
RUN chmod 755 /usr/lib/dhcpcd/dhcpcd-hooks/20-ppproxy-ipv6
COPY gost ./gost
COPY --from=builder /app/ppproxy/target/x86_64-unknown-linux-musl/release/ppproxy .

ENV TZ=Asia/Taipei
ENV docker=true

CMD ["./ppproxy"]