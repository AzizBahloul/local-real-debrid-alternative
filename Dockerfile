# syntax=docker/dockerfile:1

# --- Build stage -------------------------------------------------------
FROM rust:1-slim-bookworm AS builder

# build-essential + cmake + perl: needed to compile rustls's crypto backend
# (aws-lc-rs/ring, small amounts of C/assembly). No libssl-dev/pkg-config --
# this project deliberately avoids OpenSSL (see Cargo.toml's `rust-tls`
# feature on librqbit), so there is no system TLS library to install.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        pkg-config \
        perl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependency compilation separately from source changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs \
    && cargo build --release --locked || true

COPY src ./src
COPY tests ./tests
# Force a rebuild of our own crate now that real sources are in place (the
# dummy main.rs/lib.rs above only exists to pre-warm the dependency cache).
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

# --- Runtime stage -------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin gateway

COPY --from=builder /build/target/release/streaming-gateway /usr/local/bin/streaming-gateway

ENV CACHE_DIRECTORY=/data \
    GATEWAY_PORT=11470 \
    GATEWAY_FALLBACK_PORT=8080 \
    GATEWAY_BIND_ADDR=0.0.0.0 \
    RUST_LOG=info

RUN mkdir -p /data && chown -R gateway:gateway /data
VOLUME ["/data"]
USER gateway

EXPOSE 11470 8080

# NOTE: the LAN-IP banner printed at startup reflects whatever IP this
# process sees -- inside a container on the default bridge network, that is
# the container's internal IP, not the host's. If you publish ports
# (`-p 11470:11470`) rather than running with `--network host`, use the
# *host machine's* LAN IP (e.g. `hostname -I`) when configuring Stremio or a
# phone, not the address the container prints. See README.md.
ENTRYPOINT ["/usr/local/bin/streaming-gateway"]
