# syntax=docker/dockerfile:1.7
FROM rust:1.96-bookworm AS builder
WORKDIR /app

ARG CARGO_PROFILE=release
ARG CARGO_BUILD_JOBS=1
ENV CARGO_INCREMENTAL=0 \
    CFLAGS="-g0" \
    CXXFLAGS="-g0"

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY duckdb-query-api ./duckdb-query-api
COPY webhook-ingest ./webhook-ingest
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    if [ "$CARGO_PROFILE" = "release" ]; then \
      cargo build --release -j "$CARGO_BUILD_JOBS" -p duckdb-query-api && build_dir=release; \
    else \
      cargo build -j "$CARGO_BUILD_JOBS" -p duckdb-query-api && build_dir=debug; \
    fi \
    && mkdir -p /out \
    && cp "target/$build_dir/duckdb-query-api" /out/duckdb-query-api

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/duckdb-query-api /usr/local/bin/duckdb-query-api
EXPOSE 8000
ENTRYPOINT ["duckdb-query-api"]
