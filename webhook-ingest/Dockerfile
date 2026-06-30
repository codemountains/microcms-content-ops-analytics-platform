# syntax=docker/dockerfile:1.7
FROM rust:1.96-bookworm AS builder
WORKDIR /app

ARG CARGO_PROFILE=release
ARG CARGO_BUILD_JOBS=1
ENV CARGO_INCREMENTAL=0

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY webhook-ingest ./webhook-ingest
COPY duckdb-query-api ./duckdb-query-api
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    if [ "$CARGO_PROFILE" = "release" ]; then \
      cargo build --release -j "$CARGO_BUILD_JOBS" -p webhook-ingest && build_dir=release; \
    else \
      cargo build -j "$CARGO_BUILD_JOBS" -p webhook-ingest && build_dir=debug; \
    fi \
    && mkdir -p /out \
    && cp "target/$build_dir/webhook-ingest" /out/webhook-ingest

FROM public.ecr.aws/lambda/provided:al2023
COPY --from=builder /out/webhook-ingest /var/runtime/bootstrap
CMD ["bootstrap"]
