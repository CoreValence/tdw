# syntax=docker/dockerfile:1.7
#
# Two-stage build:
#   1. `builder` compiles the pgrx extension against system-installed pg18 dev
#      headers and drops beetle.so + control + sql into Debian's standard
#      postgres install paths.
#   2. `runtime` is stock postgres:18-bookworm with those three files copied in,
#      so the extension is loadable via shared_preload_libraries=beetle.

FROM rust:1-bookworm AS builder

# Postgres 18 dev headers from PGDG, plus clang for bindgen.
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates gnupg lsb-release build-essential \
        libssl-dev libclang-dev pkg-config clang git \
  && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
     | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg \
  && echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
     > /etc/apt/sources.list.d/pgdg.list \
  && apt-get update && apt-get install -y --no-install-recommends \
        postgresql-server-dev-18

ENV PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config

# pgrx cli pinned to match [dependencies].
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo install --locked --version =0.18.0 cargo-pgrx

# Register the system pg18 install with pgrx; skip the managed-postgres build.
RUN cargo pgrx init --pg18 "$PG_CONFIG"

WORKDIR /build
COPY Cargo.toml Cargo.lock beetle.control ./
COPY .cargo ./.cargo
COPY src ./src

# Release build → baked into postgres:18-bookworm in the runtime stage.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo pgrx install --release --pg-config "$PG_CONFIG"


FROM postgres:18-bookworm AS runtime

COPY --from=builder /usr/lib/postgresql/18/lib/beetle.so \
                    /usr/lib/postgresql/18/lib/beetle.so
COPY --from=builder /usr/share/postgresql/18/extension/beetle.control \
                    /usr/share/postgresql/18/extension/beetle.control
COPY --from=builder /usr/share/postgresql/18/extension/beetle--0.1.0.sql \
                    /usr/share/postgresql/18/extension/beetle--0.1.0.sql
