# syntax=docker/dockerfile:1.7
#
# Two-stage build:
#   1. `builder` compiles the pgrx extension against system-installed pg18 dev
#      headers and drops tbw.so + control + sql into Debian's standard
#      postgres install paths.
#   2. `runtime` is stock postgres:18-bookworm with those three files copied in,
#      so the extension is loadable via shared_preload_libraries=tbw.

FROM rust:1-bookworm AS builder

ARG TARGETARCH

# Postgres 18 dev headers from PGDG, plus clang for bindgen. Cache mounts are
# partitioned by target arch so parallel amd64/arm64 builds don't fight over
# /var/cache/apt or /usr/local/cargo/registry locks.
RUN --mount=type=cache,target=/var/cache/apt,id=apt-${TARGETARCH} \
    --mount=type=cache,target=/var/lib/apt,id=apt-lib-${TARGETARCH} \
    apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates gnupg lsb-release build-essential \
        libssl-dev libclang-dev pkg-config clang git xz-utils \
  && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
     | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg \
  && echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
     > /etc/apt/sources.list.d/pgdg.list \
  && apt-get update && apt-get install -y --no-install-recommends \
        postgresql-server-dev-18

ENV PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config

# Install Zig 0.14.1 and export ZIG_PATH so tigerbeetle-unofficial-sys's
# build.rs uses it directly instead of trying to download Zig itself (the
# auto-download path fails in network-restricted build sandboxes).
ARG ZIG_VERSION=0.14.1
RUN set -eux; \
    case "${TARGETARCH}" in \
        amd64)  ZIG_ARCH=x86_64 ;; \
        arm64)  ZIG_ARCH=aarch64 ;; \
        *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz" \
        | tar -xJ -C /opt; \
    mv "/opt/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}" /opt/zig
ENV ZIG_PATH=/opt/zig/zig
ENV PATH=/opt/zig:$PATH

# pgrx cli pinned to match [dependencies].
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-${TARGETARCH} \
    cargo install --locked --version =0.18.0 cargo-pgrx

# Register the system pg18 install with pgrx; skip the managed-postgres build.
RUN cargo pgrx init --pg18 "$PG_CONFIG"

WORKDIR /build
COPY Cargo.toml Cargo.lock tbw.control ./
COPY .cargo ./.cargo
COPY src ./src

# On x86_64, Zig's libtb_client.a has initial-exec TLS relocations
# (R_X86_64_TPOFF32) that cannot be linked into a -shared cdylib. Patch
# the sys crate to link libtb_client.so dynamically and add $ORIGIN rpath
# so the loader finds it next to tbw.so. aarch64 accepts the equivalent
# TLS relocations from the static archive and is unaffected.
#
# Everything runs in a single RUN so the sed patch, the $ORIGIN-linked
# build, and the libtb_client.so extraction all see the same /build/target
# cache mount. Artifacts are copied into /export/ (persisted in the image
# layer) for the runtime stage to pick up.
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-${TARGETARCH} \
    --mount=type=cache,target=/build/target,id=target-${TARGETARCH} \
    set -eux; \
    cargo fetch; \
    RUSTFLAGS=""; \
    if [ "$(uname -m)" = "x86_64" ]; then \
        CRATE_DIR=$(find /usr/local/cargo/registry/src -maxdepth 2 -type d \
            -name 'tigerbeetle-unofficial-sys-*' | head -1); \
        test -n "$CRATE_DIR"; \
        sed -i 's|cargo:rustc-link-lib=static=tb_client|cargo:rustc-link-lib=dylib=tb_client|' \
            "$CRATE_DIR/build.rs"; \
        RUSTFLAGS='-C link-arg=-Wl,-rpath,$ORIGIN'; \
    fi; \
    RUSTFLAGS="$RUSTFLAGS" cargo pgrx install --release --pg-config "$PG_CONFIG"; \
    mkdir -p /export/lib /export/ext; \
    cp /usr/lib/postgresql/18/lib/tbw.so /export/lib/; \
    cp /usr/share/postgresql/18/extension/tbw.control /export/ext/; \
    cp /usr/share/postgresql/18/extension/tbw--*.sql /export/ext/; \
    if [ "$(uname -m)" = "x86_64" ]; then \
        TB_SO=$(find target/release/build -name 'libtb_client.so' \
            -path '*x86_64-linux-gnu*' | head -1); \
        test -n "$TB_SO"; \
        cp "$TB_SO" /export/lib/; \
    fi


FROM postgres:18-bookworm AS runtime

COPY --from=builder /export/lib/ /usr/lib/postgresql/18/lib/
COPY --from=builder /export/ext/ /usr/share/postgresql/18/extension/
