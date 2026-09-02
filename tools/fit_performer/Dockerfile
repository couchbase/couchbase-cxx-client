# syntax=docker/dockerfile:1
#
# Multi-stage build for the Couchbase C++ SDK FIT performer.
#
# Stage 1 ("builder") compiles tools/fit_performer in Release mode. To keep the
# final image down to "just the binary", we deliberately do NOT install the
# system gRPC/protobuf/OpenSSL development packages: their absence makes the
# project's find_package() calls fail, so gRPC, protobuf, abseil, c-ares,
# OpenTelemetry and curl are fetched and built statically from source, and
# COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL=ON statically links BoringSSL. The
# resulting executable depends only on the C/C++ runtime and zlib, all of which
# the ubuntu:24.04 runtime base already provides.
#
# Build context MUST be the repository root, e.g.:
#   docker build -f tools/fit_performer/Dockerfile -t cxx-fit-performer .
#
# Everything (BoringSSL, the Mozilla CA bundle, gRPC, OpenTelemetry, ...) is
# downloaded at CMake configure time, so the build needs network access.

ARG UBUNTU_VERSION=24.04

# ---------------------------------------------------------------------------
# Stage 1: build
# ---------------------------------------------------------------------------
FROM ubuntu:${UBUNTU_VERSION} AS builder

ENV DEBIAN_FRONTEND=noninteractive

# build-essential -> gcc/g++/make, cmake+ninja-build -> generator,
# git -> CPM dependency checkout, curl/ca-certificates -> CMake's file(DOWNLOAD)
# of the Mozilla CA bundle over TLS, zlib1g-dev -> system zlib that gRPC links
# against on Linux, pkg-config -> dependency probing. libssl-dev is required by
# OpenTelemetry's bundled curl, which runs its own find_package(OpenSSL); the
# SDK core itself still uses static BoringSSL (forced by STATIC_BORINGSSL=ON),
# so only curl links the system OpenSSL.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        git \
        curl \
        ca-certificates \
        pkg-config \
        zlib1g-dev \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy the whole repository (the performer links the full C++ client, which
# lives in core/ and couchbase/, and uses helpers from cmake/).
COPY . .

# Configure a Release build of only the performer and its dependencies.
# Tools are required (fit_performer lives under tools/); tests, examples and
# docs are disabled to avoid building anything we will not ship. Static client
# library + static BoringSSL + (implicitly) source-built gRPC/protobuf/curl
# yield a self-contained binary.
RUN cmake -S . -B build -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCOUCHBASE_CXX_CLIENT_BUILD_TOOLS=ON \
        -DCOUCHBASE_CXX_CLIENT_BUILD_TESTS=OFF \
        -DCOUCHBASE_CXX_CLIENT_BUILD_EXAMPLES=OFF \
        -DCOUCHBASE_CXX_CLIENT_BUILD_DOCS=OFF \
        -DCOUCHBASE_CXX_CLIENT_BUILD_SHARED=OFF \
        -DCOUCHBASE_CXX_CLIENT_BUILD_STATIC=ON \
        -DCOUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY=ON \
        -DCOUCHBASE_CXX_CLIENT_STATIC_BORINGSSL=ON \
    && cmake --build build --target fit_performer -j"$(nproc)" \
    && strip build/tools/fit_performer/fit_performer

# Fail the build early if the binary depends on shared libraries the runtime
# image will not have. Allowed: the C/C++ runtime, zlib, the dynamic loader, and
# system OpenSSL (libssl/libcrypto, pulled in transitively via OpenTelemetry's
# curl). This keeps the "only the binary" promise honest.
RUN ldd build/tools/fit_performer/fit_performer || true; \
    if ldd build/tools/fit_performer/fit_performer \
         | grep -Eiv 'libstdc\+\+|libc\.so|libm\.so|libgcc_s|libz\.so|libssl|libcrypto|libdl|librt|libpthread|ld-linux|linux-vdso|not a dynamic' \
         | grep -q '=>'; then \
      echo "ERROR: unexpected shared library dependency (see ldd output above)"; \
      ldd build/tools/fit_performer/fit_performer; \
      exit 1; \
    fi

# ---------------------------------------------------------------------------
# Stage 2: runtime
# ---------------------------------------------------------------------------
FROM ubuntu:${UBUNTU_VERSION} AS runtime

# Passed by couchbaselabs/sdk-docker-build-action (build-arg SDK=...). Declared
# so the build arg is consumed cleanly and recorded as a label.
ARG SDK=cxx

LABEL org.opencontainers.image.title="${SDK}-fit-performer" \
      org.opencontainers.image.description="Couchbase C++ SDK FIT performer (gRPC server)"

ENV DEBIAN_FRONTEND=noninteractive

# ca-certificates: general TLS trust store. libssl3 is needed because
# OpenTelemetry's curl links system OpenSSL. libstdc++6, libgcc-s1 and zlib1g are
# already present in the ubuntu base; install explicitly to be robust against
# slim variants and to make the runtime dependencies self-documenting.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3t64 \
        libstdc++6 \
        zlib1g \
    && rm -rf /var/lib/apt/lists/*

# Run as an unprivileged user.
RUN useradd --create-home --uid 10001 performer
USER performer

# Copy ONLY the performer binary from the build stage.
COPY --from=builder /src/build/tools/fit_performer/fit_performer /usr/local/bin/fit_performer

# The performer is a gRPC server listening on 0.0.0.0:8060 by default. The first
# CLI argument overrides the port; the second sets the log level (also settable
# via the LOG_LEVEL environment variable).
EXPOSE 8060

ENTRYPOINT ["/usr/local/bin/fit_performer"]
