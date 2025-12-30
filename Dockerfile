# Build stage - using nightly for edition2024 support
FROM rustlang/rust:nightly-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Configure git and build with token for private repos
# GitHub App tokens require x-access-token format
RUN --mount=type=secret,id=GIT_AUTH_TOKEN \
    git config --global url."https://x-access-token:$(cat /run/secrets/GIT_AUTH_TOKEN)@github.com/".insteadOf "https://github.com/" && \
    CARGO_NET_GIT_FETCH_WITH_CLI=true \
    cargo build --release

# Runtime stage - using same base for glibc compatibility
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /app/target/release/scraping_service /usr/local/bin/scraping_service

# Copy config file (can be overridden with volume mount)
COPY config.json ./config.json

# Create data and log directories
RUN mkdir -p /app/data /app/logs

# Set environment variables
ENV RUST_LOG=info

# Run the application
CMD ["scraping_service"]
