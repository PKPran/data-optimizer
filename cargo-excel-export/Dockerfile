# Build stage
FROM rust:1.75 as builder

WORKDIR /usr/src/app

# Copy only Cargo.toml first to cache dependencies
COPY Cargo.toml .

# Create dummy source structure for dependency caching
RUN mkdir -p src/db src/excel && \
    echo "fn main() { println!(\"Hello, world!\"); }" > src/main.rs && \
    echo "pub mod db; pub mod excel;" > src/lib.rs && \
    echo "pub fn init() {}" > src/db/mod.rs && \
    echo "pub fn init() {}" > src/excel/mod.rs && \
    cargo build --release

# Remove the dummy source code
RUN rm -rf src

# Copy the real source code
COPY src ./src

# Build the actual application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /usr/local/bin

# Copy the built binary
COPY --from=builder /usr/src/app/target/release/cargo-excel-export .

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip \
    python3-pandas \
    python3-numpy \
    python3-psycopg2 \
    python3-openpyxl \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Create output directory
RUN mkdir -p /usr/local/bin/output

# Make binary executable
RUN chmod +x /usr/local/bin/cargo-excel-export

CMD ["./cargo-excel-export"] 