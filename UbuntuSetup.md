# Running Excel Export on Ubuntu

## Prerequisites

### 1. System Dependencies

```bash
# Update package list
sudo apt update

# Install required dependencies
sudo apt install -y \
postgresql-server-dev-all \
libssl-dev \
pkg-config \
build-essential
```

### 2. Install Rust

```bash
# Install Rust using rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Add Rust to your current shell session
source $HOME/.cargo/env

# Verify installation
rustc --version
cargo --version
```

## Project Setup

### 1. Create Project Directory

```bash
# Create project directory
mkdir -p ~/projects/excel-export
cd ~/projects/excel-export
```

### 2. Create Project Files

Create `Cargo.toml`:
```toml
[package]
name = "cargo-excel-export"
version = "0.1.0"
edition = "2021"

[dependencies]
postgres = "0.19"
rust_xlsxwriter = "0.42"
rayon = "1.7"
num_cpus = "1.16"
sys-info = "0.9"
```

Copy the source code to `src/main.rs`.

### 3. Run the Application

```bash
# Run the executable
./target/release/cargo-excel-export
```

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Verify PostgreSQL is running: `sudo systemctl status postgresql`
   - Check connection string parameters
   - Ensure database permissions are correct

2. **Build Failures**
   - Run `cargo check` for compilation issues
   - Ensure all dependencies are installed
   - Check Rust version compatibility: `rustc --version`

3. **Permission Issues**
   - Ensure proper file permissions: `chmod +x ./target/release/cargo-excel-export`
   - Check database user permissions

## Performance Monitoring

Monitor system resources while running:
```bash
# Install monitoring tools
sudo apt install htop iotop

# Monitor CPU and memory
htop

# Monitor disk I/O
sudo iotop

# Build in release mode
cargo build --release
```
