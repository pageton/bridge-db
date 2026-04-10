#!/usr/bin/env bash
# Cleanup script for Bridge-DB test databases

set -e

SEED_DIR="$PWD/.seed-data"

echo "Cleaning up Bridge-DB test databases..."

# Kill any running processes
pkill -f mysqld || true
pkill -f mariadbd || true
pkill -f mariadbd-safe || true
pkill -f mongod || true
pkill -f redis-server || true
pkill -f postgres || true
pkill -f cockroach || true

# Remove old data directories
rm -rf "$SEED_DIR"

# Clean up sockets
rm -f /tmp/bridge-test-*.sock

echo "Cleanup complete. Run 'nix develop' to start fresh databases."
