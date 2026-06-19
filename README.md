# CS3211 Assignment 2 — Software Transactional Memory (STM)

## Overview

This project implements a **Software Transactional Memory (STM)** server in Go. Clients connect over a Unix domain socket and submit transactions consisting of read and write operations on a shared 256-address memory space. The server guarantees **serialisable isolation** using an optimistic concurrency control (OCC) protocol with an actor-per-address model.

## Design

### Actor Model

Each of the 256 memory addresses is managed by a dedicated goroutine (`Actor`). All access to a memory location goes through that actor's channel, eliminating shared-state races without explicit mutexes.

### Transaction Protocol (OCC — 6 Phases)

For every transaction the server executes:

| Phase | Description |
|-------|-------------|
| 1 | **Read** — speculatively read all addresses in the read set, recording their versions |
| 2 | **Build write set** — collect all write operations |
| 3 | **Lock** — acquire locks on all write-set actors (sorted by address to prevent deadlock) |
| 4 | **Validate** — confirm read-set versions are still current |
| 5 | **Commit** — apply writes and increment version counters |
| 6 | **Unlock** — release all locks |

If locking or validation fails the transaction retries from phase 1 (active retry / spin).

### Global Clock

A dedicated goroutine provides a monotonically increasing `int64` timestamp via `clockChan`. Each committed transaction is stamped with a unique commit time that is returned to the client.

## Project Structure

```
submission/stminterface.go  # STM implementation (the only file to modify)
main/main.go                # Entry point — sets up Unix socket listener
utils/io.go                 # I/O helpers (read/print)
wg/waitgroup.go             # Custom WaitGroup wrapper
cpp-src/client.cpp          # Reference C++ test client
scripts/run_tests.sh        # Batch test runner
scripts/input/              # Test input files
scripts/output/             # Test output files (generated)
grader / grader-arm64       # Pre-built grader binaries
```

## Building

Requires **Go 1.24+** and **clang++** (C++20).

```bash
# Build both the STM server and the C++ client
make

# Build only the server
make stminterface

# Build only the client
make client

# Clean build artefacts
make clean
```

## Running

```bash
# Start the STM server (listens on a Unix socket)
./stminterface <socket_path>

# Example
./stminterface /tmp/stm.sock
```

## Testing

```bash
# Run all tests against the grader (from the scripts/ directory)
cd scripts
bash run_tests.sh
```

Results are written to `scripts/output/<test-name>.out`. Each output file ends with either `test passed` or `test failed`.

Individual tests can be run manually:

```bash
./grader stminterface < scripts/input/<test>.in
```

## Key Constants

| Constant | Value | Meaning |
|----------|-------|---------|
| `NumActors` | 256 | Number of distinct memory addresses |

## Authors

- A0276238W  
- A0286456N
