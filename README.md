# Copy Postgres tables between databases

[![Zig Version](https://img.shields.io/badge/Zig-0.13.0+-blue.svg)](https://ziglang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Postgres](https://img.shields.io/badge/Postgres-14+-blue.svg)](https://www.postgresql.org)

`pg_transfer` copies Postgres tables between databases quickly by splitting the work across multiple workers. Each worker handles a different part of the table using CTID ranges, streaming data directly.

## Performance Comparison

Benchmark results comparing pg_transfer with pg_dump+psql pipeline for transferring pgbench_accounts table:

**pg_dump+psql command used for comparison:**
```bash
pg_dump "$SOURCE_DB" -t pgbench_accounts --data-only | psql "$DEST_DB"
```

### 10M rows (1GB data) - pgbench with scaling factor 100
| Tool | Real Time | User Time | Sys Time | Rows Transferred | Improvement |
|------|-----------|-----------|----------|------------------|------------|
| pg_transfer (4 workers) | 11.1s | 1.7s | 0.7s | 10,000,000 | - |
| pg_dump+psql | 13.9s | 2.6s | 1.4s | 10,000,000 | +25% slower |

### 20M rows (2GB data) - pgbench with scaling factor 200
| Tool | Real Time | User Time | Sys Time | Rows Transferred | Improvement |
|------|-----------|-----------|----------|------------------|------------|
| pg_transfer (4 workers) | 22.8s | 3.2s | 1.4s | 20,000,000 | - |
| pg_dump+psql | 30.1s | 4.8s | 2.8s | 20,000,000 | +32% slower |

### 30M rows (3GB data) - pgbench with scaling factor 300
| Tool | Real Time | User Time | Sys Time | Rows Transferred | Improvement |
|------|-----------|-----------|----------|------------------|------------|
| pg_transfer (4 workers) | 31.1s | 4.9s | 2.0s | 30,000,000 | - |
| pg_dump+psql | 45.6s | 8.2s | 3.9s | 30,000,000 | +47% slower |

pg_transfer shows significantly faster real time and lower CPU usage compared to traditional pg_dump+psql pipeline. The performance advantage increases with larger tables due to better parallelization and reduced serialization overhead.

## Installation

### Quick Start
```bash
# Clone the repository
git clone https://github.com/abc3/pg_transfer.git
cd pg_transfer

# Build the tool
make build

# Check possible arguments
./zig-out/bin/pg_transfer -h
```

## Usage

### Prerequisites

Before transferring data, you need to copy the table schema to the destination database:

```bash
# Copy table schema (structure) to destination database
pg_dump "$SOURCE_DB" -t pgbench_accounts --schema-only | psql "$DEST_DB"
```

**Note:** pg_transfer only transfers data, not table structure. The destination table must exist before running the transfer.

### Basic Usage
```bash
# Simple table copy with default settings
./zig-out/bin/pg_transfer \
  --source "postgresql://user:pass@source:5432/db" \
  --destination "postgresql://user:pass@dest:5432/db" \
  --table public.large_table

# Copy specific table with custom workers
./zig-out/bin/pg_transfer \
  --source "postgresql://user:pass@source:5432/db" \
  --destination "postgresql://user:pass@dest:5432/db" \
  --table public.massive_table \
  --workers 16 \
  --buffer 2
```

### Advanced Configuration
```bash
# Production-grade configuration for massive tables
./zig-out/bin/pg_transfer \
  --source "postgresql://prod:pass@prod-db:5432/production" \
  --destination "postgresql://staging:pass@staging-db:5432/staging" \
  --table public.user_transactions \
  --workers 32 \
  --buffer 4
```

## Configuration Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--source` | string | Required | Source database connection string |
| `--destination` | string | Required | Destination database connection string |
| `--table` | string | Required | Table name to transfer |
| `--workers` | u32 | CPU count | Number of parallel worker threads |
| `--buffer` | u32 | 1 | Batch buffer size in MB |

## Development

### Setup Test Environment
```bash
# Setup 'public.pgbench_accounts' table on the source database
pgbench -i -s 100 postgresql://postgres:postgres@127.0.0.1:5432

# Note: pg_transfer does not create schemas or tables
# If the destination table doesn't exist, create it first:
pg_dump "postgresql://postgres:postgres@127.0.0.1:5432/postgres" -t pgbench_accounts --no-owner --no-privileges --data-only | psql "postgresql://postgres:postgres@127.0.0.1:6432/postgres"
```
