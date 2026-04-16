# FIX 4.4 Protocol Log Analyzer

A FIX 4.4 session log generation, parsing, and monitoring system built for Ubuntu EC2 (t2.micro). Uses **UV** for Python package management.

## Project Structure

```
.
├── pyproject.toml              # UV-managed project & dependencies
├── .env.example                # Environment variable template
├── schema/
│   └── init.sql                # MySQL schema + indexes + views
├── src/
│   ├── generator.py            # FIX 4.4 session log generator
│   ├── parser.py               # Log parser → MySQL ingester
│   └── alerts.py               # Slack anomaly alerting module
├── scripts/
│   └── tail_and_parse.sh       # Tail log + trigger parser in real-time
└── logs/                       # Generated at runtime (gitignored)
    ├── fix_session.log
    ├── tail.log
    ├── .parser_offset
    └── .tail.pid
```

---

## Prerequisites (EC2 Ubuntu)

### 1. Install UV

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env
```

### 2. Install MySQL 8.0

```bash
sudo apt update && sudo apt install -y mysql-server
sudo systemctl start mysql
sudo mysql_secure_installation
```

### 3. Create MySQL user & database

```sql
-- Run as root: sudo mysql
CREATE DATABASE fix_analyzer CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'fix_user'@'127.0.0.1' IDENTIFIED BY 'changeme';
GRANT ALL PRIVILEGES ON fix_analyzer.* TO 'fix_user'@'127.0.0.1';
FLUSH PRIVILEGES;
EXIT;
```

---

## Setup

### 1. Clone & configure

```bash
git clone <your-repo-url>
cd FIX-Protocol-Log-Analyzer

cp .env.example .env
# Edit .env with your MySQL credentials and Slack webhook URL
```

### 2. Install Python dependencies with UV

```bash
uv sync
```

### 3. Apply MySQL schema

```bash
mysql -u fix_user -p fix_analyzer < schema/init.sql
```

---

## Usage

### Step 1 — Generate a FIX session log

```bash
uv run python src/generator.py
# Options:
#   --messages 500          Number of NewOrderSingle messages (default: 200)
#   --seed 42               RNG seed for reproducibility (default: 42)
#   --gaps 3                Number of sequence gaps to inject (default: 3)
#   --output logs/my.log    Custom output file

uv run python src/generator.py --messages 500 --seed 99
```

This produces `logs/fix_session.log` containing:
- Logon / Logout
- NewOrderSingle (D) across AAPL, MSFT, TSLA, AMZN, GOOG
- ExecutionReports (8): full fills, partial fills, rejections
- OrderCancelRejects (9)
- Deliberate sequence number gaps

### Step 2 — Parse the log into MySQL

```bash
# Full parse (first run)
uv run python src/parser.py --full

# Incremental parse (only new lines since last run)
uv run python src/parser.py
```

**Verify records:**

```sql
-- Connect: mysql -u fix_user -p fix_analyzer

SELECT msg_type_name, COUNT(*) AS cnt
FROM fix_messages
GROUP BY msg_type_name
ORDER BY cnt DESC;

-- Check rejections
SELECT * FROM v_rejections LIMIT 20;

-- Check sequence gaps
SELECT * FROM v_sequence_gaps;
```

### Step 3 — Real-time tail mode

In one terminal, run the tail watcher:

```bash
bash scripts/tail_and_parse.sh
# Optional: bash scripts/tail_and_parse.sh logs/fix_session.log 10
```

In another terminal, append new messages to simulate live traffic:

```bash
# Generate additional messages and append
uv run python src/generator.py --seed 123 >> logs/fix_session.log
```

Stop the watcher with `Ctrl+C`.

### Step 4 — Test Slack alerts

```bash
# Update SLACK_WEBHOOK_URL in .env first
uv run python src/alerts.py
```

---

## Anomaly Alerts (`src/alerts.py`)

Three anomaly detectors are called automatically after each parser batch:

| Check | Trigger | Severity |
|---|---|---|
| `check_rejections()` | Rejection rate ≥ threshold in lookback window | ⚠️ Warning |
| `check_sequence_gaps()` | Non-consecutive MsgSeqNum detected | 🔴 Critical |
| `check_latency_outliers()` | Inter-message delta > threshold | ⚠️ Warning |

Each function contains **`# TODO`** comments marking where production thresholds, deduplication, and statistical logic need to be implemented.

---

## MySQL Schema Highlights

| Index | Purpose |
|---|---|
| `idx_symbol` | Symbol-level price/order queries |
| `idx_sending_time` | Time-range analytics |
| `idx_msg_type` | Fast MsgType filtering |
| `idx_seq_gap (sender, seq)` | Sequence gap detection |
| `idx_cl_ord_id` | Order lifecycle join (D→8→9) |
| `idx_symbol_time` | Combined symbol+time scans |
| `idx_type_status` | Rejection rate queries |

Two built-in views: `v_rejections` and `v_sequence_gaps`.

---

## Development

```bash
# Run linting
uv run ruff check src/

# Run type checks
uv run mypy src/

# Format
uv run ruff format src/
```

Add `ruff` and `mypy` to `[dependency-groups]` in `pyproject.toml` as dev dependencies:

```bash
uv add --dev ruff mypy
```
