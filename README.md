# fix-simulator

A FIX 4.4 session simulator for SPY equity trading. Generates a realistic stream of outbound orders and inbound execution reports, writing raw FIX messages to a remote log file over SSH.

## What it does

Simulates a full order lifecycle every 3 seconds:

- **New Order Single** (D) — market and limit orders
- **Cancel Request** (F) — cancel open orders
- **Cancel/Replace** (G) — modify price or quantity
- **Execution Report** (8) — acks, partial fills, fills, rejections
- **Order Cancel Reject** (9) — cancel too late

Each message is appended as raw FIX bytes to `fix-analyzer:/home/ubuntu/fix-analyzer/logs/fix-session.log` via SFTP. The simulator will not run if the remote connection cannot be established.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- SSH access to the `fix-analyzer` EC2 host (configured in `~/.ssh/config`)

## Setup

```bash
uv sync
```

## Run

```bash
uv run session_sim.py
```

Example output:

```
FIX 4.4 SPY Session Simulator
  Writing to fix-analyzer:/home/ubuntu/fix-analyzer/logs/fix-session.log

  00:13:52.959 UTC  →  NEW ORDER SINGLE  [seq 1]
  00:13:55.964 UTC  ←  EXEC REPORT  [ACK]  [seq 2]
  00:13:58.969 UTC  →  CANCEL/REPLACE  [seq 3]
  00:14:01.971 UTC  ←  EXEC REPORT  [FILL]  [seq 4]
```

## Configuration

Edit the constants at the top of `session_sim.py`:

| Variable | Default | Description |
|---|---|---|
| `REMOTE_HOST` | `fix-analyzer` | SSH config alias for the EC2 host |
| `REMOTE_LOG_PATH` | `/home/ubuntu/fix-analyzer/logs/fix-session.log` | Destination log file |
| `INTERVAL` | `3` | Seconds between messages |
| `SYMBOL` | `SPY` | Equity symbol to simulate |
