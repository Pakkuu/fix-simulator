#!/usr/bin/env bash
# =============================================================================
# tail_and_parse.sh — FIX Log Tail & Parser Trigger
# =============================================================================
# Watches the FIX session log file for new data using `tail -F` (follows
# renames/rotations) and triggers the Python parser on each batch of new lines.
#
# Usage:
#   bash scripts/tail_and_parse.sh [LOG_FILE] [BATCH_INTERVAL_SECS]
#
# Defaults:
#   LOG_FILE            = logs/fix_session.log
#   BATCH_INTERVAL_SECS = 5
#
# The script writes a PID file to prevent duplicate instances.
# Send SIGINT or SIGTERM (Ctrl+C or `kill <pid>`) for graceful shutdown.
#
# Requirements:
#   - uv must be on PATH (or set UV_BIN below)
#   - .env with MySQL credentials must exist in the project root
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Config (override via environment variables or positional args)
# ---------------------------------------------------------------------------

LOG_FILE="${1:-logs/fix_session.log}"
BATCH_INTERVAL="${2:-5}"   # seconds to accumulate lines before triggering parser
TAIL_LOG="logs/tail.log"
PID_FILE="logs/.tail.pid"
UV_BIN="${UV_BIN:-uv}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log() {
    echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') [tail_and_parse] $*" | tee -a "$TAIL_LOG"
}

die() {
    log "ERROR: $*"
    exit 1
}

# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------

mkdir -p logs

# Prevent duplicate instances
if [[ -f "$PID_FILE" ]]; then
    existing_pid=$(cat "$PID_FILE")
    if kill -0 "$existing_pid" 2>/dev/null; then
        die "Already running with PID $existing_pid. Remove $PID_FILE to force restart."
    else
        log "Stale PID file found (PID $existing_pid). Removing and continuing."
        rm -f "$PID_FILE"
    fi
fi

# Write our own PID
echo $$ > "$PID_FILE"

# Check uv is available
command -v "$UV_BIN" >/dev/null 2>&1 || die "'$UV_BIN' not found on PATH. Install uv: https://github.com/astral-sh/uv"

# Check log file exists (wait up to 30s if not yet generated)
wait_secs=0
while [[ ! -f "$LOG_FILE" ]]; do
    if (( wait_secs >= 30 )); then
        die "Log file '$LOG_FILE' not found after 30s. Run the generator first."
    fi
    log "Waiting for log file '$LOG_FILE' … (${wait_secs}s)"
    sleep 2
    (( wait_secs += 2 ))
done

log "Starting tail on: $LOG_FILE (batch interval: ${BATCH_INTERVAL}s)"

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

TAIL_PID=""

cleanup() {
    log "Shutting down …"
    if [[ -n "$TAIL_PID" ]] && kill -0 "$TAIL_PID" 2>/dev/null; then
        kill "$TAIL_PID"
    fi
    rm -f "$PID_FILE"
    log "Stopped. PID file removed."
    exit 0
}

trap cleanup SIGINT SIGTERM EXIT

# ---------------------------------------------------------------------------
# Tail loop
# ---------------------------------------------------------------------------

# Use a temp file to stage newly arrived lines between parser invocations
STAGING_FILE="$(mktemp /tmp/fix_tail_staging.XXXXXX)"
trap 'rm -f "$STAGING_FILE"' EXIT

# Background tail writes new lines to staging file
tail -F "$LOG_FILE" >> "$STAGING_FILE" &
TAIL_PID=$!
log "tail PID: $TAIL_PID"

trigger_count=0

while true; do
    sleep "$BATCH_INTERVAL"

    # Check if there is anything new to process
    if [[ ! -s "$STAGING_FILE" ]]; then
        log "No new data in staging file — waiting …"
        continue
    fi

    # Snapshot and clear the staging file atomically
    SNAP_FILE="$(mktemp /tmp/fix_tail_snap.XXXXXX)"
    cp "$STAGING_FILE" "$SNAP_FILE"
    : > "$STAGING_FILE"   # truncate staging

    line_count=$(wc -l < "$SNAP_FILE" | tr -d ' ')
    log "Triggering parser on $line_count new line(s) …"

    # Append snapshot lines to the end of the main log file so the
    # parser's offset tracking picks them up correctly.
    # NOTE: In real tail mode the parser reads from the live file offset;
    #       the staging file is just a buffer for the trigger decision.
    #
    # Trigger the parser (incremental — reads from last offset)
    if "$UV_BIN" run python src/parser.py; then
        (( trigger_count += 1 ))
        log "Parser run #${trigger_count} succeeded."
    else
        log "WARNING: Parser run #${trigger_count} exited non-zero — check logs."
    fi

    rm -f "$SNAP_FILE"
done
