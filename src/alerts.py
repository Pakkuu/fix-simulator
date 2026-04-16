"""
alerts.py — Slack Webhook Anomaly Alerting
==========================================
Fires Slack alerts when anomalies are detected in the parsed FIX data:
  - Rejection rate spikes
  - Sequence number gaps (dropped messages)
  - Latency outliers (unusual inter-message timing)

Each check function is a placeholder with clear TODOs marking where
production logic needs to be implemented.

Usage (called automatically by parser.py after each batch):
    from src import alerts
    alerts.run_all_checks(cursor)

Direct invocation for testing:
    uv run python src/alerts.py
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("fix.alerts")

# ---------------------------------------------------------------------------
# Severity levels
# ---------------------------------------------------------------------------

SEVERITY_INFO = "info"
SEVERITY_WARNING = "warning"
SEVERITY_CRITICAL = "critical"

# Slack color sidebars per severity
SEVERITY_COLORS = {
    SEVERITY_INFO:     "#36a64f",   # green
    SEVERITY_WARNING:  "#ffa500",   # orange
    SEVERITY_CRITICAL: "#ff0000",   # red
}


# ---------------------------------------------------------------------------
# Slack delivery
# ---------------------------------------------------------------------------

def send_slack_alert(
    title: str,
    message: str,
    severity: str = SEVERITY_WARNING,
    fields: list[dict[str, str]] | None = None,
) -> bool:
    """
    POST a formatted message to the configured Slack incoming webhook.

    Args:
        title:    Short headline for the alert
        message:  Descriptive body text
        severity: One of SEVERITY_INFO | SEVERITY_WARNING | SEVERITY_CRITICAL
        fields:   Optional list of {"title": ..., "value": ..., "short": bool}
                  dicts to display as attachment fields in Slack

    Returns:
        True if the POST succeeded (HTTP 200), False otherwise.

    TODO: Add deduplication — track a dict of (alert_type → last_sent_time) and
          skip re-sending the same alert type within a cooldown window (e.g. 5 min).
    TODO: Add a --dry-run / DRY_RUN env flag that logs the payload without sending.
    TODO: Consider migrating from legacy incoming webhooks to Slack Block Kit for
          richer formatting (buttons, context blocks, etc.).
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url or webhook_url.startswith("https://hooks.slack.com/services/YOUR"):
        log.warning(
            "[alerts] SLACK_WEBHOOK_URL is not configured — skipping alert: %s", title
        )
        return False

    color = SEVERITY_COLORS.get(severity, SEVERITY_COLORS[SEVERITY_WARNING])
    now_iso = datetime.now(tz=timezone.utc).isoformat()

    attachment: dict[str, Any] = {
        "fallback": f"[{severity.upper()}] {title}: {message}",
        "color": color,
        "title": f"[FIX Analyzer] {title}",
        "text": message,
        "footer": "FIX Protocol Log Analyzer",
        "ts": int(datetime.now(tz=timezone.utc).timestamp()),
    }
    if fields:
        attachment["fields"] = fields

    payload = {"attachments": [attachment]}

    try:
        resp = requests.post(webhook_url, json=payload, timeout=5)
        resp.raise_for_status()
        log.info("[alerts] Slack alert sent: %s (%s)", title, severity)
        return True
    except requests.RequestException as exc:
        log.error("[alerts] Failed to send Slack alert: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Anomaly check: Rejection rate
# ---------------------------------------------------------------------------

def check_rejections(cursor: Any) -> None:
    """
    Query recent ExecutionReports and fire an alert if the rejection rate
    exceeds a defined threshold within a lookback window.

    Current state: queries the last N minutes and logs findings.

    TODO: Define REJECTION_RATE_THRESHOLD (e.g., 0.10 = 10% of orders rejected).
    TODO: Define REJECTION_LOOKBACK_MINUTES (e.g., 5 minutes).
    TODO: Implement deduplication so we don't spam Slack on every parse cycle.
    TODO: Break down rejections by symbol to identify if one instrument is problematic.
    TODO: Include the top rejection reason (tag 58 / text) in the alert payload.
    """
    # TODO: Replace hardcoded values with config / env vars
    REJECTION_RATE_THRESHOLD = 0.10       # TODO: tune this
    LOOKBACK_MINUTES = 5                  # TODO: tune this

    since = datetime.now(tz=timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)

    # TODO: Implement this query and the logic below
    query = """
        SELECT
            COUNT(*)                                          AS total,
            SUM(CASE WHEN ord_status = '8' THEN 1 ELSE 0 END) AS rejected
        FROM fix_messages
        WHERE msg_type = '8'
          AND sending_time >= %s
    """

    try:
        cursor.execute(query, (since,))
        row = cursor.fetchone()
    except Exception as exc:
        log.error("[alerts] check_rejections query failed: %s", exc)
        return

    if row is None or row[0] == 0:
        log.debug("[alerts] check_rejections: no execution reports in lookback window")
        return

    total, rejected = row[0], row[1] or 0
    rate = rejected / total if total else 0.0

    log.info(
        "[alerts] Rejection check — total: %d, rejected: %d, rate: %.2f%%",
        total, rejected, rate * 100,
    )

    if rate >= REJECTION_RATE_THRESHOLD:
        send_slack_alert(
            title="High Rejection Rate Detected",
            message=(
                f"Rejection rate is *{rate:.1%}* over the last {LOOKBACK_MINUTES} min "
                f"({rejected}/{total} execution reports)."
            ),
            severity=SEVERITY_WARNING,
            fields=[
                {"title": "Total ExecRpts", "value": str(total), "short": True},
                {"title": "Rejected", "value": str(rejected), "short": True},
                {"title": "Rate", "value": f"{rate:.1%}", "short": True},
                {"title": "Window", "value": f"{LOOKBACK_MINUTES} min", "short": True},
            ],
        )


# ---------------------------------------------------------------------------
# Anomaly check: Sequence gaps
# ---------------------------------------------------------------------------

def check_sequence_gaps(cursor: Any) -> None:
    """
    Detect gaps in MsgSeqNum per SenderCompID by looking for non-consecutive
    sequence numbers in recently inserted messages.

    Current state: uses a window function to find gaps and logs them.

    TODO: Define an acceptable gap tolerance (gaps of 1 within a session are
          always an error; you may want to ignore gaps across session boundaries).
    TODO: Implement a cooldown so repeat alerts for the same gap are suppressed.
    TODO: Optionally trigger a ResendRequest (MsgType=2) to request gap-fill.
    TODO: Persist detected gaps to a separate table for audit purposes.
    """
    # TODO: Replace with config / env var
    LOOKBACK_MINUTES = 10           # TODO: tune this
    MIN_GAP_SIZE = 1                # TODO: tune — gaps > 0 are always anomalous

    since = datetime.now(tz=timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)

    query = """
        SELECT
            sender_comp_id,
            msg_seq_num,
            prev_seq                                   AS prev_seq_num,
            msg_seq_num - prev_seq - 1                 AS gap_size,
            sending_time
        FROM (
            SELECT
                sender_comp_id,
                msg_seq_num,
                LAG(msg_seq_num) OVER (
                    PARTITION BY sender_comp_id
                    ORDER BY msg_seq_num
                ) AS prev_seq,
                sending_time
            FROM fix_messages
            WHERE sending_time >= %s
              AND msg_type NOT IN ('A', '5')
        ) sub
        WHERE prev_seq IS NOT NULL
          AND msg_seq_num - prev_seq - 1 > %s
        ORDER BY sending_time DESC
        LIMIT 20
    """

    try:
        cursor.execute(query, (since, MIN_GAP_SIZE - 1))
        gaps = cursor.fetchall()
    except Exception as exc:
        log.error("[alerts] check_sequence_gaps query failed: %s", exc)
        return

    if not gaps:
        log.debug("[alerts] check_sequence_gaps: no gaps detected")
        return

    log.warning("[alerts] Sequence gaps detected: %d gap(s)", len(gaps))

    gap_details = "\n".join(
        f"• {g[0]}: seq {g[1]} (prev {g[2]}, gap={g[3]}) @ {g[4]}"
        for g in gaps
    )

    send_slack_alert(
        title="FIX Sequence Gap(s) Detected",
        message=(
            f"*{len(gaps)} sequence gap(s)* found in the last {LOOKBACK_MINUTES} min:\n"
            f"```{gap_details}```"
        ),
        severity=SEVERITY_CRITICAL,
        fields=[
            {"title": "Gap Count", "value": str(len(gaps)), "short": True},
            {"title": "Window", "value": f"{LOOKBACK_MINUTES} min", "short": True},
        ],
    )


# ---------------------------------------------------------------------------
# Anomaly check: Latency outliers
# ---------------------------------------------------------------------------

def check_latency_outliers(cursor: Any) -> None:
    """
    Detect unusual gaps between consecutive message SendingTimes (tag 52),
    which may indicate network latency spikes or processing bottlenecks.

    Current state: fetches recent inter-message deltas and computes basic stats.

    TODO: Define a latency baseline using a rolling average (e.g., 30-day P99).
    TODO: Define an outlier threshold — e.g., deltas > mean + 3σ (3-sigma rule).
    TODO: Implement a sliding window so we don't re-alert for the same spike.
    TODO: Correlate latency spikes with high-volume symbols to find hot paths.
    TODO: Integrate with a time-series store (e.g., Prometheus/InfluxDB) for
          proper latency tracking beyond what MySQL can offer long-term.
    """
    # TODO: Replace with config / env var
    LOOKBACK_MINUTES = 5            # TODO: tune this
    LATENCY_THRESHOLD_MS = 1000     # TODO: tune — alert if any delta > 1s

    since = datetime.now(tz=timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)

    query = """
        SELECT
            TIMESTAMPDIFF(MICROSECOND,
                LAG(sending_time) OVER (ORDER BY sending_time),
                sending_time
            ) / 1000.0   AS delta_ms,
            sending_time,
            msg_type,
            symbol,
            msg_seq_num
        FROM fix_messages
        WHERE sending_time >= %s
          AND msg_type NOT IN ('A', '5')
        ORDER BY sending_time
    """

    try:
        cursor.execute(query, (since,))
        rows = cursor.fetchall()
    except Exception as exc:
        log.error("[alerts] check_latency_outliers query failed: %s", exc)
        return

    if not rows:
        log.debug("[alerts] check_latency_outliers: no messages in lookback window")
        return

    deltas = [r[0] for r in rows if r[0] is not None]
    if not deltas:
        return

    avg_ms = sum(deltas) / len(deltas)
    max_ms = max(deltas)

    log.info(
        "[alerts] Latency check — %d deltas, avg: %.2fms, max: %.2fms",
        len(deltas), avg_ms, max_ms,
    )

    # TODO: Replace simple max check with proper statistical outlier detection
    if max_ms > LATENCY_THRESHOLD_MS:
        # Find the worst offender
        worst = max(rows, key=lambda r: r[0] if r[0] is not None else 0)

        send_slack_alert(
            title="FIX Latency Outlier Detected",
            message=(
                f"Max inter-message delta is *{max_ms:.0f}ms* "
                f"(avg: {avg_ms:.0f}ms, threshold: {LATENCY_THRESHOLD_MS}ms)\n"
                f"Worst at seq {worst[4]} ({worst[2]}) @ {worst[1]}"
            ),
            severity=SEVERITY_WARNING,
            fields=[
                {"title": "Max Delta", "value": f"{max_ms:.0f}ms", "short": True},
                {"title": "Avg Delta", "value": f"{avg_ms:.0f}ms", "short": True},
                {"title": "Threshold", "value": f"{LATENCY_THRESHOLD_MS}ms", "short": True},
                {"title": "Samples", "value": str(len(deltas)), "short": True},
            ],
        )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_all_checks(cursor: Any) -> None:
    """
    Run all anomaly checks in sequence. Called by parser.py after each batch.
    Each check is isolated — a failure in one will not abort the others.

    TODO: Add a CHECK_INTERVAL_SECONDS env var to avoid re-running checks on
          every tiny incremental parse (e.g., only run every 60 seconds).
    TODO: Add a check for HeartBeat gaps (missing tag 0 / MsgType=0 messages
          beyond the configured HeartBtInt from the Logon message).
    TODO: Add a check for duplicate ClOrdIDs across sessions.
    """
    log.info("[alerts] Running anomaly checks …")

    check_rejections(cursor)
    check_sequence_gaps(cursor)
    check_latency_outliers(cursor)

    log.info("[alerts] Anomaly checks complete.")


# ---------------------------------------------------------------------------
# Direct invocation for testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.DEBUG)

    # TODO: For standalone testing, connect to MySQL and run checks directly.
    #       Right now this just validates the Slack webhook is reachable.
    print("Testing Slack webhook connection …")
    ok = send_slack_alert(
        title="FIX Analyzer — Test Alert",
        message="This is a test alert from `alerts.py`. If you see this, your webhook is configured correctly.",
        severity=SEVERITY_INFO,
    )
    if ok:
        print("✓ Slack alert sent successfully.")
    else:
        print("✗ Slack alert failed — check SLACK_WEBHOOK_URL in your .env file.")
        sys.exit(1)
