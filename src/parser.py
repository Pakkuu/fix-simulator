"""
parser.py — FIX 4.4 Log Parser & MySQL Ingester
================================================
Reads a FIX 4.4 session log file (pipe-delimited), decodes tag/value pairs,
and inserts structured records into the fix_messages MySQL table.

Features:
  - Incremental parsing via byte-offset tracking (.parser_offset file)
  - Batch inserts with executemany for performance
  - Graceful handling of malformed lines (warns and skips)
  - Triggers anomaly checks from alerts.py after each batch
  - Supports --full flag to re-parse from the beginning

Usage:
    uv run python src/parser.py               # incremental (default)
    uv run python src/parser.py --full        # re-parse from start
    uv run python src/parser.py --file logs/other.log
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import mysql.connector
from dotenv import load_dotenv

# alerts is imported lazily inside run_anomaly_checks() so missing deps
# don't break the parser if alerts.py can't connect to Slack.

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("fix.parser")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

LOG_DIR = Path("logs")
DEFAULT_LOG_FILE = LOG_DIR / "fix_session.log"
OFFSET_FILE = LOG_DIR / ".parser_offset"
BATCH_SIZE = 500  # rows per executemany call

# FIX tag → column name mapping
# Tags not listed here are ignored (still available in raw_message)
TAG_MAP = {
    35:  "msg_type",
    49:  "sender_comp_id",
    56:  "target_comp_id",
    34:  "msg_seq_num",
    52:  "sending_time",
    55:  "symbol",
    54:  "side",
    38:  "order_qty",
    44:  "price",
    6:   "avg_px",
    14:  "cum_qty",
    151: "leaves_qty",
    39:  "ord_status",
    150: "exec_type",
    11:  "cl_ord_id",
    37:  "order_id",
    17:  "exec_id",
    58:  "text",
}

MSG_TYPE_NAMES = {
    "D":  "NewOrderSingle",
    "8":  "ExecutionReport",
    "9":  "OrderCancelReject",
    "A":  "Logon",
    "5":  "Logout",
    "3":  "SessionLevelReject",
    "G":  "OrderCancelReplaceRequest",
    "F":  "OrderCancelRequest",
    "V":  "MarketDataRequest",
    "W":  "MarketDataSnapshotFullRefresh",
}

INSERT_SQL = """
INSERT INTO fix_messages (
    msg_type, msg_type_name, sender_comp_id, target_comp_id, msg_seq_num,
    sending_time, symbol, side, order_qty, price, avg_px, cum_qty, leaves_qty,
    ord_status, exec_type, cl_ord_id, order_id, exec_id, text, raw_message
) VALUES (
    %(msg_type)s, %(msg_type_name)s, %(sender_comp_id)s, %(target_comp_id)s,
    %(msg_seq_num)s, %(sending_time)s, %(symbol)s, %(side)s, %(order_qty)s,
    %(price)s, %(avg_px)s, %(cum_qty)s, %(leaves_qty)s, %(ord_status)s,
    %(exec_type)s, %(cl_ord_id)s, %(order_id)s, %(exec_id)s, %(text)s,
    %(raw_message)s
)
"""


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db_connection() -> mysql.connector.MySQLConnection:
    """Connect to MySQL using credentials from environment."""
    load_dotenv()
    conn = mysql.connector.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ.get("MYSQL_PORT", 3306)),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ["MYSQL_DATABASE"],
        autocommit=False,
        connection_timeout=10,
    )
    log.info(
        "Connected to MySQL at %s:%s/%s",
        os.environ["MYSQL_HOST"],
        os.environ.get("MYSQL_PORT", 3306),
        os.environ["MYSQL_DATABASE"],
    )
    return conn


# ---------------------------------------------------------------------------
# FIX decoding helpers
# ---------------------------------------------------------------------------

def decode_line(raw_line: str) -> dict[str, Any] | None:
    """
    Decode a single pipe-delimited FIX message line into a dict of tag→value.
    Returns None if the line is empty or malformed.
    """
    line = raw_line.strip()
    if not line:
        return None

    pairs: dict[int, str] = {}
    for pair in line.split("|"):
        pair = pair.strip()
        if not pair:
            continue
        if "=" not in pair:
            continue
        tag_str, _, value = pair.partition("=")
        try:
            tag = int(tag_str)
        except ValueError:
            continue
        pairs[tag] = value

    if 35 not in pairs:
        # Not a valid FIX message (no MsgType)
        return None

    return pairs


def parse_sending_time(value: str) -> datetime | None:
    """
    Parse FIX SendingTime (tag 52).
    Accepts: YYYYMMDD-HH:MM:SS, YYYYMMDD-HH:MM:SS.fff, YYYYMMDD-HH:MM:SS.ffffff
    """
    for fmt in (
        "%Y%m%d-%H:%M:%S.%f",
        "%Y%m%d-%H:%M:%S",
    ):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def build_row(pairs: dict[int, str], raw_line: str) -> dict[str, Any]:
    """
    Map decoded FIX tag/value pairs to a database row dict.
    Missing optional fields default to None.
    """
    msg_type = pairs.get(35, "")

    # Parse sending_time
    sending_time_raw = pairs.get(52)
    sending_time: datetime | None = None
    if sending_time_raw:
        sending_time = parse_sending_time(sending_time_raw)
    if sending_time is None:
        sending_time = datetime.now(tz=timezone.utc)

    def decimal_or_none(tag: int) -> float | None:
        val = pairs.get(tag)
        if val is None:
            return None
        try:
            return float(val)
        except ValueError:
            return None

    def int_or_zero(tag: int) -> int:
        val = pairs.get(tag)
        if val is None:
            return 0
        try:
            return int(val)
        except ValueError:
            return 0

    return {
        "msg_type":       msg_type,
        "msg_type_name":  MSG_TYPE_NAMES.get(msg_type, "Unknown"),
        "sender_comp_id": pairs.get(49, ""),
        "target_comp_id": pairs.get(56, ""),
        "msg_seq_num":    int_or_zero(34),
        "sending_time":   sending_time,
        "symbol":         pairs.get(55, ""),
        "side":           pairs.get(54),
        "order_qty":      decimal_or_none(38),
        "price":          decimal_or_none(44),
        "avg_px":         decimal_or_none(6),
        "cum_qty":        decimal_or_none(14),
        "leaves_qty":     decimal_or_none(151),
        "ord_status":     pairs.get(39),
        "exec_type":      pairs.get(150),
        "cl_ord_id":      pairs.get(11, ""),
        "order_id":       pairs.get(37, ""),
        "exec_id":        pairs.get(17, ""),
        "text":           pairs.get(58, ""),
        "raw_message":    raw_line.strip(),
    }


# ---------------------------------------------------------------------------
# Offset tracking (incremental parsing)
# ---------------------------------------------------------------------------

def read_offset(offset_file: Path) -> int:
    """Return the last recorded byte offset, or 0 if none."""
    if offset_file.exists():
        try:
            return int(offset_file.read_text().strip())
        except (ValueError, OSError):
            return 0
    return 0


def write_offset(offset_file: Path, offset: int) -> None:
    """Persist the current byte offset to disk."""
    offset_file.parent.mkdir(parents=True, exist_ok=True)
    offset_file.write_text(str(offset))


# ---------------------------------------------------------------------------
# Anomaly check trigger
# ---------------------------------------------------------------------------

def run_anomaly_checks(cursor: mysql.connector.cursor.MySQLCursor) -> None:
    """
    Import and call the alerts module to run all anomaly checks.
    Errors in alerting must never crash the parser.
    """
    try:
        from src import alerts  # noqa: PLC0415
        alerts.run_all_checks(cursor)
    except Exception as exc:  # noqa: BLE001
        log.warning("Anomaly check failed (non-fatal): %s", exc)


# ---------------------------------------------------------------------------
# Core parse loop
# ---------------------------------------------------------------------------

def parse_file(
    log_file: Path,
    conn: mysql.connector.MySQLConnection,
    full: bool = False,
    offset_file: Path = OFFSET_FILE,
) -> int:
    """
    Parse the FIX log file and insert new records into the database.

    Args:
        log_file:    Path to the .log file
        conn:        Active MySQL connection
        full:        If True, re-parse from byte 0 (ignores offset file)
        offset_file: Path to the offset tracking file

    Returns:
        Number of rows inserted
    """
    start_offset = 0 if full else read_offset(offset_file)

    if not log_file.exists():
        log.error("Log file not found: %s", log_file)
        return 0

    cursor = conn.cursor()
    rows: list[dict[str, Any]] = []
    total_inserted = 0
    lines_read = 0
    lines_skipped = 0

    log.info(
        "Parsing %s from byte offset %d (full=%s) …",
        log_file, start_offset, full
    )

    with open(log_file, "r", encoding="ascii", errors="replace") as fh:
        fh.seek(start_offset)

        for raw_line in fh:
            lines_read += 1
            try:
                pairs = decode_line(raw_line)
                if pairs is None:
                    lines_skipped += 1
                    continue
                row = build_row(pairs, raw_line)
                rows.append(row)
            except Exception as exc:  # noqa: BLE001
                log.warning("Skipping malformed line %d: %s — %r", lines_read, exc, raw_line[:80])
                lines_skipped += 1
                continue

            # Flush batch
            if len(rows) >= BATCH_SIZE:
                cursor.executemany(INSERT_SQL, rows)
                conn.commit()
                total_inserted += len(rows)
                log.info("Inserted batch of %d rows (total: %d)", len(rows), total_inserted)
                rows.clear()

        # Final partial batch
        if rows:
            cursor.executemany(INSERT_SQL, rows)
            conn.commit()
            total_inserted += len(rows)
            log.info("Inserted final batch of %d rows (total: %d)", len(rows), total_inserted)

        # Record end position
        new_offset = fh.tell()

    write_offset(offset_file, new_offset)

    log.info(
        "Done — lines read: %d, skipped: %d, inserted: %d, new offset: %d",
        lines_read, lines_skipped, total_inserted, new_offset,
    )

    # Run anomaly checks after each parse pass
    if total_inserted > 0:
        run_anomaly_checks(cursor)

    cursor.close()
    return total_inserted


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Parse a FIX 4.4 log file and insert records into MySQL."
    )
    ap.add_argument(
        "--file", "-f",
        type=str,
        default=str(DEFAULT_LOG_FILE),
        help=f"Path to the FIX log file (default: {DEFAULT_LOG_FILE})",
    )
    ap.add_argument(
        "--full",
        action="store_true",
        default=False,
        help="Re-parse from the beginning, ignoring the offset file",
    )
    ap.add_argument(
        "--offset-file",
        type=str,
        default=str(OFFSET_FILE),
        help=f"Path to the offset tracking file (default: {OFFSET_FILE})",
    )
    args = ap.parse_args()

    log_path = Path(args.file)
    offset_path = Path(args.offset_file)

    if args.full and offset_path.exists():
        log.info("--full specified: resetting offset file %s", offset_path)
        offset_path.unlink()

    try:
        conn = get_db_connection()
    except Exception as exc:
        log.error("Cannot connect to MySQL: %s", exc)
        sys.exit(1)

    try:
        inserted = parse_file(log_path, conn, full=args.full, offset_file=offset_path)
        log.info("Parse complete — %d rows inserted.", inserted)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
