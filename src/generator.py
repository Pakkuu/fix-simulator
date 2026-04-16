"""
generator.py — FIX 4.4 Session Log Generator
==============================================
Generates a realistic FIX 4.4 session log containing:
  - Logon / Logout messages
  - NewOrderSingle (D) across 5 symbols
  - ExecutionReports (8): full fills, partial fills, rejections
  - OrderCancelReject (9)
  - Deliberate sequence gaps (simulated message drops)

Output: logs/fix_session.log
  - One FIX message per line
  - SOH (\\x01) used internally; pipe (|) written to log file for readability
  - Each line also preserved as raw bytes parsable by simplefix

Usage:
    uv run python src/generator.py
    uv run python src/generator.py --messages 1000 --seed 99
"""

import argparse
import os
import random
import string
import struct
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import simplefix

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SYMBOLS = ["AAPL", "MSFT", "TSLA", "AMZN", "GOOG"]

SENDER = "CLIENT1"
TARGET = "BROKER1"

# Approximate mid prices per symbol (USD)
BASE_PRICES = {
    "AAPL": 185.00,
    "MSFT": 415.00,
    "TSLA": 175.00,
    "AMZN": 185.00,
    "GOOG": 170.00,
}

MSG_TYPE_NAMES = {
    b"D": "NewOrderSingle",
    b"8": "ExecutionReport",
    b"9": "OrderCancelReject",
    b"A": "Logon",
    b"5": "Logout",
    b"3": "SessionLevelReject",
}

# Rejection texts for ExecutionReport rejects
REJECTION_REASONS = [
    "Unknown symbol",
    "Price out of range",
    "Insufficient margin",
    "Market closed",
    "Duplicate ClOrdID",
    "Order size too large",
    "Invalid account",
    "Risk limit exceeded",
]

# Output directory
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "fix_session.log"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def gen_cl_ord_id(rng: random.Random) -> str:
    """Generate a unique client order ID."""
    return "ORD-" + "".join(rng.choices(string.ascii_uppercase + string.digits, k=10))


def gen_order_id(rng: random.Random) -> str:
    """Generate a broker-assigned order ID."""
    return "BKID-" + "".join(rng.choices(string.digits, k=8))


def gen_exec_id(seq: int) -> str:
    """Generate an execution ID based on sequence."""
    return f"EXEC-{seq:08d}"


def format_time(dt: datetime) -> str:
    """Format datetime as FIX YYYYMMDD-HH:MM:SS.ffffff."""
    return dt.strftime("%Y%m%d-%H:%M:%S.%f")


def checksum(body: bytes) -> str:
    """Compute FIX checksum: sum of all bytes mod 256, zero-padded to 3 digits."""
    return str(sum(body) % 256).zfill(3)


def build_header(msg_type: bytes, seq_num: int, sending_time: datetime) -> simplefix.FixMessage:
    """Create a FixMessage pre-populated with standard header tags."""
    msg = simplefix.FixMessage()
    msg.append_pair(8, b"FIX.4.4")           # BeginString
    msg.append_pair(35, msg_type)              # MsgType
    msg.append_pair(49, SENDER)               # SenderCompID
    msg.append_pair(56, TARGET)               # TargetCompID
    msg.append_pair(34, str(seq_num))         # MsgSeqNum
    msg.append_pair(52, format_time(sending_time))  # SendingTime
    return msg


def encode_to_pipe(msg: simplefix.FixMessage) -> str:
    """Encode a FixMessage to a pipe-delimited string (SOH → |)."""
    raw: bytes = msg.encode()
    return raw.replace(b"\x01", b"|").decode("ascii", errors="replace")


# ---------------------------------------------------------------------------
# Message builders
# ---------------------------------------------------------------------------

def make_logon(seq_num: int, dt: datetime) -> simplefix.FixMessage:
    msg = build_header(b"A", seq_num, dt)
    msg.append_pair(98, b"0")    # EncryptMethod=None
    msg.append_pair(108, b"30") # HeartBtInt=30s
    return msg


def make_logout(seq_num: int, dt: datetime) -> simplefix.FixMessage:
    msg = build_header(b"5", seq_num, dt)
    msg.append_pair(58, b"Normal session end")
    return msg


def make_new_order(
    seq_num: int,
    dt: datetime,
    cl_ord_id: str,
    symbol: str,
    side: str,  # "1"=Buy, "2"=Sell
    qty: float,
    price: float,
    rng: random.Random,
) -> simplefix.FixMessage:
    msg = build_header(b"D", seq_num, dt)
    msg.append_pair(11, cl_ord_id)              # ClOrdID
    msg.append_pair(55, symbol)                 # Symbol
    msg.append_pair(54, side)                   # Side
    msg.append_pair(60, format_time(dt))        # TransactTime
    msg.append_pair(38, f"{qty:.2f}")           # OrderQty
    msg.append_pair(40, b"2")                   # OrdType=Limit
    msg.append_pair(44, f"{price:.4f}")         # Price
    msg.append_pair(59, b"0")                   # TimeInForce=Day
    msg.append_pair(1, "ACC-" + "".join(rng.choices(string.digits, k=5)))  # Account
    return msg


def make_exec_report(
    seq_num: int,
    dt: datetime,
    exec_id: str,
    cl_ord_id: str,
    order_id: str,
    symbol: str,
    side: str,
    order_qty: float,
    last_px: float,
    cum_qty: float,
    leaves_qty: float,
    avg_px: float,
    ord_status: str,
    exec_type: str,
    text: str = "",
) -> simplefix.FixMessage:
    msg = build_header(b"8", seq_num, dt)
    msg.append_pair(37, order_id)               # OrderID
    msg.append_pair(11, cl_ord_id)              # ClOrdID
    msg.append_pair(17, exec_id)                # ExecID
    msg.append_pair(150, exec_type)             # ExecType
    msg.append_pair(39, ord_status)             # OrdStatus
    msg.append_pair(55, symbol)                 # Symbol
    msg.append_pair(54, side)                   # Side
    msg.append_pair(38, f"{order_qty:.2f}")     # OrderQty
    msg.append_pair(32, f"{last_px:.4f}")       # LastPx
    msg.append_pair(31, f"{last_px:.4f}")       # LastShares
    msg.append_pair(14, f"{cum_qty:.2f}")       # CumQty
    msg.append_pair(151, f"{leaves_qty:.2f}")   # LeavesQty
    msg.append_pair(6, f"{avg_px:.4f}")         # AvgPx
    msg.append_pair(60, format_time(dt))        # TransactTime
    if text:
        msg.append_pair(58, text)               # Text
    return msg


def make_cancel_reject(
    seq_num: int,
    dt: datetime,
    cl_ord_id: str,
    orig_cl_ord_id: str,
    order_id: str,
    reason: str = "Unknown order",
) -> simplefix.FixMessage:
    msg = build_header(b"9", seq_num, dt)
    msg.append_pair(37, order_id)               # OrderID
    msg.append_pair(11, cl_ord_id)              # ClOrdID
    msg.append_pair(41, orig_cl_ord_id)         # OrigClOrdID
    msg.append_pair(39, b"8")                   # OrdStatus=Rejected
    msg.append_pair(102, b"1")                  # CxlRejReason=UnknownOrder
    msg.append_pair(434, b"1")                  # CxlRejResponseTo=OrderCancelRequest
    msg.append_pair(58, reason)                 # Text
    return msg


# ---------------------------------------------------------------------------
# Session generator
# ---------------------------------------------------------------------------

def generate_session(
    num_orders: int,
    rng: random.Random,
    gap_positions: set[int],
) -> list[str]:
    """
    Build a full FIX 4.4 session and return a list of pipe-delimited message strings.

    gap_positions: set of 1-based order indexes after which a sequence gap is inserted
                   (i.e., seq_num is incremented extra to simulate a dropped message)
    """
    lines: list[str] = []
    seq = 1

    # Start time: today at 09:30:00 ET (simulate market open)
    base_dt = datetime.now(tz=timezone.utc).replace(
        hour=14, minute=30, second=0, microsecond=0
    )
    dt = base_dt

    def tick(min_ms: int = 50, max_ms: int = 500) -> datetime:
        nonlocal dt
        dt += timedelta(milliseconds=rng.randint(min_ms, max_ms))
        return dt

    def emit(msg: simplefix.FixMessage) -> None:
        lines.append(encode_to_pipe(msg))

    # ------------------------------------------------------------------
    # 1. Logon
    # ------------------------------------------------------------------
    emit(make_logon(seq, tick(10, 50)))
    seq += 1

    # ------------------------------------------------------------------
    # 2. Orders + execution reports
    # ------------------------------------------------------------------
    exec_counter = 1

    for order_idx in range(1, num_orders + 1):
        symbol = rng.choice(SYMBOLS)
        side = rng.choice(["1", "2"])  # 1=Buy, 2=Sell
        base_price = BASE_PRICES[symbol]
        # Randomise price within ±2%
        price = round(base_price * rng.uniform(0.98, 1.02), 4)
        qty = round(rng.choice([100, 200, 500, 1000, 2000]) * rng.uniform(0.5, 2.0), 0)
        cl_ord_id = gen_cl_ord_id(rng)
        order_id = gen_order_id(rng)

        # NewOrderSingle
        emit(make_new_order(seq, tick(), cl_ord_id, symbol, side, qty, price, rng))
        seq += 1

        # Simulate a sequence gap: skip seq_num(s) by +2
        if order_idx in gap_positions:
            seq += 2  # gap of 2 (simulates 2 dropped messages)

        # Determine fate of this order
        fate = rng.choices(
            ["full_fill", "partial_fill", "reject", "cancel_reject"],
            weights=[50, 30, 15, 5],
        )[0]

        tick(20, 200)  # propagation latency

        if fate == "full_fill":
            exec_id = gen_exec_id(exec_counter); exec_counter += 1
            emit(make_exec_report(
                seq, tick(10, 100), exec_id, cl_ord_id, order_id, symbol, side,
                order_qty=qty, last_px=price, cum_qty=qty, leaves_qty=0.0,
                avg_px=price, ord_status="2", exec_type="F",
            ))
            seq += 1

        elif fate == "partial_fill":
            # First partial fill
            filled1 = round(qty * rng.uniform(0.2, 0.6), 0)
            exec_id = gen_exec_id(exec_counter); exec_counter += 1
            emit(make_exec_report(
                seq, tick(10, 100), exec_id, cl_ord_id, order_id, symbol, side,
                order_qty=qty, last_px=price, cum_qty=filled1, leaves_qty=qty - filled1,
                avg_px=price, ord_status="1", exec_type="1",
            ))
            seq += 1

            # 60% chance of a second partial that completes the order
            if rng.random() < 0.6:
                tick(100, 800)
                exec_id = gen_exec_id(exec_counter); exec_counter += 1
                remaining = qty - filled1
                emit(make_exec_report(
                    seq, tick(10, 100), exec_id, cl_ord_id, order_id, symbol, side,
                    order_qty=qty, last_px=price, cum_qty=qty, leaves_qty=0.0,
                    avg_px=price, ord_status="2", exec_type="F",
                ))
                seq += 1

        elif fate == "reject":
            reason = rng.choice(REJECTION_REASONS)
            exec_id = gen_exec_id(exec_counter); exec_counter += 1
            emit(make_exec_report(
                seq, tick(5, 50), exec_id, cl_ord_id, order_id, symbol, side,
                order_qty=qty, last_px=0.0, cum_qty=0.0, leaves_qty=0.0,
                avg_px=0.0, ord_status="8", exec_type="8", text=reason,
            ))
            seq += 1

        elif fate == "cancel_reject":
            new_cl_ord_id = gen_cl_ord_id(rng)
            emit(make_cancel_reject(
                seq, tick(5, 50), new_cl_ord_id, cl_ord_id, order_id,
                reason="Order already filled or does not exist",
            ))
            seq += 1

    # ------------------------------------------------------------------
    # 3. Logout
    # ------------------------------------------------------------------
    emit(make_logout(seq, tick(100, 500)))

    return lines


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a synthetic FIX 4.4 session log file."
    )
    parser.add_argument(
        "--messages", "-n",
        type=int,
        default=200,
        help="Number of NewOrderSingle messages to generate (default: 200)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=str(LOG_FILE),
        help=f"Output log file path (default: {LOG_FILE})",
    )
    parser.add_argument(
        "--gaps",
        type=int,
        default=3,
        help="Number of deliberate sequence gaps to inject (default: 3)",
    )
    args = parser.parse_args()

    rng = random.Random(args.seed)

    # Pick random order indexes to inject gaps after
    gap_positions: set[int] = set(
        rng.sample(range(1, args.messages), min(args.gaps, args.messages - 1))
    )
    print(f"[generator] Injecting sequence gaps after order indexes: {sorted(gap_positions)}")

    # Ensure output directory exists
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[generator] Generating {args.messages} orders (seed={args.seed}) …")
    lines = generate_session(args.messages, rng, gap_positions)

    with open(out_path, "w", encoding="ascii") as fh:
        for line in lines:
            fh.write(line + "\n")

    print(f"[generator] Wrote {len(lines)} messages to {out_path}")


if __name__ == "__main__":
    main()
