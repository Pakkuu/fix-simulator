"""FIX 4.4 SPY equity order generator — prints a message every 3 seconds."""

import random
import signal
import sys
import time
import uuid
from datetime import UTC, datetime

import simplefix

# ── Config ────────────────────────────────────────────────────────────────────
SENDER, TARGET, ACCOUNT = "ALGO_DESK", "EQUITY_GW", "SPY-EQ-01"
SYMBOL, EXCHANGE         = "SPY", "ARCA"
INTERVAL                 = 3  # seconds between messages

# Terminal colours
R, B, D = "\033[0m", "\033[1m", "\033[2m"
CYAN, GREEN, YELLOW, RED, MAG, WHT = (
    "\033[96m", "\033[92m", "\033[93m", "\033[91m", "\033[95m", "\033[97m"
)
MSG_COLOR = {"D": GREEN, "F": RED, "G": YELLOW}

# ── State ─────────────────────────────────────────────────────────────────────
_seq  = 0
_live: list[dict] = []   # pool of open orders for cancel/replace

# ── Helpers ───────────────────────────────────────────────────────────────────
def _seq_next() -> int:
    global _seq; _seq += 1; return _seq

def _cid() -> str:
    return f"SPY-{uuid.uuid4().hex[:8].upper()}"

def _header(msg: simplefix.FixMessage, msg_type: str) -> None:
    msg.append_pair(8, "FIX.4.4")
    msg.append_pair(35, msg_type)
    msg.append_pair(49, SENDER)
    msg.append_pair(56, TARGET)
    msg.append_pair(34, _seq_next(), header=True)
    msg.append_utc_timestamp(52, precision=6, header=True)

def _decode(raw: bytes) -> list[tuple[str, str]]:
    return [
        (t.decode(), v.decode())
        for f in raw.split(b"\x01")
        if b"=" in f
        for t, _, v in [f.partition(b"=")]
    ]

# ── Message builders ──────────────────────────────────────────────────────────
def build_new_order(side: int, qty: int, ord_type: str, price: float | None, tif: str) -> tuple[simplefix.FixMessage, dict]:
    msg, cid = simplefix.FixMessage(), _cid()
    _header(msg, "D")
    msg.append_pair(11, cid)
    msg.append_pair(1,  ACCOUNT)
    msg.append_pair(21, "1")          # HandlInst: automated
    msg.append_pair(55, SYMBOL)
    msg.append_pair(207, EXCHANGE)
    msg.append_pair(54, side)
    msg.append_pair(38, qty)
    msg.append_pair(40, ord_type)
    if price: msg.append_pair(44, f"{price:.2f}")
    msg.append_pair(59, tif)
    msg.append_utc_timestamp(60, precision=6)
    return msg, {"t": "D", "cid": cid, "side": side, "qty": qty, "price": price, "tif": tif, "ord_type": ord_type}

def build_cancel(o: dict) -> tuple[simplefix.FixMessage, dict]:
    msg, cid = simplefix.FixMessage(), _cid()
    _header(msg, "F")
    msg.append_pair(41, o["cid"])
    msg.append_pair(11, cid)
    msg.append_pair(1,  ACCOUNT)
    msg.append_pair(55, SYMBOL)
    msg.append_pair(207, EXCHANGE)
    msg.append_pair(54, o["side"])
    msg.append_pair(38, o["qty"])
    msg.append_utc_timestamp(60, precision=6)
    return msg, {"t": "F", "cid": cid, "orig": o["cid"], "side": o["side"], "qty": o["qty"]}

def build_replace(o: dict, new_qty: int, new_price: float) -> tuple[simplefix.FixMessage, dict]:
    msg, cid = simplefix.FixMessage(), _cid()
    _header(msg, "G")
    msg.append_pair(41, o["cid"])
    msg.append_pair(11, cid)
    msg.append_pair(1,  ACCOUNT)
    msg.append_pair(21, "1")
    msg.append_pair(55, SYMBOL)
    msg.append_pair(207, EXCHANGE)
    msg.append_pair(54, o["side"])
    msg.append_pair(38, new_qty)
    msg.append_pair(40, "2")          # Limit
    msg.append_pair(44, f"{new_price:.2f}")
    msg.append_pair(59, "0")          # Day
    msg.append_utc_timestamp(60, precision=6)
    return msg, {"t": "G", "cid": cid, "orig": o["cid"], "side": o["side"], "new_qty": new_qty, "new_price": new_price, "price": new_price}

# ── Random order logic ────────────────────────────────────────────────────────
def next_message() -> tuple[simplefix.FixMessage, dict]:
    # 30% chance to act on an existing order
    if _live and random.random() < 0.30:
        o = random.choice(_live); _live.remove(o)
        if random.random() < 0.5:
            return build_cancel(o)
        new_price = round((o.get("price") or random.uniform(560, 580)) * random.uniform(0.99, 1.01), 2)
        msg, meta = build_replace(o, random.choice([100, 200, 300, 500]), new_price)
        _live.append(meta)
        return msg, meta

    # New order
    side     = random.choice([1, 2])
    qty      = random.choice([100, 200, 300, 500, 1000])
    tif      = random.choice(["0", "0", "0", "3"])     # Day-weighted
    is_mkt   = random.random() < 0.20
    ord_type = "1" if is_mkt else "2"
    price    = None if is_mkt else round(random.uniform(560.00, 585.00), 2)

    msg, meta = build_new_order(side, qty, ord_type, price, tif)
    _live.append(meta)
    if len(_live) > 15: _live.pop(0)
    return msg, meta

# ── Display ───────────────────────────────────────────────────────────────────
TIF_LBL = {"0": "Day", "3": "IOC", "6": "GTD"}
SIDE_LBL = {1: "BUY", 2: "SELL"}
LABEL    = {"D": "NEW ORDER SINGLE", "F": "ORDER CANCEL REQUEST", "G": "ORDER CANCEL/REPLACE"}

def display(msg: simplefix.FixMessage, meta: dict) -> None:
    raw   = msg.encode()
    pairs = _decode(raw)
    color = MSG_COLOR.get(meta["t"], WHT)
    ts    = datetime.now(UTC).strftime("%H:%M:%S.%f")[:-3]
    side  = SIDE_LBL.get(meta.get("side", 0), "?")

    print(f"\n{D}{'─'*65}{R}")
    print(f"{B}{color}▶  {LABEL[meta['t']]}{R}  {D}{ts} UTC  seq={_seq}{R}")
    print(f"{D}{'─'*65}{R}")

    if meta["t"] == "D":
        p    = f"@ {meta['price']:.2f}" if meta["price"] else "@ MKT"
        tif  = TIF_LBL.get(meta["tif"], meta["tif"])
        print(f"  {B}{side}{R}  {CYAN}{meta['qty']} {SYMBOL}{R}  {WHT}{p}{R}  {D}TIF={tif}{R}")
    elif meta["t"] == "F":
        print(f"  {B}CANCEL{R}  {CYAN}{meta['qty']} {SYMBOL}{R}  {D}orig={meta['orig']}{R}")
    elif meta["t"] == "G":
        print(f"  {B}MODIFY{R}  {CYAN}{meta['new_qty']} {SYMBOL}{R}  {WHT}→ {meta['new_price']:.2f}{R}  {D}orig={meta['orig']}{R}")

    for tag, val in pairs:
        print(f"    {MAG}{tag:>5}{R}={WHT}{val}{R}")

# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    signal.signal(signal.SIGINT, lambda *_: (print(f"\n{YELLOW}Stopped.{R}\n"), sys.exit(0)))
    print(f"\n{B}{CYAN}FIX 4.4 SPY Equity Generator{R}  {D}{SENDER} → {TARGET}  |  Ctrl+C to stop{R}")
    while True:
        display(*next_message())
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
