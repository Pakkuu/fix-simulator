"""FIX 4.4 SPY equity session simulator — full order lifecycle, prints every 3s."""

import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import simplefix

# ── Config ────────────────────────────────────────────────────────────────────
SENDER, TARGET, ACCOUNT = "ALGO_DESK", "EQUITY_GW", "SPY-EQ-01"
SYMBOL, EXCHANGE        = "SPY", "ARCA"
INTERVAL                = 3

# Colours
R, B, D = "\033[0m", "\033[1m", "\033[2m"
CYAN, GREEN, YELLOW, RED, MAG, WHT, BLUE = (
    "\033[96m", "\033[92m", "\033[93m", "\033[91m", "\033[95m", "\033[97m", "\033[94m"
)

# ── Order state ───────────────────────────────────────────────────────────────
@dataclass
class Order:
    cid:   str
    oid:   str          # exchange-assigned OrderID
    side:  int
    qty:   int
    price: float | None
    cum_qty: int = 0

    @property
    def leaves(self) -> int:
        return self.qty - self.cum_qty

_seq                  = 0
_pending: list[Order] = []   # sent D, awaiting ER ack
_open:    list[Order] = []   # acked & live

# ── Helpers ───────────────────────────────────────────────────────────────────
def _next_seq(gap: bool = False) -> int:
    global _seq
    if gap:
        n = random.randint(2, 5)
        _seq += n
        print(f"\n  {YELLOW}⚠ SEQ GAP — skipped {n-1} seq num(s){R}")
    else:
        _seq += 1
    return _seq

def _cid()  -> str: return f"SPY-{uuid.uuid4().hex[:8].upper()}"
def _oid()  -> str: return f"ORD-{uuid.uuid4().hex[:6].upper()}"
def _exid() -> str: return f"EXC-{uuid.uuid4().hex[:6].upper()}"

def _hdr(msg: simplefix.FixMessage, mtype: str, gap: bool = False) -> None:
    msg.append_pair(8,  "FIX.4.4")
    msg.append_pair(35, mtype)
    msg.append_pair(49, SENDER)
    msg.append_pair(56, TARGET)
    msg.append_pair(34, _next_seq(gap), header=True)
    msg.append_utc_timestamp(52, precision=6, header=True)

def _decode(raw: bytes) -> list[tuple[str, str]]:
    return [
        (t.decode(), v.decode())
        for f in raw.split(b"\x01") if b"=" in f
        for t, _, v in [f.partition(b"=")]
    ]

# ── Outbound builders ─────────────────────────────────────────────────────────
def _new_order(gap: bool = False) -> tuple[simplefix.FixMessage, Order, str]:
    side   = random.choice([1, 2])
    qty    = random.choice([100, 200, 300, 500, 1000])
    tif    = random.choice(["0", "0", "0", "3"])
    is_mkt = random.random() < 0.20
    otype  = "1" if is_mkt else "2"
    price  = None if is_mkt else round(random.uniform(560.0, 585.0), 2)
    cid, oid = _cid(), _oid()

    msg = simplefix.FixMessage()
    _hdr(msg, "D", gap)
    msg.append_pair(11, cid);  msg.append_pair(1,   ACCOUNT)
    msg.append_pair(21, "1");  msg.append_pair(55,  SYMBOL)
    msg.append_pair(207, EXCHANGE)
    msg.append_pair(54, side); msg.append_pair(38,  qty)
    msg.append_pair(40, otype)
    if price: msg.append_pair(44, f"{price:.2f}")
    msg.append_pair(59, tif);  msg.append_utc_timestamp(60, precision=6)

    o   = Order(cid=cid, oid=oid, side=side, qty=qty, price=price)
    lbl = f"{'BUY' if side == 1 else 'SELL'} {qty} {SYMBOL} {'@ MKT' if is_mkt else f'@ {price:.2f}'}"
    return msg, o, lbl

def _cancel(o: Order, gap: bool = False) -> tuple[simplefix.FixMessage, str]:
    msg = simplefix.FixMessage()
    _hdr(msg, "F", gap)
    msg.append_pair(41, o.cid); msg.append_pair(11, _cid())
    msg.append_pair(37, o.oid); msg.append_pair(1,  ACCOUNT)
    msg.append_pair(55, SYMBOL); msg.append_pair(207, EXCHANGE)
    msg.append_pair(54, o.side); msg.append_pair(38, o.qty)
    msg.append_utc_timestamp(60, precision=6)
    return msg, f"CANCEL  {o.cid}"

def _replace(o: Order, gap: bool = False) -> tuple[simplefix.FixMessage, str]:
    new_px  = round((o.price or 572.0) * random.uniform(0.99, 1.01), 2)
    new_qty = random.choice([100, 200, 300, 500])
    msg = simplefix.FixMessage()
    _hdr(msg, "G", gap)
    msg.append_pair(41, o.cid); msg.append_pair(11, _cid())
    msg.append_pair(37, o.oid); msg.append_pair(1,  ACCOUNT)
    msg.append_pair(21, "1");   msg.append_pair(55, SYMBOL)
    msg.append_pair(207, EXCHANGE)
    msg.append_pair(54, o.side); msg.append_pair(38, new_qty)
    msg.append_pair(40, "2");    msg.append_pair(44, f"{new_px:.2f}")
    msg.append_pair(59, "0");    msg.append_utc_timestamp(60, precision=6)
    o.price = new_px; o.qty = new_qty   # update live state
    return msg, f"MODIFY  {o.cid}  qty={new_qty} px={new_px:.2f}"

# ── Inbound builders ──────────────────────────────────────────────────────────
_ER_LABEL = {
    "0": f"{GREEN}ACK (New){R}",
    "1": f"{YELLOW}PARTIAL FILL{R}",
    "2": f"{GREEN}FILL{R}",
    "8": f"{RED}REJECTED{R}",
}

def _exec_report(
    o: Order, etype: str,
    last_qty: int = 0, last_px: float = 0.0,
    gap: bool = False,
) -> tuple[simplefix.FixMessage, str]:
    """Execution Report (35=8). etype: 0=New, 1=Partial, 2=Fill, 8=Rejected."""
    msg = simplefix.FixMessage()
    _hdr(msg, "8", gap)
    msg.append_pair(37, o.oid);  msg.append_pair(17,  _exid())
    msg.append_pair(150, etype); msg.append_pair(39,  etype)
    msg.append_pair(11, o.cid);  msg.append_pair(55,  SYMBOL)
    msg.append_pair(207, EXCHANGE)
    msg.append_pair(54, o.side); msg.append_pair(38,  o.qty)
    if last_qty:
        msg.append_pair(32, last_qty); msg.append_pair(31, f"{last_px:.2f}")
    msg.append_pair(14, o.cum_qty); msg.append_pair(151, o.leaves)
    if o.price: msg.append_pair(44, f"{o.price:.2f}")
    msg.append_utc_timestamp(60, precision=6)

    summary = f"{_ER_LABEL[etype]}  {o.cid}"
    if last_qty:
        summary += f"  {last_qty}@{last_px:.2f}  cum={o.cum_qty} lvs={o.leaves}"
    return msg, summary

def _cancel_reject(o: Order, gap: bool = False) -> tuple[simplefix.FixMessage, str]:
    """Order Cancel Reject (35=9) — order already filled."""
    msg = simplefix.FixMessage()
    _hdr(msg, "9", gap)
    msg.append_pair(37, o.oid);  msg.append_pair(11, _cid())
    msg.append_pair(41, o.cid);  msg.append_pair(39, "2")   # OrdStatus=Filled
    msg.append_pair(102, "0");   msg.append_pair(58, "Too late to cancel")
    msg.append_utc_timestamp(60, precision=6)
    return msg, f"{RED}CANCEL REJECT{R}  {o.cid} — already filled"

# ── Lifecycle scheduler ───────────────────────────────────────────────────────
# Returns (msg, direction "→"/"←", type_label, summary)
Tick = tuple[simplefix.FixMessage, str, str, str]

def next_tick() -> Tick:
    gap = random.random() < 0.05   # 5% seq gap

    # 1. Respond to oldest pending-new order
    if _pending:
        o = _pending.pop(0)
        if random.random() < 0.12:      # 12% rejection
            msg, summary = _exec_report(o, "8", gap=gap)
        else:                            # ack → move to open
            msg, summary = _exec_report(o, "0", gap=gap)
            _open.append(o)
        return msg, "←", "EXEC REPORT", summary

    # 2. Advance an open order (fill / partial fill)
    if _open and random.random() < 0.45:
        o = random.choice(_open)
        _open.remove(o)
        fill_px = (o.price or round(random.uniform(560, 585), 2))

        if o.leaves > 100 and random.random() < 0.40:   # partial fill
            qty = random.randint(o.leaves // 4, o.leaves // 2)
            o.cum_qty += qty
            msg, summary = _exec_report(o, "1", qty, fill_px, gap)
            _open.append(o)
        else:                                             # full fill
            qty = o.leaves
            o.cum_qty += qty
            msg, summary = _exec_report(o, "2", qty, fill_px, gap)
        return msg, "←", "EXEC REPORT", summary

    # 3. Outbound action on an open order
    if _open and random.random() < 0.25:
        o = random.choice(_open)
        _open.remove(o)
        if random.random() < 0.5:
            msg, summary = _cancel(o, gap)
            # 20% chance order was already filled → cancel reject next tick
            if random.random() < 0.20:
                o.cum_qty = o.qty
                _pending.insert(0, o)   # sentinel: will emit cancel reject
        else:
            msg, summary = _replace(o, gap)
            _open.append(o)
        label = "CANCEL REQUEST" if "CANCEL" in summary else "CANCEL/REPLACE"
        return msg, "→", label, summary

    # 4. Send a new outbound order (default)
    msg, o, summary = _new_order(gap)
    _pending.append(o)
    return msg, "→", "NEW ORDER SINGLE", summary

# ── Display ───────────────────────────────────────────────────────────────────
DIR_COLOR   = {"→": GREEN, "←": BLUE}
TYPE_COLOR  = {
    "NEW ORDER SINGLE": GREEN,
    "EXEC REPORT":      BLUE,
    "CANCEL REQUEST":   RED,
    "CANCEL/REPLACE":   YELLOW,
}

def display(msg: simplefix.FixMessage, direction: str, type_label: str, summary: str) -> None:
    pairs = _decode(msg.encode())
    ts    = datetime.now(UTC).strftime("%H:%M:%S.%f")[:-3]
    dc    = DIR_COLOR.get(direction, WHT)
    tc    = TYPE_COLOR.get(type_label, WHT)

    print(f"\n{D}{'─'*65}{R}")
    print(f"  {B}{dc}{direction}{R}  {B}{tc}{type_label}{R}  {D}{ts} UTC  seq={_seq}{R}")
    print(f"     {summary}")
    for tag, val in pairs:
        print(f"    {MAG}{tag:>5}{R}={WHT}{val}{R}")

# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    signal.signal(signal.SIGINT, lambda *_: (print(f"\n{YELLOW}Stopped.{R}\n"), sys.exit(0)))
    print(f"\n{B}{CYAN}FIX 4.4 SPY Session Simulator{R}  {D}{SENDER} ⟷ {TARGET}  |  Ctrl+C to stop{R}")
    print(f"  {D}{GREEN}→ outbound (us → exchange){R}   {BLUE}← inbound (exchange → us){R}")
    while True:
        display(*next_tick())
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
