"""FIX 4.4 SPY equity session simulator — writes raw FIX to a remote log file."""

import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import paramiko
import simplefix

# ── Config ────────────────────────────────────────────────────────────────────
SENDER, TARGET, ACCOUNT = "ALGO_DESK", "EQUITY_GW", "SPY-EQ-01"
SYMBOL, EXCHANGE        = "SPY", "ARCA"
INTERVAL                = 3

# Remote log target (EC2 via SSH alias from ~/.ssh/config)
REMOTE_HOST     = "fix-analyzer"
REMOTE_LOG_PATH = "/home/ubuntu/fix-analyzer/logs/fix-session.log"
SSH_KEY_PATH    = os.path.expanduser("~/downloads/fix-parser-kp.pem")

# Colours
R, B, D = "\033[0m", "\033[1m", "\033[2m"
CYAN, GREEN, YELLOW, RED, BLUE = (
    "\033[96m", "\033[92m", "\033[93m", "\033[91m", "\033[94m"
)

# ── Order state ───────────────────────────────────────────────────────────────
@dataclass
class Order:
    cid:     str
    oid:     str        # exchange-assigned OrderID
    side:    int
    qty:     int
    price:   float | None
    cum_qty: int = 0

    @property
    def leaves(self) -> int:
        return self.qty - self.cum_qty

_seq                  = 0
_pending: list[Order] = []   # sent D, awaiting ER ack
_open:    list[Order] = []   # acked & live
_cid_history: list[str] = []   # track used ClOrdIDs for duplicates

# ── Anomaly Logic ─────────────────────────────────────────────────────────────
ANOMALY_PROB = 0.10

def _get_anomaly() -> str | None:
    if random.random() > ANOMALY_PROB:
        return None
    return random.choice([
        "SEQ_GAP",
        "FAT_FINGER",
        "DUP_CID",
        "UNKNOWN_ORIG",
    ])

# ── Helpers ───────────────────────────────────────────────────────────────────
def _next_seq(anomaly: str | None = None) -> int:
    global _seq
    if anomaly == "SEQ_GAP":
        # 50/50 skip forward or skip backward (out of order)
        if random.random() < 0.5:
            _seq += random.randint(5, 15)
        else:
            return max(1, _seq - random.randint(5, 15))
    else:
        _seq += 1
    return _seq

def _cid(duplicate: bool = False) -> str:
    if duplicate and _cid_history:
        return random.choice(_cid_history)
    cid = f"SPY-{uuid.uuid4().hex[:8].upper()}"
    _cid_history.append(cid)
    if len(_cid_history) > 100: _cid_history.pop(0)
    return cid
def _oid()  -> str: return f"ORD-{uuid.uuid4().hex[:6].upper()}"
def _exid() -> str: return f"EXC-{uuid.uuid4().hex[:6].upper()}"

def _hdr(msg: simplefix.FixMessage, mtype: str, anomaly: str | None = None) -> None:
    msg.append_pair(8,  "FIX.4.4")
    msg.append_pair(35, mtype)
    msg.append_pair(49, SENDER)
    msg.append_pair(56, TARGET)
    msg.append_pair(34, _next_seq(anomaly), header=True)
    msg.append_utc_timestamp(52, precision=6, header=True)

# ── Outbound builders ─────────────────────────────────────────────────────────
def _new_order(anomaly: str | None = None) -> tuple[simplefix.FixMessage, Order]:
    side   = random.choice([1, 2])
    qty    = random.choice([100, 200, 300, 500, 1000])
    tif    = random.choice(["0", "0", "0", "3"])
    is_mkt = random.random() < 0.20
    otype  = "1" if is_mkt else "2"
    price  = None if is_mkt else round(random.uniform(560.0, 585.0), 2)

    # Apply anomalies
    if anomaly == "FAT_FINGER":
        # Market orders can only have qty fat fingers; price is skipped.
        if is_mkt or random.random() < 0.5:
            qty *= 1000  # Massive quantity
        else:
            price = (price or 570.0) * (10.0 if random.random() < 0.5 else 0.1)
            price = round(price, 2)

    cid = _cid(duplicate=(anomaly == "DUP_CID"))
    oid = _oid()

    msg = simplefix.FixMessage()
    _hdr(msg, "D", anomaly)
    msg.append_pair(11, cid);  msg.append_pair(1,   ACCOUNT)
    msg.append_pair(21, "1");  msg.append_pair(55,  SYMBOL)
    msg.append_pair(207, EXCHANGE)
    msg.append_pair(54, side); msg.append_pair(38,  qty)
    msg.append_pair(40, otype)
    if price: msg.append_pair(44, f"{price:.2f}")
    msg.append_pair(59, tif);  msg.append_utc_timestamp(60, precision=6)

    return msg, Order(cid=cid, oid=oid, side=side, qty=qty, price=price)

def _cancel(o: Order, anomaly: str | None = None) -> simplefix.FixMessage:
    msg = simplefix.FixMessage()
    _hdr(msg, "F", anomaly)
    orig_cid = _cid() if anomaly == "UNKNOWN_ORIG" else o.cid
    msg.append_pair(41, orig_cid); msg.append_pair(11, _cid())
    msg.append_pair(37, o.oid); msg.append_pair(1,  ACCOUNT)
    msg.append_pair(55, SYMBOL); msg.append_pair(207, EXCHANGE)
    msg.append_pair(54, o.side); msg.append_pair(38, o.qty)
    msg.append_utc_timestamp(60, precision=6)
    return msg

def _replace(o: Order, anomaly: str | None = None) -> simplefix.FixMessage:
    new_px  = round((o.price or 572.0) * random.uniform(0.99, 1.01), 2)
    new_qty = random.choice([100, 200, 300, 500])
    msg = simplefix.FixMessage()
    _hdr(msg, "G", anomaly)
    orig_cid = _cid() if anomaly == "UNKNOWN_ORIG" else o.cid
    msg.append_pair(41, orig_cid); msg.append_pair(11, _cid())
    msg.append_pair(37, o.oid); msg.append_pair(1,  ACCOUNT)
    msg.append_pair(21, "1");   msg.append_pair(55, SYMBOL)
    msg.append_pair(207, EXCHANGE)
    msg.append_pair(54, o.side); msg.append_pair(38, new_qty)
    msg.append_pair(40, "2");    msg.append_pair(44, f"{new_px:.2f}")
    msg.append_pair(59, "0");    msg.append_utc_timestamp(60, precision=6)
    o.price = new_px; o.qty = new_qty
    return msg

# ── Inbound builders ──────────────────────────────────────────────────────────
def _exec_report(
    o: Order, etype: str,
    last_qty: int = 0, last_px: float = 0.0,
    anomaly: str | None = None,
) -> simplefix.FixMessage:
    """Execution Report (35=8). etype: 0=New, 1=Partial, 2=Fill, 8=Rejected."""
    msg = simplefix.FixMessage()
    _hdr(msg, "8", anomaly)
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
    return msg

def _cancel_reject(o: Order, anomaly: str | None = None) -> simplefix.FixMessage:
    """Order Cancel Reject (35=9) — order already filled."""
    msg = simplefix.FixMessage()
    _hdr(msg, "9", anomaly)
    msg.append_pair(37, o.oid);  msg.append_pair(11, _cid())
    msg.append_pair(41, o.cid);  msg.append_pair(39, "2")
    msg.append_pair(102, "0");   msg.append_pair(58, "Too late to cancel")
    msg.append_utc_timestamp(60, precision=6)
    return msg

# ── Lifecycle scheduler ───────────────────────────────────────────────────────
Tick = tuple[simplefix.FixMessage, str, str]   # (msg, direction, type_label)

def next_tick() -> Tick:
    anomaly = _get_anomaly()
    label_suffix = f" {RED}[ANOMALY: {anomaly}]{R}" if anomaly else ""

    # 1. Respond to oldest pending-new order
    if _pending:
        o = _pending.pop(0)
        if random.random() < 0.12:      # 12% rejection
            return _exec_report(o, "8", anomaly=anomaly), "←", f"EXEC REPORT  [REJECTED]{label_suffix}"
        _open.append(o)
        return _exec_report(o, "0", anomaly=anomaly), "←", f"EXEC REPORT  [ACK]{label_suffix}"

    # 2. Advance an open order (fill / partial fill)
    if _open and random.random() < 0.45:
        o = random.choice(_open)
        _open.remove(o)
        fill_px = o.price or round(random.uniform(560, 585), 2)

        if o.leaves > 100 and random.random() < 0.40:
            qty = random.randint(o.leaves // 4, o.leaves // 2)
            o.cum_qty += qty
            _open.append(o)
            return _exec_report(o, "1", qty, fill_px, anomaly), "←", f"EXEC REPORT  [PARTIAL FILL]{label_suffix}"
        qty = o.leaves
        o.cum_qty += qty
        return _exec_report(o, "2", qty, fill_px, anomaly), "←", f"EXEC REPORT  [FILL]{label_suffix}"

    # 3. Outbound action on an open order
    if _open and random.random() < 0.25:
        o = random.choice(_open)
        _open.remove(o)
        if random.random() < 0.5:
            msg = _cancel(o, anomaly)
            if random.random() < 0.20:
                o.cum_qty = o.qty
                _pending.insert(0, o)
            return msg, "→", f"CANCEL REQUEST{label_suffix}"
        _open.append(o)
        return _replace(o, anomaly), "→", f"CANCEL/REPLACE{label_suffix}"

    # 4. Default: new outbound order
    msg, o = _new_order(anomaly)
    _pending.append(o)
    return msg, "→", f"NEW ORDER SINGLE{label_suffix}"

# ── Remote log ────────────────────────────────────────────────────────────────
class RemoteLog:
    """Persistent SFTP connection that appends raw FIX frames to a remote file."""

    def __init__(self) -> None:
        self._ssh:  paramiko.SSHClient | None = None
        self._sftp: paramiko.SFTPClient | None = None
        self._fh:   paramiko.SFTPFile | None = None

    def connect(self) -> None:
        """Open SSH → SFTP → remote file.  Raises on any failure."""
        ssh_config  = paramiko.SSHConfig()
        config_path = os.path.expanduser("~/.ssh/config")
        if os.path.exists(config_path):
            with open(config_path) as f:
                ssh_config.parse(f)
        cfg = ssh_config.lookup(REMOTE_HOST)

        hostname = cfg.get("hostname", REMOTE_HOST)
        username = cfg.get("user", "ubuntu")
        key_file = cfg.get("identityfile", [SSH_KEY_PATH])
        key_file = os.path.expanduser(key_file[0] if isinstance(key_file, list) else key_file)

        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=hostname, username=username, key_filename=key_file, timeout=10)
        sftp = client.open_sftp()

        # mkdir -p the remote log directory
        remote_dir = REMOTE_LOG_PATH.rsplit("/", 1)[0]
        try:
            sftp.stat(remote_dir)
        except FileNotFoundError:
            path = ""
            for part in remote_dir.lstrip("/").split("/"):
                path += f"/{part}"
                try:
                    sftp.stat(path)
                except FileNotFoundError:
                    sftp.mkdir(path)

        fh = sftp.open(REMOTE_LOG_PATH, "ab")
        fh.set_pipelined(True)
        self._ssh, self._sftp, self._fh = client, sftp, fh

    def write(self, raw: bytes) -> None:
        assert self._fh is not None, "RemoteLog.write() called before connect()"
        try:
            self._fh.write(raw + b"\n")
        except OSError as exc:
            print(f"\n{RED}✗ Remote write failed: {exc} — attempting reconnect…{R}")
            self.close()
            try:
                self.connect()
            except Exception as reconn_exc:  # noqa: BLE001
                print(f"{RED}✗ Reconnect failed: {reconn_exc} — exiting.{R}")
                sys.exit(1)

    def close(self) -> None:
        for obj in (self._fh, self._sftp, self._ssh):
            try:
                if obj is not None:
                    obj.close()
            except Exception:  # noqa: BLE001
                pass
        self._fh = self._sftp = self._ssh = None

# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    remote_log = RemoteLog()

    def _shutdown(*_: object) -> None:
        print(f"\n{YELLOW}Stopped.{R}\n")
        remote_log.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)

    print(f"\n{B}{CYAN}FIX 4.4 SPY Session Simulator{R}")

    try:
        remote_log.connect()
    except Exception as exc:  # noqa: BLE001
        print(f"{RED}✗ Cannot connect to {REMOTE_HOST}: {exc}{R}")
        print(f"{RED}  Exiting — remote log is required.{R}")
        sys.exit(1)

    print(f"  {D}Writing to {REMOTE_HOST}:{REMOTE_LOG_PATH}{R}\n")

    while True:
        msg, direction, type_label = next_tick()
        ts  = datetime.now(UTC).strftime("%H:%M:%S.%f")[:-3]
        dc  = GREEN if direction == "→" else BLUE
        print(f"  {D}{ts} UTC{R}  {B}{dc}{direction}{R}  {type_label}  {D}[seq {_seq}]{R}")
        remote_log.write(msg.encode())
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
