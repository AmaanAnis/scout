"""
MEXC 1-minute EMA20 / EMA200 cross scanner

- Scans every USDT spot pair on MEXC (filtered by 24h volume)
- Alerts when EMA20 crosses ABOVE EMA200 (bullish). Set BULLISH_ONLY=0 to also get bearish crosses.
- Only uses CLOSED 1m candles, so alerts never "repaint"
- Runs once a minute, just after each candle closes

Alerts always go to: terminal (with beep) + alerts.log
Also, by default:    desktop popup + sound (Linux / macOS / Windows beep)
Optional extras (set the environment variable to enable):
    LOCAL_SERVER_URL  your own alert_server.py, e.g. http://127.0.0.1:8000/alert  (+ ALERT_TOKEN if set there)
    DISCORD_WEBHOOK   Discord channel webhook URL
    NTFY_TOPIC        ntfy.sh topic (phone push; NTFY_SERVER to self-host)
    EMAIL_TO + SMTP_USER + SMTP_PASS   (Gmail: use an App Password)

Only "utility" coins are scanned. Excluded automatically: tokenized stocks/ETFs, stablecoins,
leveraged tokens (3L/3S...), plus anything in blocklist.txt. To scan ONLY coins you choose, put
them in allowlist.txt. Both files sit next to this script, one coin per line (HIMSON or HIMSONUSDT).

Usage:
    pip install requests
    python mexc_ema_cross_alert.py --dry-run   # show which coins are kept / excluded and why, then exit
    python mexc_ema_cross_alert.py --inspect   # show how MEXC tags coins (categories), then exit
    python mexc_ema_cross_alert.py --test    # sends a test alert and exits
    python mexc_ema_cross_alert.py           # run the scanner
"""
import os
import platform
import re
import shutil
import smtplib
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from email.message import EmailMessage
from pathlib import Path

import requests

# ------------------------- settings -------------------------
QUOTE = "USDT"                  # only pairs quoted in this asset
MIN_24H_QUOTE_VOLUME = 50_000   # skip illiquid coins (24h volume in USDT). 0 = scan everything
FAST, SLOW = 20, 200            # EMA lengths
INTERVAL = "1m"
CANDLES = 500                   # history per request (EMA200 needs warm-up)
BULLISH_ONLY = os.getenv("BULLISH_ONLY", "1") == "1"   # True: only EMA20 crossing ABOVE EMA200
LOOKBACK = 2                    # check the last N closed candles (covers a slow scan)
REQS_PER_SEC = 25               # sources disagree (300 or 500 per 10 s per endpoint) -> stay well below both
REFRESH_SYMBOLS_EVERY = 3600    # seconds

# --- "utility coins only" filter ---
HERE = Path(__file__).resolve().parent
BLOCKLIST_FILE = Path(os.getenv("BLOCKLIST_FILE", HERE / "blocklist.txt"))
ALLOWLIST_FILE = Path(os.getenv("ALLOWLIST_FILE", HERE / "allowlist.txt"))
STABLECOINS = {"USDC", "FDUSD", "TUSD", "USDD", "USDE", "DAI", "PYUSD", "USD1", "BUSD", "USDP", "EURC", "USDJ"}
LEVERAGED_RE = re.compile(r".+\d+[LS]$")                 # TOMO3L, BTC5S, ...
# whole-word match against MEXC's fullName + conceptPlates tags (so "Ondo" the coin is NOT excluded)
NON_UTILITY_RE = re.compile(r"\b(?:tokenized|xstocks?|stocks?|etfs?|equity|equities)\b", re.I)
EXCLUDE_PLATES = {p.strip().lower() for p in os.getenv("EXCLUDE_PLATES", "").split(",") if p.strip()}

DESKTOP_NOTIFY = os.getenv("DESKTOP_NOTIFY", "1") == "1"
LOG_FILE = os.getenv("ALERT_LOG", "alerts.log")
LOCAL_SERVER_URL = os.getenv("LOCAL_SERVER_URL", "")
ALERT_TOKEN = os.getenv("ALERT_TOKEN", "")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
NTFY_SERVER = os.getenv("NTFY_SERVER", "https://ntfy.sh")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")

BASE = "https://api.mexc.com"
session = requests.Session()
local_session = requests.Session()
local_session.trust_env = False      # never send localhost traffic through a proxy


# ------------------------- market data ----------------------
_cooldown_until = 0.0                # when set, EVERY thread waits until this time
_cooldown_lock = threading.Lock()


def get(path, **params):
    """GET with a shared back-off: if MEXC says 429, all threads pause, then retry."""
    global _cooldown_until
    for attempt in range(1, 4):
        wait = _cooldown_until - time.time()
        if wait > 0:
            time.sleep(wait)
        r = session.get(BASE + path, params=params, timeout=10)
        if r.status_code == 429:
            pause = 5 * attempt
            with _cooldown_lock:
                if time.time() + pause > _cooldown_until + 1:     # print once per pause, not per thread
                    print(f"[warn] MEXC rate limit (429): pausing all requests for {pause}s")
                _cooldown_until = max(_cooldown_until, time.time() + pause)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("still rate limited (429) after 3 tries")


def ema(values, length):
    """EMA seeded with the SMA of the first `length` values (same idea as TradingView)."""
    k = 2 / (length + 1)
    e = sum(values[:length]) / length
    out = [None] * (length - 1) + [e]
    for v in values[length:]:
        e = v * k + e * (1 - k)
        out.append(e)
    return out


def find_crosses(rows, now_ms=None):
    """Return [(direction, candle_open_ms, close_price)] for the last LOOKBACK closed candles."""
    now_ms = now_ms or int(time.time() * 1000)
    if rows and rows[-1][0] + 60_000 > now_ms:   # last row is still forming -> drop it
        rows = rows[:-1]
    if len(rows) < SLOW + LOOKBACK + 1:
        return []

    closes = [float(r[4]) for r in rows]
    fast, slow = ema(closes, FAST), ema(closes, SLOW)
    diff = [f - s if f is not None and s is not None else None for f, s in zip(fast, slow)]

    found = []
    for i in range(len(rows) - LOOKBACK, len(rows)):
        prev, curr = diff[i - 1], diff[i]
        if prev is None or curr is None:
            continue
        if prev <= 0 < curr:
            found.append(("UP", rows[i][0], closes[i]))
        elif prev >= 0 > curr and not BULLISH_ONLY:
            found.append(("DOWN", rows[i][0], closes[i]))
    return found


def check_symbol(symbol):
    try:
        rows = get("/api/v3/klines", symbol=symbol, interval=INTERVAL, limit=CANDLES)
        return symbol, find_crosses(rows)
    except Exception as e:
        print(f"[warn] {symbol}: {e}")
        return symbol, []


def read_list(path):
    """Read a coin list file: one coin per line, '#' starts a comment. Accepts HIMSON or HIMSONUSDT."""
    if not path.exists():
        return set()
    out = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#")[0].strip().upper()
        if line:
            out.add(line[: -len(QUOTE)] if line.endswith(QUOTE) and len(line) > len(QUOTE) else line)
    return out


def exclusion_reason(info, blocklist):
    """Why a coin is NOT a utility coin (None = keep it)."""
    base = info["baseAsset"].upper()
    if base in blocklist:
        return "in blocklist.txt"
    if base in STABLECOINS:
        return "stablecoin"
    if LEVERAGED_RE.match(base):
        return "leveraged token"
    plates = info.get("conceptPlates") or []
    for plate in plates:
        if plate.lower() in EXCLUDE_PLATES:
            return f"category '{plate}' (EXCLUDE_PLATES)"
    m = NON_UTILITY_RE.search(" ".join([info.get("fullName") or ""] + list(plates)))
    if m:
        return f"tagged '{m.group(0)}' (stock/ETF token)"
    return None


def load_universe():
    """Returns (kept_symbols, {excluded_symbol: reason}, allowlist_names_not_found)."""
    info = get("/api/v3/exchangeInfo")
    blocklist, allowlist = read_list(BLOCKLIST_FILE), read_list(ALLOWLIST_FILE)
    kept, excluded, seen = [], {}, set()
    for s in info["symbols"]:
        if (s.get("quoteAsset") != QUOTE or not s.get("isSpotTradingAllowed", True)
                or str(s.get("status")).upper() not in ("1", "ENABLED")):
            continue
        base = s["baseAsset"].upper()
        if allowlist:                                   # strict mode: only the coins you listed
            if base in allowlist:
                kept.append(s["symbol"]); seen.add(base)
            continue
        reason = exclusion_reason(s, blocklist)
        if reason:
            excluded[s["symbol"]] = reason
        else:
            kept.append(s["symbol"])
    return kept, excluded, sorted(allowlist - seen)


def get_symbols():
    kept, excluded, missing = load_universe()
    if missing:
        print(f"[warn] allowlist coins not found as tradable {QUOTE} pairs: {', '.join(missing)}")
    tickers = get("/api/v3/ticker/24hr")
    liquid = {
        t["symbol"]
        for t in tickers
        if float(t.get("quoteVolume") or 0) >= MIN_24H_QUOTE_VOLUME
    }
    symbols = sorted(set(kept) & liquid)
    print(f"Filter: {len(excluded)} non-utility pairs excluded, {len(kept)} kept, "
          f"{len(symbols)} pass the volume filter")
    return symbols


def dry_run():
    kept, excluded, missing = load_universe()
    print(f"\nKEPT: {len(kept)} pairs   EXCLUDED: {len(excluded)} pairs\n")
    for sym, why in sorted(excluded.items()):
        print(f"  excluded  {sym:<18} {why}")
    if missing:
        print(f"\n[warn] allowlist coins not found: {', '.join(missing)}")
    print("\nIf a stock token is still in KEPT, add its base asset to blocklist.txt "
          "(or use --inspect to find the category MEXC puts it in).")


def inspect_tags(args):
    """Show how MEXC describes specific coins, and every category tag with example coins."""
    info = get("/api/v3/exchangeInfo")
    rows = [s for s in info["symbols"] if s.get("quoteAsset") == QUOTE]
    by_symbol = {s["symbol"]: s for s in rows}
    wanted = args or ["HIMSONUSDT", "TTMIONUSDT", "VSTONUSDT", "MAGMAUSDT", "TNSRUSDT"]
    blocklist = read_list(BLOCKLIST_FILE)
    print("\n== Coins you asked about ==")
    for sym in wanted:
        sym = sym if sym.endswith(QUOTE) else sym + QUOTE
        s = by_symbol.get(sym)
        if not s:
            print(f"{sym}: not found"); continue
        print(f"{sym}\n    fullName: {s.get('fullName')}\n    conceptPlates: {s.get('conceptPlates')}"
              f"\n    contractAddress: {s.get('contractAddress')}"
              f"\n    filter verdict: {exclusion_reason(s, blocklist) or 'KEPT (treated as utility)'}")
    groups = {}
    for s in rows:
        for plate in (s.get("conceptPlates") or ["(no tag)"]):
            groups.setdefault(plate, []).append(s["baseAsset"])
    print("\n== All category tags on MEXC (count, tag, examples) ==")
    for plate, bases in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        print(f"{len(bases):6d}  {plate:<42} {', '.join(bases[:6])}")
    print("\nTo exclude a whole tag:  EXCLUDE_PLATES=<tag>[,<tag>]  (exact text from the list above)")


def format_alert(symbol, direction, candle_ms, price):
    base = symbol[: -len(QUOTE)]
    t = time.strftime("%H:%M", time.localtime(candle_ms / 1000))
    side = "ABOVE" if direction == "UP" else "BELOW"
    mood = "bullish" if direction == "UP" else "bearish"
    icon = "🟢" if direction == "UP" else "🔴"
    return (f"{icon} {symbol}: EMA{FAST} crossed {side} EMA{SLOW} ({mood})\n"
            f"1m candle {t} | close {price}\n"
            f"https://www.mexc.com/exchange/{base}_{QUOTE}")


# ------------------------- notifications --------------------
def send_desktop(title, text):
    if not DESKTOP_NOTIFY:
        return
    system = platform.system()
    if system == "Linux":
        if shutil.which("notify-send"):
            subprocess.run(["notify-send", title, text], timeout=5)
        sound = "/usr/share/sounds/freedesktop/stereo/message.oga"
        if shutil.which("paplay") and os.path.exists(sound):
            subprocess.Popen(["paplay", sound])
    elif system == "Darwin":
        import json
        script = (f"display notification {json.dumps(text)} "
                  f"with title {json.dumps(title)} sound name \"Glass\"")
        subprocess.run(["osascript", "-e", script], timeout=5)
    elif system == "Windows":
        import winsound
        winsound.MessageBeep()


def send_local_server(title, text):
    if LOCAL_SERVER_URL:
        headers = {"X-Token": ALERT_TOKEN} if ALERT_TOKEN else {}
        local_session.post(LOCAL_SERVER_URL, json={"title": title, "text": text},
                           headers=headers, timeout=5).raise_for_status()


def send_discord(title, text):
    if DISCORD_WEBHOOK:
        session.post(DISCORD_WEBHOOK, json={"content": text}, timeout=10).raise_for_status()


def send_ntfy(title, text):
    if NTFY_TOPIC:
        session.post(f"{NTFY_SERVER}/{NTFY_TOPIC}", data=text.encode("utf-8"),
                     headers={"Title": title}, timeout=10).raise_for_status()


def send_email(title, text):
    if EMAIL_TO and SMTP_USER and SMTP_PASS:
        msg = EmailMessage()
        msg["Subject"] = text.splitlines()[0]
        msg["From"], msg["To"] = SMTP_USER, EMAIL_TO
        msg.set_content(text)
        with smtplib.SMTP_SSL(SMTP_HOST, 465, timeout=15) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)


def notify(text, title="MEXC EMA cross"):
    print(text + "\a", flush=True)                      # terminal + bell
    with open(LOG_FILE, "a", encoding="utf-8") as f:    # log file
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {text}\n")
    for sender in (send_desktop, send_local_server, send_discord, send_ntfy, send_email):
        try:
            sender(title, text)
        except Exception as e:                           # one failing channel never blocks the rest
            print(f"[warn] {sender.__name__}: {e}")


# --------------------------- main ---------------------------
def default_on_signal(symbol, direction, candle_ms, price):
    """What happens when a cross is found. Other programs can pass in their own handler."""
    notify(format_alert(symbol, direction, candle_ms, price))


def main(on_signal=default_on_signal):
    if "--dry-run" in sys.argv:
        return dry_run()
    if "--inspect" in sys.argv:
        return inspect_tags([a.upper() for a in sys.argv[sys.argv.index("--inspect") + 1:] if not a.startswith("-")])
    if "--test" in sys.argv:
        notify("🟢 TEST: if you can see/hear this, alerts work.")
        return

    alerted = set()                 # (symbol, candle_open_ms) already sent
    symbols, last_refresh = [], 0
    scan_pool = ThreadPoolExecutor(max_workers=REQS_PER_SEC)
    alert_pool = ThreadPoolExecutor(max_workers=2)   # sending never slows the scan

    notify("Scanner started.")
    while True:
        # wait until 3 s after the next minute boundary (candle has closed)
        time.sleep(60 - (time.time() % 60) + 3)

        if not symbols or time.time() - last_refresh > REFRESH_SYMBOLS_EVERY:
            try:
                symbols = get_symbols()
                last_refresh = time.time()
                print(f"Watching {len(symbols)} {QUOTE} pairs")
            except Exception as e:
                print(f"[warn] could not load symbols: {e}")
                continue
            if not symbols:
                print("[warn] 0 symbols after filtering - check QUOTE / volume filter")
                continue

        started = time.time()
        for i in range(0, len(symbols), REQS_PER_SEC):
            t0 = time.time()
            for symbol, crosses in scan_pool.map(check_symbol, symbols[i : i + REQS_PER_SEC]):
                for direction, candle_ms, price in crosses:
                    key = (symbol, candle_ms)
                    if key not in alerted:
                        alerted.add(key)
                        alert_pool.submit(on_signal, symbol, direction, candle_ms, price)
            time.sleep(max(0, 1 - (time.time() - t0)))   # <= REQS_PER_SEC requests per second

        cutoff = (time.time() - 3600) * 1000
        alerted = {k for k in alerted if k[1] > cutoff}
        print(f"Scan finished in {time.time() - started:.1f}s")


if __name__ == "__main__":
    main()
