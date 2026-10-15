import hmac
import json
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import threading
import time
import webbrowser
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
import uvicorn
from fastapi import FastAPI, Header, HTTPException
from requests.adapters import HTTPAdapter
from pydantic import BaseModel, Field

QUOTE = "USDT"
TOP_COINS = int(os.getenv("TOP_COINS", "0"))
MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "50000"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "0"))
FAST, SLOW = 20, 200
K_FAST, K_SLOW = 2 / (FAST + 1), 2 / (SLOW + 1)
INTERVAL = "1m"
CANDLES = 1000
LOOKBACK = 3
LIQUIDITY_WINDOW = 60
MIN_BOX_PCT = float(os.getenv("MIN_BOX_PCT", "0.8"))
MIN_ACTIVE_MINUTES = int(os.getenv("MIN_ACTIVE_MINUTES", "30"))
MIN_HOURLY_QUOTE_VOLUME = float(os.getenv("MIN_HOURLY_QUOTE_VOLUME", "0"))
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0"))
BUYER_WINDOW_MIN = int(os.getenv("BUYER_WINDOW_MIN", "15"))
MIN_BUY_RATIO = float(os.getenv("MIN_BUY_RATIO", "0"))
MIN_TAPE_TRADES = int(os.getenv("MIN_TAPE_TRADES", "30"))
VERBOSE = os.getenv("VERBOSE", "0") == "1"
REQS_PER_SEC = float(os.getenv("REQS_PER_SEC", "30"))
WEIGHT_SOFT_LIMIT = int(os.getenv("WEIGHT_SOFT_LIMIT", "4800"))
REFRESH_SYMBOLS_EVERY = 3600
SCAN_DELAY = float(os.getenv("SCAN_DELAY", "1"))
NEAR_PCT = float(os.getenv("NEAR_PCT", "0.1"))
FULL_SCAN = os.getenv("FULL_SCAN", "0") == "1"
WARM_WORKERS = int(os.getenv("WARM_WORKERS", "12"))
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "16"))
ALERT_WORKERS = int(os.getenv("ALERT_WORKERS", "8"))
REWARM_AFTER_S = 3 * 3600
REWARM_PER_SCAN = 5

# Coin screening. Market cap is supplied by CoinGecko; all other live ticker metrics come from Binance.
MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD", "0"))
MAX_MARKET_CAP_USD = float(os.getenv("MAX_MARKET_CAP_USD", "0"))

def _optional_float_env(name):
    raw = os.getenv(name, "").strip()
    return float(raw) if raw else None

MIN_24H_CHANGE_PCT = _optional_float_env("MIN_24H_CHANGE_PCT")
MAX_24H_CHANGE_PCT = _optional_float_env("MAX_24H_CHANGE_PCT")
MIN_1H_VOLUME_CHANGE_PCT = _optional_float_env("MIN_1H_VOLUME_CHANGE_PCT")
MAX_1H_VOLUME_CHANGE_PCT = _optional_float_env("MAX_1H_VOLUME_CHANGE_PCT")
MARKET_DATA_REFRESH_S = max(60, int(os.getenv("MARKET_DATA_REFRESH_S", "300")))
VOLUME_CHANGE_WINDOW_S = max(900, int(os.getenv("VOLUME_CHANGE_WINDOW_S", "3600")))
SUGGESTIONS_TOP_N = max(1, int(os.getenv("SUGGESTIONS_TOP_N", "15")))
COINGECKO_PAGES = max(1, min(10, int(os.getenv("COINGECKO_PAGES", "5"))))
MARKET_CAP_TTL_S = max(3600, int(os.getenv("MARKET_CAP_TTL_S", str(6 * 3600))))
MARKET_CAP_CACHE = Path(os.getenv("MARKET_CAP_CACHE", Path(__file__).resolve().parent / "market_caps.json"))
VOLUME_HISTORY_FILE = Path(os.getenv("VOLUME_HISTORY_FILE", Path(__file__).resolve().parent / "volume_history.json"))
COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
COINGECKO_LIST_URL = "https://api.coingecko.com/api/v3/coins/list"

# Filled at runtime. Volume-change is a change in the rolling 24h quote-volume total versus
# the value recorded about an hour earlier, not a claim about exact standalone hourly turnover.
MARKET_CAPS = {}
MARKET_CAPS_LOADED_AT = 0.0
MARKET_METRICS = {}
VOLUME_HISTORY = {}
_VOLUME_HISTORY_LOADED = False
LAST_MARKET_METRICS_REFRESH = 0.0
BASE_SCREEN_SYMBOLS = []

DESKTOP_NOTIFY = os.getenv("DESKTOP_NOTIFY", "1") == "1"
RUN_SERVER = os.getenv("RUN_SERVER", "1") == "1"
USE_SERVER = RUN_SERVER or "LOCAL_SERVER_URL" in os.environ
ALERT_HOST = os.getenv("ALERT_HOST", "127.0.0.1")
ALERT_PORT = int(os.getenv("ALERT_PORT", "8000"))
ALERT_TOKEN = os.getenv("ALERT_TOKEN", "")
LOCAL_SERVER_URL = os.getenv("LOCAL_SERVER_URL", f"http://127.0.0.1:{ALERT_PORT}/alert")
TV_LAYOUT_ID = os.getenv("TV_LAYOUT_ID", "")
TV_EXCHANGE = os.getenv("TV_EXCHANGE", "BINANCE")
TEST_WAIT = float(os.getenv("TEST_WAIT", "30"))

EXCLUDE_COINS = {c.strip().upper() for c in os.getenv("EXCLUDE_COINS", "").split(",") if c.strip()}
KEEP_COINS = {c.strip().upper() for c in os.getenv("KEEP_COINS", "").split(",") if c.strip()}
BLOCKLIST_FILE = Path(os.getenv("BLOCKLIST_FILE", Path(__file__).resolve().parent / "blocklist.txt"))
EXCLUDE_TAGS = {t.strip().lower() for t in
                os.getenv("EXCLUDE_TAGS", "bstocks,tcommodities,stablecoin,meme,memes").split(",") if t.strip()}
TAG_REASON = {"bstocks": "tokenized stock/ETF (not crypto)",
              "tcommodities": "tokenized commodity (not crypto)",
              "stablecoin": "stablecoin (Binance tag)",
              "meme": "tagged Meme by Binance", "memes": "tagged Meme by Binance"}
LEVERAGED_RE = re.compile(r".+\d+[LS]$")
USE_BINANCE_TAGS = os.getenv("USE_BINANCE_TAGS", "1") == "1"
TAGS_CACHE = Path(os.getenv("TAGS_CACHE", Path(__file__).resolve().parent / "binance_tags.json"))
TAGS_TTL = 24 * 3600
TAGS_URL = "https://www.binance.com/bapi/asset/v2/public/asset-service/product/get-products?includeEtf=true"
STABLECOINS = {"USDC", "FDUSD", "TUSD", "USDP", "USDE", "DAI", "PYUSD", "USD1", "BFUSD", "RLUSD", "EURI",
               "EUR", "AEUR", "XUSD", "USDS"}
MICRO_RE = re.compile(r"^(1000|1M)[A-Z0-9]")
MEME_SEED = {
    "DOGE", "SHIB", "PEPE", "BONK", "WIF", "FLOKI", "BRETT", "POPCAT", "MOG", "TURBO", "NEIRO", "BOME",
    "MEME", "PNUT", "GOAT", "FARTCOIN", "MOODENG", "TRUMP", "MELANIA", "BABYDOGE", "DOGS", "CATI",
    "HMSTR", "MYRO", "SLERF", "MEW", "GIGA", "TOSHI", "MICHI", "SNEK", "WOJAK", "BOBO", "SAMO", "AKITA",
    "KISHU", "ELON", "PONKE", "MUMU", "MAGA", "DEGEN", "FWOG", "MOTHER", "WEN", "PEOPLE", "ACT", "LUNC",
}
BINANCE_BASES = ([os.environ["BINANCE_BASE"].rstrip("/")] if os.getenv("BINANCE_BASE") else
                 ["https://api.binance.com", "https://api-gcp.binance.com", "https://api1.binance.com",
                  "https://api2.binance.com", "https://api3.binance.com", "https://api4.binance.com",
                  "https://data-api.binance.vision"])

BASE = BINANCE_BASES[0]
session = requests.Session()
# requests keeps only 10 connections per host by default. With SCAN_WORKERS + WARM_WORKERS threads in flight the
# rest were opened, used once and thrown away, so most calls paid a fresh TCP + TLS handshake.
_adapter = HTTPAdapter(pool_connections=10, pool_maxsize=SCAN_WORKERS + WARM_WORKERS + 4)
session.mount("https://", _adapter)
session.mount("http://", _adapter)
local_session = requests.Session()
local_session.trust_env = False
RANK = {}

_actions_ok = None
_warned = set()


def _warn_once(message):
    if message not in _warned:
        _warned.add(message)
        print(f"[warn] {message}", flush=True)


def read_coin_list(path):
    """Coin list file: one coin per line, '#' starts a comment. Accepts BTC or BTCUSDT."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return set()
    out = set()
    for line in text.splitlines():
        line = line.split("#")[0].strip().upper()
        if line:
            out.add(line[: -len(QUOTE)] if line.endswith(QUOTE) and len(line) > len(QUOTE) else line)
    return out


BLOCKLIST = read_coin_list(BLOCKLIST_FILE)


def _notify_send_supports_actions():
    global _actions_ok
    if _actions_ok is None:
        try:
            out = subprocess.run(["notify-send", "--help"], capture_output=True, text=True, timeout=5)
            _actions_ok = "--action" in (out.stdout + out.stderr)
        except Exception:
            _actions_ok = False
    return _actions_ok


def _open_if_clicked(out, url):
    if (out or "").strip() in ("default", "open"):
        webbrowser.open(url)


def _wait_and_open(proc, url):
    try:
        out, _ = proc.communicate(timeout=90)
    except subprocess.TimeoutExpired:
        proc.kill()
        return
    _open_if_clicked(out, url)


def _linux_popup(title, text, url):
    if not shutil.which("notify-send"):
        _warn_once("notify-send not found, so there are no desktop popups "
                   "(Debian/Ubuntu: apt install libnotify-bin, Arch: pacman -S libnotify)")
        return False
    if url and _notify_send_supports_actions():
        proc = subprocess.Popen(
            ["notify-send", "--app-name=Crypto scanner", "-t", "60000", "--wait",
             "-A", "default=Open chart", "-A", "open=Open in TradingView", "--", title, text],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            out, err = proc.communicate(timeout=0.5)
        except subprocess.TimeoutExpired:
            threading.Thread(target=_wait_and_open, args=(proc, url), daemon=True).start()
            return True
        if proc.returncode != 0:
            _warn_once(f"notify-send failed ({(err or '').strip() or 'exit ' + str(proc.returncode)}); "
                       f"is a notification daemon running?")
            return False
        _open_if_clicked(out, url)
        return True
    result = subprocess.run(["notify-send", "--", title, text], capture_output=True, text=True, timeout=5)
    if result.returncode != 0:
        _warn_once(f"notify-send failed ({result.stderr.strip() or 'exit ' + str(result.returncode)}); "
                   f"is a notification daemon running?")
        return False
    return True


def send_desktop(title, text, url=""):
    if not DESKTOP_NOTIFY:
        return False
    system = platform.system()
    if system == "Linux":
        shown = _linux_popup(title, text, url)
        if shown:
            sound = "/usr/share/sounds/freedesktop/stereo/message.oga"
            if shutil.which("paplay") and os.path.exists(sound):
                subprocess.Popen(["paplay", sound])
        return shown
    if system == "Darwin":
        if url and shutil.which("terminal-notifier"):
            subprocess.run(["terminal-notifier", "-title", title, "-message", text.splitlines()[0],
                            "-open", url, "-sound", "Glass"], timeout=5)
        else:
            script = (f"display notification {json.dumps(text.splitlines()[0])} "
                      f"with title {json.dumps(title)} sound name \"Glass\"")
            subprocess.run(["osascript", "-e", script], timeout=5)
        return True
    if system == "Windows":
        import winsound
        winsound.MessageBeep()
        _warn_once("Windows: only a beep is played, there is no desktop popup")
        return False
    return False


app = FastAPI(title="Local alert server")


class Alert(BaseModel):
    title: str = Field(default="Alert", max_length=200)
    text: str = Field(max_length=2000)
    url: str = Field(default="", max_length=500)


@app.post("/alert", status_code=201)
def post_alert(alert: Alert, x_token: str = Header(default="")):
    if ALERT_TOKEN and not hmac.compare_digest(x_token.encode(), ALERT_TOKEN.encode()):
        raise HTTPException(status_code=401, detail="bad token")
    url = alert.url if alert.url.startswith("https://www.tradingview.com/") else ""
    popup = send_desktop(alert.title, alert.text, url)
    print(f"alert: {alert.text.splitlines()[0]} (popup {'shown' if popup else 'NOT shown'})", flush=True)
    return {"popup": popup}


@app.get("/health")
def health():
    return {"status": "ok"}


def start_server():
    config = uvicorn.Config(app, host=ALERT_HOST, port=ALERT_PORT, log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    threading.Thread(target=server.run, daemon=True).start()
    for _ in range(50):
        if server.started:
            return server
        time.sleep(0.1)
    print(f"[warn] could not start the local alert server on {ALERT_HOST}:{ALERT_PORT} (port in use?). "
          f"If another alert server is running there it will be used; otherwise popups are shown directly.",
          flush=True)
    return None


_server_warned = False


def send_local_server(title, text, url=""):
    global _server_warned
    headers = {"X-Token": ALERT_TOKEN} if ALERT_TOKEN else {}
    try:
        r = local_session.post(LOCAL_SERVER_URL, json={"title": title, "text": text, "url": url},
                               headers=headers, timeout=2)
        r.raise_for_status()
        shown = bool(r.json().get("popup"))
    except Exception as e:
        if not _server_warned:
            _server_warned = True
            print(f"[warn] alert server not reachable at {LOCAL_SERVER_URL} ({e}); "
                  f"showing popups directly instead.", flush=True)
        return False
    _server_warned = False
    return shown


def notify(text, title="Binance bullish cross", url=""):
    print(text, flush=True)
    if USE_SERVER and send_local_server(title, text, url):
        return
    try:
        send_desktop(title, text, url)
    except Exception as e:
        print(f"[warn] send_desktop: {e}")


def clock_ms():
    return int(time.time() * 1000)


class RateLimiter:
    def __init__(self, per_sec):
        self.gap = 1.0 / per_sec
        self.next = 0.0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            at = max(now, self.next)
            self.next = at + self.gap
        if at > now:
            time.sleep(at - now)


limiter = RateLimiter(REQS_PER_SEC)
_cooldown_until = 0.0
_ban_until = 0.0
_cooldown_lock = threading.Lock()


def retry_after(headers, default):
    try:
        return float(headers.get("Retry-After", default))
    except (TypeError, ValueError):
        return float(default)


def pick_base():
    global BASE
    problems = []
    for base in BINANCE_BASES:
        try:
            r = session.get(base + "/api/v3/ping", timeout=8)
            if r.status_code == 200:
                BASE = base
                return base
            problems.append(f"{base}: HTTP {r.status_code}")
        except Exception as e:
            problems.append(f"{base}: {type(e).__name__}")
    raise RuntimeError("cannot reach Binance (" + "; ".join(problems) + "). HTTP 451 means Binance blocks your "
                       "location: use a VPN, or set BINANCE_BASE=https://api.binance.us (Binance.US, fewer coins)")


def get(path, **params):
    global _cooldown_until, _ban_until
    if time.time() < _ban_until:
        raise RuntimeError(f"Binance IP ban active until {time.strftime('%H:%M:%S', time.localtime(_ban_until))}; "
                           f"not sending requests")
    for attempt in range(1, 4):
        wait = _cooldown_until - time.time()
        if wait > 0:
            time.sleep(wait)
        limiter.wait()
        r = session.get(BASE + path, params=params, timeout=10)
        headers = getattr(r, "headers", None) or {}
        if r.status_code == 418:
            pause = retry_after(headers, 120)
            _ban_until = time.time() + pause
            print(f"[warn] Binance has temporarily banned this IP (HTTP 418) for {pause:.0f}s; "
                  f"all requests are paused until then", flush=True)
            raise RuntimeError("IP banned by Binance (HTTP 418)")
        if r.status_code == 429:
            pause = min(max(retry_after(headers, 5 * attempt), 1), 120)
            with _cooldown_lock:
                if time.time() + pause > _cooldown_until + 1:
                    print(f"[warn] Binance rate limit (429): pausing all requests for {pause:.0f}s")
                _cooldown_until = max(_cooldown_until, time.time() + pause)
            continue
        if r.status_code in (403, 451):
            raise RuntimeError(f"HTTP {r.status_code} from Binance (is your location restricted?)")
        r.raise_for_status()
        try:
            used = int(headers.get("X-MBX-USED-WEIGHT-1M", 0))
        except (TypeError, ValueError):
            used = 0
        if used >= WEIGHT_SOFT_LIMIT:
            with _cooldown_lock:
                until = (int(time.time() // 60) + 1) * 60 + 1
                if until > _cooldown_until:
                    print(f"[warn] Binance request weight is {used}/6000 this minute; "
                          f"slowing down until the next minute", flush=True)
                _cooldown_until = max(_cooldown_until, until)
        return r.json()
    raise RuntimeError("still rate limited (429) after 3 tries")


def ema(values, length):
    if len(values) < length:
        return [None] * len(values)
    k = 2 / (length + 1)
    e = sum(values[:length]) / length
    out = [None] * (length - 1) + [e]
    for v in values[length:]:
        e = v * k + e * (1 - k)
        out.append(e)
    return out


def liquidity_stats(rows, i):
    start = int(rows[i][0]) - (LIQUIDITY_WINDOW - 1) * 60_000
    traded = 0.0
    active = 0
    high, low = 0.0, float("inf")
    j = i
    while j >= 0 and int(rows[j][0]) >= start:
        r = rows[j]
        vol = float(r[5])
        traded += float(r[7]) if len(r) > 7 else vol * float(r[4])
        if vol > 0:
            active += 1
        if float(r[3]) > 0:
            high, low = max(high, float(r[2])), min(low, float(r[3]))
        j -= 1
    close = float(rows[i][4])
    box = (high - low) / close * 100 if close > 0 and low != float("inf") else 0.0
    return {"traded": traded, "active": active, "box_pct": box, "spread_pct": None}


def liquidity_reject(stats):
    if stats["active"] < MIN_ACTIVE_MINUTES:
        return (f"dead: trades in only {stats['active']} of the last {LIQUIDITY_WINDOW} minutes "
                f"(min {MIN_ACTIVE_MINUTES})")
    if stats["box_pct"] < MIN_BOX_PCT:
        return (f"sideways: price stayed inside a {stats['box_pct']:.2f}% box for the last "
                f"{LIQUIDITY_WINDOW} min (min {MIN_BOX_PCT}%)")
    if MIN_HOURLY_QUOTE_VOLUME > 0 and stats["traded"] < MIN_HOURLY_QUOTE_VOLUME:
        return (f"low volume: only ${stats['traded']:,.0f} traded in the last {LIQUIDITY_WINDOW} min "
                f"(min ${MIN_HOURLY_QUOTE_VOLUME:,.0f})")
    return None


def closed_rows(rows, now_ms=None):
    """Drop the still-forming candle. Returns None when there is too little history to trust the EMAs."""
    now_ms = now_ms or clock_ms()
    if rows and int(rows[-1][0]) + 60_000 > now_ms:
        rows = rows[:-1]
    return rows if len(rows) >= SLOW + LOOKBACK + 1 else None


def ema_tail(closes):
    """EMA(FAST) and EMA(SLOW) in one pass, with the same arithmetic as ema() so results are identical.

    Returns (fast_last, slow_last, diffs) where diffs holds fast - slow for just the last LOOKBACK + 1 candles
    (diffs[0] belongs to candle len(closes) - LOOKBACK - 1). Needs len(closes) >= SLOW + LOOKBACK + 1.
    """
    first = len(closes) - LOOKBACK - 1
    fast = sum(closes[:FAST]) / FAST
    slow = sum(closes[:SLOW]) / SLOW
    for v in closes[FAST:SLOW]:
        fast = v * K_FAST + fast * (1 - K_FAST)
    for v in closes[SLOW:first]:
        fast = v * K_FAST + fast * (1 - K_FAST)
        slow = v * K_SLOW + slow * (1 - K_SLOW)
    diffs = []
    for v in closes[first:]:
        fast = v * K_FAST + fast * (1 - K_FAST)
        slow = v * K_SLOW + slow * (1 - K_SLOW)
        diffs.append(fast - slow)
    return fast, slow, diffs


def _crosses(rows, closes, diffs):
    n = len(rows)
    base = n - LOOKBACK - 1
    found = []
    for i in range(n - LOOKBACK, n):
        if not (diffs[i - 1 - base] <= 0 < diffs[i - base]):
            continue
        stats = liquidity_stats(rows, i)
        stats.update(taker_stats(rows, i))
        found.append({"candle_ms": int(rows[i][0]), "price": closes[i],
                      "reject": liquidity_reject(stats) or buyers_reject(stats), "stats": stats})
    return found


def find_crosses(rows, now_ms=None):
    rows = closed_rows(rows, now_ms)
    if rows is None:
        return []
    closes = [float(r[4]) for r in rows]
    return _crosses(rows, closes, ema_tail(closes)[2])


def spread_pct(symbol):
    t = get("/api/v3/ticker/bookTicker", symbol=symbol)
    bid, ask = float(t["bidPrice"]), float(t["askPrice"])
    if bid <= 0 or ask <= 0:
        return float("inf")
    return (ask - bid) / ((ask + bid) / 2) * 100


def taker_stats(rows, i):
    start = int(rows[i][0]) - (BUYER_WINDOW_MIN - 1) * 60_000
    buy = total = 0.0
    trades = 0
    j = i
    while j >= 0 and int(rows[j][0]) >= start:
        r = rows[j]
        if len(r) > 10:
            buy += float(r[10])
            total += float(r[7])
            trades += int(r[8])
        j -= 1
    return {"ratio": buy / total if total > 0 else None, "buy": buy, "sell": max(total - buy, 0.0), "trades": trades}


def buyers_reject(stats):
    if MIN_BUY_RATIO <= 0:
        return None
    if stats["ratio"] is None or stats["trades"] < MIN_TAPE_TRADES:
        return (f"thin tape: only {stats['trades']} trades in the last {BUYER_WINDOW_MIN} min, "
                f"cannot tell who is buying (min {MIN_TAPE_TRADES})")
    if stats["ratio"] < MIN_BUY_RATIO:
        return (f"more sellers: only {stats['ratio'] * 100:.0f}% of traded value is aggressive buying "
                f"(min {MIN_BUY_RATIO * 100:.0f}%)")
    return None


def verify(symbol, cross):
    if MAX_SPREAD_PCT <= 0:
        return
    stats = cross["stats"]
    try:
        stats["spread_pct"] = spread_pct(symbol)
    except Exception as e:
        cross["reject"] = f"unverified: spread unavailable ({e})"
        return
    if stats["spread_pct"] > MAX_SPREAD_PCT:
        shown = "no bids or no asks" if stats["spread_pct"] == float("inf") else f"{stats['spread_pct']:.2f}%"
        cross["reject"] = f"wide spread: {shown} (max {MAX_SPREAD_PCT}%)"


STATE = {}
WARMING = set()
NO_HISTORY = {}
RETRY = {}
MAX_RETRIES = 3


def _sync(symbol, rows, now_ms=None):
    """Rebuild STATE[symbol] from fresh candles. Returns (rows, closes, diffs) for cross detection, or None."""
    rows = closed_rows(rows, now_ms)
    if rows is None:
        return None
    closes = [float(r[4]) for r in rows]
    fast, slow, diffs = ema_tail(closes)
    STATE[symbol] = {"fast": fast, "slow": slow, "next": int(rows[-1][0]) // 60_000 + 1, "synced": time.time()}
    return rows, closes, diffs


def resync(symbol, rows, now_ms=None):
    return _sync(symbol, rows, now_ms) is not None


def advance(state, price, bucket):
    prev = state["fast"] - state["slow"]
    state["fast"] += K_FAST * (price - state["fast"])
    state["slow"] += K_SLOW * (price - state["slow"])
    state["next"] = bucket
    return prev, state["fast"] - state["slow"]


def snapshot():
    for attempt in range(1, 4):
        try:
            data = get("/api/v3/ticker/price")
            if isinstance(data, list) and data:
                return {t["symbol"]: float(t["price"]) for t in data}
            _warn_once("/ticker/price returned no list; using /ticker/24hr for prices instead")
            return {t["symbol"]: float(t["lastPrice"]) for t in get("/api/v3/ticker/24hr") if t.get("lastPrice")}
        except Exception:
            if attempt == 3:
                raise
            time.sleep(0.5)


def warm_task(symbol):
    try:
        rows = get("/api/v3/klines", symbol=symbol, interval=INTERVAL, limit=CANDLES)
        if not resync(symbol, rows):
            NO_HISTORY[symbol] = time.time() + 1800
    except Exception as e:
        print(f"[warn] warm-up {symbol}: {e}")
    finally:
        WARMING.discard(symbol)


def warm_up(symbols, pool):
    now = time.time()
    wanted = set(symbols)
    todo = [s for s in symbols if s not in STATE and s not in WARMING and NO_HISTORY.get(s, 0) <= now]
    aged = sorted((st["synced"], sym) for sym, st in list(STATE.items())
                  if sym in wanted and sym not in WARMING and now - st["synced"] > REWARM_AFTER_S)
    stale = [sym for _, sym in aged[:REWARM_PER_SCAN]]
    for symbol in todo + stale:
        WARMING.add(symbol)
        pool.submit(warm_task, symbol)
    return len(todo)


def check_symbol(symbol, handled=frozenset()):
    try:
        rows = get("/api/v3/klines", symbol=symbol, interval=INTERVAL, limit=CANDLES)
        synced = _sync(symbol, rows)
        found = _crosses(*synced) if synced else []
        crosses = [c for c in found if (symbol, c["candle_ms"]) not in handled]
        for c in crosses:
            if c["reject"] is None:
                verify(symbol, c)
        return symbol, crosses
    except Exception as e:
        print(f"[warn] {symbol}: {e}")
        return symbol, None


def load_tags():
    if not USE_BINANCE_TAGS:
        _warn_once("USE_BINANCE_TAGS=0: tokenized stocks/ETFs and tagged coins cannot be excluded "
                   "(they are only filtered through Binance sector tags)")
        return {}
    cached = None
    try:
        if TAGS_CACHE.exists():
            cached = json.loads(TAGS_CACHE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        cached = None
    if cached and time.time() - cached.get("ts", 0) < TAGS_TTL:
        return {k: set(v) for k, v in cached["tags"].items()}
    try:
        r = session.get(TAGS_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.raise_for_status()
        tags = {}
        for product in r.json().get("data") or []:
            base = str(product.get("b") or "").upper()
            found = {str(t).lower() for t in (product.get("tags") or [])}
            if base and found:
                tags.setdefault(base, set()).update(found)
        TAGS_CACHE.write_text(json.dumps({"ts": time.time(), "tags": {k: sorted(v) for k, v in tags.items()}}),
                              encoding="utf-8")
        if not tags:
            _warn_once("Binance returned no sector tags; using the built-in meme list and the 1000x/1M rule only")
        return tags
    except Exception as e:
        _warn_once(f"Binance sector tags unavailable ({e}); using the built-in meme list and the 1000x/1M rule only")
        return {k: set(v) for k, v in cached["tags"].items()} if cached else {}


def non_utility_reason(base, tags=()):
    if base in KEEP_COINS:
        return None
    if base in EXCLUDE_COINS:
        return "in EXCLUDE_COINS"
    if base in BLOCKLIST:
        return f"in {BLOCKLIST_FILE.name}"
    if base in STABLECOINS:
        return "stablecoin"
    if LEVERAGED_RE.match(base):
        return "leveraged token"
    if MICRO_RE.match(base):
        return "1000x/1M-scaled micro-priced token (almost always a meme)"
    for tag in tags:
        if tag in EXCLUDE_TAGS:
            return TAG_REASON.get(tag, f"tagged '{tag}'")
    if base in MEME_SEED:
        return "meme coin (built-in list)"
    return None



def load_market_caps(force=False):
    """Load unambiguous symbol -> USD market-cap mappings from a cached CoinGecko markets snapshot."""
    global MARKET_CAPS, MARKET_CAPS_LOADED_AT
    now = time.time()
    if MARKET_CAPS_LOADED_AT and not force and now - MARKET_CAPS_LOADED_AT < MARKET_CAP_TTL_S:
        return MARKET_CAPS

    stale_caps = {}
    try:
        if MARKET_CAP_CACHE.exists():
            cached = json.loads(MARKET_CAP_CACHE.read_text(encoding="utf-8"))
            cached_caps = cached.get("caps", {})
            if isinstance(cached_caps, dict):
                stale_caps = {str(k).upper(): float(v) for k, v in cached_caps.items()
                              if v is not None and float(v) > 0}
            if stale_caps and not force and now - float(cached.get("ts", 0)) < MARKET_CAP_TTL_S:
                MARKET_CAPS = stale_caps
                MARKET_CAPS_LOADED_AT = now
                return MARKET_CAPS
    except (OSError, ValueError, TypeError):
        stale_caps = {}

    found = {}
    try:
        # Resolve symbol collisions against the complete CoinGecko coin list, not just the top pages.
        listing_response = session.get(
            COINGECKO_LIST_URL, params={"include_platform": "false"},
            headers={"accept": "application/json", "User-Agent": "CryptoScanner/1.0"}, timeout=12,
        )
        listing_response.raise_for_status()
        listing = listing_response.json()
        counts = Counter(str(row.get("symbol") or "").strip().upper()
                         for row in listing if isinstance(row, dict) and row.get("symbol"))
        globally_unique = {symbol for symbol, count in counts.items() if count == 1}

        for page in range(1, COINGECKO_PAGES + 1):
            response = session.get(
                COINGECKO_MARKETS_URL,
                params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 250,
                        "page": page, "sparkline": "false"},
                headers={"accept": "application/json", "User-Agent": "CryptoScanner/1.0"},
                timeout=12,
            )
            response.raise_for_status()
            rows = response.json()
            if not isinstance(rows, list) or not rows:
                break
            for row in rows:
                symbol = str(row.get("symbol") or "").strip().upper()
                cap = row.get("market_cap")
                try:
                    cap = float(cap)
                except (TypeError, ValueError):
                    continue
                if symbol and cap > 0:
                    found.setdefault(symbol, []).append(cap)
            if len(rows) < 250:
                break
        # Never guess where CoinGecko has multiple assets with the same ticker symbol.
        fresh_caps = {symbol: values[0] for symbol, values in found.items()
                      if len(values) == 1 and symbol in globally_unique}
        if fresh_caps:
            MARKET_CAPS = fresh_caps
            try:
                MARKET_CAP_CACHE.parent.mkdir(parents=True, exist_ok=True)
                tmp = MARKET_CAP_CACHE.with_suffix(MARKET_CAP_CACHE.suffix + ".tmp")
                tmp.write_text(json.dumps({"ts": now, "caps": fresh_caps}), encoding="utf-8")
                tmp.replace(MARKET_CAP_CACHE)
            except OSError as e:
                _warn_once(f"could not cache CoinGecko market caps ({e}); using them until process exit")
        else:
            MARKET_CAPS = stale_caps
            _warn_once("CoinGecko returned no unambiguous market caps; cap-based filters may exclude unknown symbols")
    except Exception as e:
        MARKET_CAPS = stale_caps
        _warn_once(f"CoinGecko market caps unavailable ({e}); using cached caps where possible")
    MARKET_CAPS_LOADED_AT = now
    return MARKET_CAPS


def _load_volume_history_once():
    global _VOLUME_HISTORY_LOADED, VOLUME_HISTORY
    if _VOLUME_HISTORY_LOADED:
        return
    _VOLUME_HISTORY_LOADED = True
    try:
        raw = json.loads(VOLUME_HISTORY_FILE.read_text(encoding="utf-8"))
        symbols = raw.get("symbols", {})
        cutoff = time.time() - max(VOLUME_CHANGE_WINDOW_S * 3, 6 * 3600)
        cleaned = {}
        if isinstance(symbols, dict):
            for symbol, points in symbols.items():
                valid = []
                for point in points if isinstance(points, list) else []:
                    try:
                        ts, volume = float(point[0]), float(point[1])
                        if ts >= cutoff and volume >= 0:
                            valid.append([ts, volume])
                    except (TypeError, ValueError, IndexError):
                        continue
                if valid:
                    cleaned[str(symbol)] = sorted(valid, key=lambda x: x[0])[-100:]
        VOLUME_HISTORY = cleaned
    except (OSError, ValueError, TypeError):
        VOLUME_HISTORY = {}


def update_market_metrics(tickers, now=None):
    """Update live market data and keep enough volume snapshots to estimate volume momentum."""
    global MARKET_METRICS, LAST_MARKET_METRICS_REFRESH
    now = float(now or time.time())
    load_market_caps()
    _load_volume_history_once()
    metrics = {}
    cutoff = now - max(VOLUME_CHANGE_WINDOW_S * 3, 6 * 3600)
    append_gap = max(60, MARKET_DATA_REFRESH_S * 0.8)

    for ticker in tickers if isinstance(tickers, list) else []:
        symbol = str(ticker.get("symbol") or "").upper()
        if not symbol or not symbol.endswith(QUOTE):
            continue
        base = symbol[:-len(QUOTE)]
        try:
            quote_volume = float(ticker.get("quoteVolume") or 0)
            price_change = float(ticker.get("priceChangePercent"))
        except (TypeError, ValueError):
            continue

        history = VOLUME_HISTORY.setdefault(symbol, [])
        if not history or now - history[-1][0] >= append_gap:
            history.append([now, quote_volume])
        history[:] = [p for p in history if p[0] >= cutoff][-100:]

        volume_change = None
        target_ts = now - VOLUME_CHANGE_WINDOW_S
        tolerance = max(MARKET_DATA_REFRESH_S * 2, 900)
        older = [p for p in history if p[0] < now and abs(p[0] - target_ts) <= tolerance]
        if older:
            baseline = min(older, key=lambda p: abs(p[0] - target_ts))[1]
            if baseline > 0:
                volume_change = (quote_volume - baseline) / baseline * 100.0

        metrics[symbol] = {
            "base": base,
            "quote_volume": quote_volume,
            "market_cap_usd": MARKET_CAPS.get(base),
            "price_change_pct": price_change,
            "volume_change_pct": volume_change,
            "updated_at": now,
        }

    MARKET_METRICS = metrics
    LAST_MARKET_METRICS_REFRESH = now
    try:
        VOLUME_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = VOLUME_HISTORY_FILE.with_suffix(VOLUME_HISTORY_FILE.suffix + ".tmp")
        tmp.write_text(json.dumps({"ts": now, "symbols": VOLUME_HISTORY}), encoding="utf-8")
        tmp.replace(VOLUME_HISTORY_FILE)
    except OSError as e:
        _warn_once(f"could not save volume history ({e}); volume-change filters reset after restart")
    return metrics


def refresh_market_metrics(force=False):
    if not force and time.time() - LAST_MARKET_METRICS_REFRESH < MARKET_DATA_REFRESH_S:
        return False
    tickers = get("/api/v3/ticker/24hr")
    update_market_metrics(tickers)
    return True


def metric_filter_reason(symbol):
    metric = MARKET_METRICS.get(symbol, {})
    cap = metric.get("market_cap_usd")
    change = metric.get("price_change_pct")
    volume_change = metric.get("volume_change_pct")

    if MIN_MARKET_CAP_USD > 0 or MAX_MARKET_CAP_USD > 0:
        if cap is None:
            return "market cap unavailable/ambiguous"
        if MIN_MARKET_CAP_USD > 0 and cap < MIN_MARKET_CAP_USD:
            return f"market cap ${cap:,.0f} below ${MIN_MARKET_CAP_USD:,.0f}"
        if MAX_MARKET_CAP_USD > 0 and cap > MAX_MARKET_CAP_USD:
            return f"market cap ${cap:,.0f} above ${MAX_MARKET_CAP_USD:,.0f}"

    if MIN_24H_CHANGE_PCT is not None or MAX_24H_CHANGE_PCT is not None:
        if change is None:
            return "24h price change unavailable"
        if MIN_24H_CHANGE_PCT is not None and change < MIN_24H_CHANGE_PCT:
            return f"24h change {change:+.2f}% below {MIN_24H_CHANGE_PCT:+.2f}%"
        if MAX_24H_CHANGE_PCT is not None and change > MAX_24H_CHANGE_PCT:
            return f"24h change {change:+.2f}% above {MAX_24H_CHANGE_PCT:+.2f}%"

    if MIN_1H_VOLUME_CHANGE_PCT is not None or MAX_1H_VOLUME_CHANGE_PCT is not None:
        if volume_change is None:
            return "volume-change history warming up/unavailable"
        if MIN_1H_VOLUME_CHANGE_PCT is not None and volume_change < MIN_1H_VOLUME_CHANGE_PCT:
            return f"rolling 24h volume change {volume_change:+.1f}% below {MIN_1H_VOLUME_CHANGE_PCT:+.1f}%"
        if MAX_1H_VOLUME_CHANGE_PCT is not None and volume_change > MAX_1H_VOLUME_CHANGE_PCT:
            return f"rolling 24h volume change {volume_change:+.1f}% above {MAX_1H_VOLUME_CHANGE_PCT:+.1f}%"
    return None


def screen_symbols(symbols, verbose=False):
    screened = []
    rejected = Counter()
    for symbol in symbols:
        reason = metric_filter_reason(symbol)
        if reason:
            rejected[reason.split(":")[0]] += 1
            if verbose and VERBOSE:
                print(f"[screen reject] {symbol}: {reason}")
        else:
            screened.append(symbol)
    screened.sort(key=lambda sym: (-MARKET_METRICS.get(sym, {}).get("quote_volume", 0), sym))
    if verbose and rejected and VERBOSE:
        print("[screen] metric-filter rejects: " + ", ".join(f"{n} {why}" for why, n in rejected.most_common()))
    return screened


def _clamp01(value):
    return max(0.0, min(1.0, value))


def coin_score(symbol):
    """Transparent 0-100 screening score; weights: volume 35%, cap 20%, price change 25%, volume change 20%."""
    m = MARKET_METRICS.get(symbol, {})
    volume = max(1.0, float(m.get("quote_volume") or 0))
    cap = m.get("market_cap_usd")
    change = m.get("price_change_pct")
    volume_change = m.get("volume_change_pct")
    volume_score = _clamp01(math.log10(volume) / 10.0)
    cap_score = _clamp01(math.log10(max(float(cap), 1.0)) / 12.0) if cap else 0.5
    change_score = _clamp01((float(change) + 20.0) / 40.0) if change is not None else 0.5
    volume_change_score = _clamp01((float(volume_change) + 50.0) / 250.0) if volume_change is not None else 0.5
    return 100.0 * (0.35 * volume_score + 0.20 * cap_score +
                    0.25 * change_score + 0.20 * volume_change_score)


def _fmt_money(value):
    if value is None:
        return "n/a"
    value = float(value)
    for divisor, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(value) >= divisor:
            return f"${value / divisor:,.2f}{suffix}"
    return f"${value:,.0f}"


def print_coin_suggestions(symbols, top_n=None):
    if not symbols:
        print("[screen] no coins pass the current filters; check filter settings and volume-history warm-up", flush=True)
        return
    count = max(1, int(top_n or SUGGESTIONS_TOP_N))
    chosen = sorted(symbols, key=lambda sym: (-coin_score(sym),
                                             -MARKET_METRICS.get(sym, {}).get("quote_volume", 0), sym))[:count]
    print("\n[screen] Suggested coins — score uses volume 35%, market cap 20%, 24h price change 25%, "
          "rolling-24h volume change vs ~1h ago 20%", flush=True)
    print("Coin       Score   24h volume     Market cap      Price Δ24h   Rolling 24h vol Δ/~1h", flush=True)
    for symbol in chosen:
        m = MARKET_METRICS.get(symbol, {})
        change = m.get("price_change_pct")
        vol_change = m.get("volume_change_pct")
        change_text = f"{change:+8.2f}%" if change is not None else "     n/a"
        volume_change_text = f"{vol_change:+9.1f}%" if vol_change is not None else "      n/a"
        print(f"{symbol:<10} {coin_score(symbol):>5.1f}   {_fmt_money(m.get('quote_volume')):<13} "
              f"{_fmt_money(m.get('market_cap_usd')):<14} {change_text:>10}   {volume_change_text:>19}", flush=True)
    print("[screen] Filters: set MIN/MAX_MARKET_CAP_USD, MIN/MAX_24H_CHANGE_PCT, and/or "
          "MIN/MAX_1H_VOLUME_CHANGE_PCT. Missing/ambiguous market caps are not guessed.\n", flush=True)



def get_symbols():
    global BLOCKLIST, BASE_SCREEN_SYMBOLS, RANK
    BLOCKLIST = read_coin_list(BLOCKLIST_FILE)
    try:
        info = get("/api/v3/exchangeInfo", symbolStatus="TRADING")
    except Exception:
        info = get("/api/v3/exchangeInfo")
    bases = {
        s["symbol"]: s["baseAsset"].upper()
        for s in info["symbols"]
        if s.get("quoteAsset") == QUOTE
        and s.get("isSpotTradingAllowed", True)
        and str(s.get("status")).upper() == "TRADING"
    }
    tickers = get("/api/v3/ticker/24hr")
    update_market_metrics(tickers)
    ticker_by_symbol = {t["symbol"]: t for t in tickers if t.get("symbol")}
    volumes = {t["symbol"]: float(t.get("quoteVolume") or 0) for t in tickers if t.get("symbol")}
    prices = {t["symbol"]: float(t.get("lastPrice") or 0) for t in tickers if t.get("symbol")}
    tags = load_tags()
    excluded, eligible = {}, []
    for symbol, base in bases.items():
        if volumes.get(symbol, 0) < MIN_24H_QUOTE_VOLUME:
            continue
        price = prices.get(symbol, 0)
        if (MIN_PRICE and price < MIN_PRICE) or (MAX_PRICE and price > MAX_PRICE):
            continue
        why = non_utility_reason(base, tags.get(base, ()))
        if why:
            excluded[symbol] = why
        else:
            eligible.append(symbol)

    # Keep the utility/liquidity-filtered universe so metric filters can be re-evaluated on refresh.
    eligible.sort(key=lambda sym: (-volumes.get(sym, 0), sym))
    BASE_SCREEN_SYMBOLS = eligible
    screened = screen_symbols(eligible, verbose=True)
    RANK.clear()
    RANK.update({sym: (i + 1, volumes.get(sym, 0), len(screened)) for i, sym in enumerate(screened)})
    symbols = screened[:TOP_COINS] if TOP_COINS > 0 else screened
    if VERBOSE:
        for sym, why in sorted(excluded.items()):
            print(f"[excluded] {sym}: {why}")
    span = ""
    if symbols:
        lo, hi = min(prices[x] for x in symbols), max(prices[x] for x in symbols)
        span = f"; prices from {fmt_price(lo)} to {fmt_price(hi)}"
    active_filters = []
    if MIN_MARKET_CAP_USD > 0 or MAX_MARKET_CAP_USD > 0:
        active_filters.append(f"market cap ${MIN_MARKET_CAP_USD:,.0f}..${MAX_MARKET_CAP_USD:,.0f} (0=max/min disabled)")
    if MIN_24H_CHANGE_PCT is not None or MAX_24H_CHANGE_PCT is not None:
        active_filters.append(f"24h change {MIN_24H_CHANGE_PCT if MIN_24H_CHANGE_PCT is not None else '-∞'}.."
                              f"{MAX_24H_CHANGE_PCT if MAX_24H_CHANGE_PCT is not None else '+∞'}%")
    if MIN_1H_VOLUME_CHANGE_PCT is not None or MAX_1H_VOLUME_CHANGE_PCT is not None:
        active_filters.append(f"rolling-24h volume Δ/~1h {MIN_1H_VOLUME_CHANGE_PCT if MIN_1H_VOLUME_CHANGE_PCT is not None else '-∞'}.."
                              f"{MAX_1H_VOLUME_CHANGE_PCT if MAX_1H_VOLUME_CHANGE_PCT is not None else '+∞'}%")
    filter_text = "; active metric filters: " + ", ".join(active_filters) if active_filters else "; metric filters off (ranking still uses all four metrics)"
    print(f"Filter: {len(bases)} Binance spot {QUOTE} pairs, {len(excluded)} excluded as non-crypto/low quality, "
          f"{len(eligible)} passed utility and 24h volume/price filters, {len(screened)} passed metric screening "
          f"(24h volume floor ${MIN_24H_QUOTE_VOLUME:,.0f}){filter_text}; "
          f"scanning {len(symbols)}{span}", flush=True)
    return symbols


def fmt_price(price):
    text = f"{price:,.8f}".rstrip("0").rstrip(".")
    return text if text not in ("", "0") else f"{price:.3g}"


def tradingview_url(symbol):
    layout = f"{TV_LAYOUT_ID}/" if TV_LAYOUT_ID else ""
    return f"https://www.tradingview.com/chart/{layout}?symbol={TV_EXCHANGE}%3A{symbol}&interval=1"


def format_alert(symbol, candle_ms, price, stats=None):
    base = symbol[: -len(QUOTE)]
    t = time.strftime("%H:%M", time.localtime(candle_ms / 1000))
    lines = [f"🟢 {symbol}: EMA{FAST} crossed ABOVE EMA{SLOW} (bullish)",
             f"1m candle {t} | close {fmt_price(price)}"]
    if stats:
        rank = RANK.get(symbol)
        value = (f"last {LIQUIDITY_WINDOW} min: moved {stats['box_pct']:.2f}%, ${stats['traded']:,.0f} traded, "
                 f"{stats['active']}/{LIQUIDITY_WINDOW} active minutes")
        if rank:
            value = f"24h ${rank[1]:,.0f} (#{rank[0]} of {rank[2]}) | " + value
        lines.append(value)
        parts = []
        if stats.get("ratio") is not None:
            parts.append(f"Buyers: {stats['ratio'] * 100:.0f}% of traded value is aggressive buying "
                         f"(last {BUYER_WINDOW_MIN} min, {stats['trades']} trades)")
        if stats.get("spread_pct") is not None:
            parts.append(f"spread {stats['spread_pct']:.2f}%")
        if parts:
            lines.append(" | ".join(parts))
    lines += [tradingview_url(symbol), f"https://www.binance.com/en/trade/{base}_{QUOTE}?type=spot"]
    return "\n".join(lines)


def send_alert(symbol, cross):
    notify(format_alert(symbol, cross["candle_ms"], cross["price"], cross["stats"]), url=tradingview_url(symbol))


def scan_minute(symbols, prices, bucket, scan_pool, alert_pool, alerted):
    if FULL_SCAN:
        candidates, ready = list(symbols), len(symbols)
    else:
        candidates, ready = [], 0
        for symbol in symbols:
            state = STATE.get(symbol)
            if state is None:
                continue
            ready += 1
            price = prices.get(symbol)
            if not price or price <= 0 or state["next"] >= bucket:
                continue
            prev, curr = advance(state, price, bucket)
            eps = price * NEAR_PCT / 100
            if prev <= eps and curr > -eps:
                candidates.append(symbol)

    queued = set(candidates)
    candidates += [sym for sym in sorted(RETRY) if sym not in queued]
    # highest 24h volume first, so the coins that matter are fetched and alerted first
    candidates.sort(key=lambda sym: RANK.get(sym, (1 << 30, 0, 0))[0])

    seen = frozenset(alerted)
    futures = {scan_pool.submit(check_symbol, sym, seen): sym for sym in candidates}

    found = 0
    skipped = Counter()
    for future in as_completed(futures):
        try:
            symbol, crosses = future.result()
        except Exception as e:
            symbol, crosses = futures[future], None
            print(f"[warn] {symbol}: {e}")
        pending = crosses is None
        for cross in crosses or []:
            key = (symbol, cross["candle_ms"])
            if key in alerted:
                continue
            found += 1
            reject = cross["reject"]
            if reject and reject.startswith("unverified"):
                pending = True
                skipped["unverified"] += 1
                if VERBOSE:
                    print(f"[skip] {symbol}: {reject} (will retry next minute)")
                continue
            alerted.add(key)
            if reject:
                skipped[reject.split(":")[0]] += 1
                if VERBOSE:
                    print(f"[skip] {symbol}: {reject}")
                continue
            alert_pool.submit(send_alert, symbol, cross)
        if pending:
            RETRY[symbol] = RETRY.get(symbol, 0) + 1
            if RETRY[symbol] > MAX_RETRIES:
                RETRY.pop(symbol)
                print(f"[warn] {symbol}: giving up after {MAX_RETRIES} retries")
        else:
            RETRY.pop(symbol, None)

    cutoff = clock_ms() - 3_600_000
    alerted.difference_update({k for k in alerted if k[1] < cutoff})
    return {"ready": ready, "candidates": len(candidates), "found": found, "skipped": skipped}


def show_tags():
    TAGS_CACHE.unlink(missing_ok=True)
    pick_base()
    tags = load_tags()
    counts = Counter(t for found in tags.values() for t in found)
    print(f"Binance sector tags: {len(tags)} coins have tags")
    print("most common tags:", ", ".join(f"{t} ({n})" for t, n in counts.most_common(15)) or "none")
    memes = sorted(b for b, found in tags.items() if "meme" in found or "memes" in found)
    print(f"coins Binance tags as Meme ({len(memes)}):", ", ".join(memes[:60]) + (" ..." if len(memes) > 60 else ""))
    for coin in ("DOGE", "PEPE", "SHIB", "LINK", "BTC", "FET"):
        print(f"  {coin}: {sorted(tags.get(coin, [])) or 'no tags'} -> {non_utility_reason(coin, tags.get(coin, ())) or 'kept as utility'}")


def main():
    if "--tags" in sys.argv:
        try:
            return show_tags()
        except Exception as e:
            print(f"[error] {e}")
            return
    server = start_server() if RUN_SERVER else None
    if "--test" in sys.argv:
        notify("🟢 TEST: if you can see this popup, alerts work. Click it: BTCUSDT should open in TradingView.",
               url=tradingview_url("BTCUSDT"))
        print(f"Waiting {TEST_WAIT:.0f}s so you can click the popup (Ctrl+C to quit)...", flush=True)
        time.sleep(TEST_WAIT)
        return

    if server:
        print(f"Local alert server running at http://{ALERT_HOST}:{ALERT_PORT} (no browser needed)")
    elif not RUN_SERVER:
        print("Local alert server not started (RUN_SERVER=0); popups are shown directly")
    print("Popups: " + ("ON" if DESKTOP_NOTIFY else "OFF (DESKTOP_NOTIFY=0)"))
    try:
        print(f"Data source: Binance spot ({pick_base()})")
    except Exception as e:
        print(f"[error] {e}")
        return

    alerted = set()
    symbols, last_refresh = [], 0
    scan_pool = ThreadPoolExecutor(max_workers=SCAN_WORKERS)
    warm_pool = ThreadPoolExecutor(max_workers=WARM_WORKERS)
    alert_pool = ThreadPoolExecutor(max_workers=ALERT_WORKERS)

    notify("Scanner started.")
    while True:
        if not BASE_SCREEN_SYMBOLS or time.time() - last_refresh > REFRESH_SYMBOLS_EVERY:
            try:
                symbols = get_symbols()
                last_refresh = time.time()
            except Exception as e:
                print(f"[warn] could not load symbols: {e}")
                time.sleep(5)
                continue
            if symbols:
                print(f"Watching {len(symbols)} {QUOTE} pairs")
                print_coin_suggestions(symbols)

        # Refresh ticker-based factors and re-apply the screening filters without reloading exchange metadata.
        try:
            if BASE_SCREEN_SYMBOLS and time.time() - LAST_MARKET_METRICS_REFRESH >= MARKET_DATA_REFRESH_S:
                refresh_market_metrics(force=True)
                screened = screen_symbols(BASE_SCREEN_SYMBOLS, verbose=True)
                symbols = screened[:TOP_COINS] if TOP_COINS > 0 else screened
                RANK.clear()
                RANK.update({sym: (i + 1, MARKET_METRICS.get(sym, {}).get("quote_volume", 0), len(screened))
                             for i, sym in enumerate(screened)})
                print_coin_suggestions(symbols)
        except Exception as e:
            print(f"[warn] metric refresh failed; retaining previous screen ({e})", flush=True)

        if not symbols:
            print("[warn] 0 symbols currently pass screening; check thresholds or wait for volume-change history", flush=True)
            time.sleep(30)
            continue
        if not FULL_SCAN:
            queued = warm_up(symbols, warm_pool)
            if queued:
                print(f"Warming up {queued} coins in the background (highest traded value first); "
                      f"each starts being scanned as soon as it is ready", flush=True)

        time.sleep(60 - (time.time() % 60) + SCAN_DELAY)
        started = time.time()
        try:
            prices = snapshot()
        except Exception as e:
            print(f"[warn] price snapshot failed: {e}")
            continue
        info = scan_minute(symbols, prices, clock_ms() // 60_000, scan_pool, alert_pool, alerted)
        total = sum(info["skipped"].values())
        detail = ", ".join(f"{n} {why}" for why, n in info["skipped"].most_common())
        scope = ("all coins checked" if FULL_SCAN
                 else f"{info['ready']}/{len(symbols)} coins ready, {info['candidates']} fetched (near a cross or retrying)")
        print(f"Scan {time.strftime('%H:%M:%S')} took {time.time() - started:.1f}s | {scope} | "
              f"{info['found']} crosses, {total} skipped" + (f" ({detail})" if total else ""), flush=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
