import hmac
import json
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
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
import uvicorn
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

QUOTE = "USDT"
TOP_COINS = int(os.getenv("TOP_COINS", "0"))
MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "50000"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "0"))
FAST, SLOW = 20, 200
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
SCAN_DELAY = float(os.getenv("SCAN_DELAY", "2"))
NEAR_PCT = float(os.getenv("NEAR_PCT", "0.05"))
FULL_SCAN = os.getenv("FULL_SCAN", "0") == "1"
WARM_WORKERS = int(os.getenv("WARM_WORKERS", "12"))
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "16"))
REWARM_AFTER_S = 3 * 3600
REWARM_PER_SCAN = 5

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
local_session = requests.Session()
local_session.trust_env = False
RANK = {}

_actions_ok = None
_warned = set()


def _warn_once(message):
    if message not in _warned:
        _warned.add(message)
        print(f"[warn] {message}", flush=True)


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
                               headers=headers, timeout=5)
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


def find_crosses(rows, now_ms=None):
    now_ms = now_ms or clock_ms()
    if rows and int(rows[-1][0]) + 60_000 > now_ms:
        rows = rows[:-1]
    if len(rows) < SLOW + LOOKBACK + 1:
        return []

    closes = [float(r[4]) for r in rows]
    fast, slow = ema(closes, FAST), ema(closes, SLOW)
    diff = [f - s if f is not None and s is not None else None for f, s in zip(fast, slow)]

    found = []
    for i in range(len(rows) - LOOKBACK, len(rows)):
        prev, curr = diff[i - 1], diff[i]
        if prev is None or curr is None or not (prev <= 0 < curr):
            continue
        stats = liquidity_stats(rows, i)
        stats.update(taker_stats(rows, i))
        found.append({"candle_ms": int(rows[i][0]), "price": closes[i],
                      "reject": liquidity_reject(stats) or buyers_reject(stats), "stats": stats})
    return found


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
K_FAST, K_SLOW = 2 / (FAST + 1), 2 / (SLOW + 1)


def resync(symbol, rows, now_ms=None):
    now_ms = now_ms or clock_ms()
    if rows and int(rows[-1][0]) + 60_000 > now_ms:
        rows = rows[:-1]
    if len(rows) < SLOW + LOOKBACK + 1:
        return False
    closes = [float(r[4]) for r in rows]
    fast, slow = ema(closes, FAST), ema(closes, SLOW)
    STATE[symbol] = {"fast": fast[-1], "slow": slow[-1], "next": int(rows[-1][0]) // 60_000 + 1,
                     "synced": time.time()}
    return True


def advance(state, price, bucket):
    prev = state["fast"] - state["slow"]
    state["fast"] += K_FAST * (price - state["fast"])
    state["slow"] += K_SLOW * (price - state["slow"])
    state["next"] = bucket
    return prev, state["fast"] - state["slow"]


def snapshot():
    try:
        data = get("/api/v3/ticker/price")
        if isinstance(data, list) and data:
            return {t["symbol"]: float(t["price"]) for t in data}
        _warn_once("/ticker/price returned no list; using /ticker/24hr for prices instead")
    except Exception as e:
        _warn_once(f"/ticker/price failed ({e}); using /ticker/24hr for prices instead")
    return {t["symbol"]: float(t["lastPrice"]) for t in get("/api/v3/ticker/24hr") if t.get("lastPrice")}


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
        resync(symbol, rows)
        crosses = [c for c in find_crosses(rows) if (symbol, c["candle_ms"]) not in handled]
        for c in crosses:
            if c["reject"] is None:
                verify(symbol, c)
        return symbol, crosses
    except Exception as e:
        print(f"[warn] {symbol}: {e}")
        return symbol, None


def load_tags():
    if not USE_BINANCE_TAGS:
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
    if base in STABLECOINS:
        return "stablecoin"
    if MICRO_RE.match(base):
        return "1000x/1M-scaled micro-priced token (almost always a meme)"
    if any(t in ("meme", "memes") for t in tags):
        return "tagged Meme by Binance"
    if base in MEME_SEED:
        return "meme coin (built-in list)"
    return None


def get_symbols():
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
    volumes = {t["symbol"]: float(t.get("quoteVolume") or 0) for t in tickers}
    prices = {t["symbol"]: float(t.get("lastPrice") or 0) for t in tickers}
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
    ranked = sorted(eligible, key=lambda sym: (-volumes[sym], sym))
    RANK.clear()
    RANK.update({sym: (i + 1, volumes[sym], len(ranked)) for i, sym in enumerate(ranked)})
    symbols = ranked[:TOP_COINS] if TOP_COINS > 0 else ranked
    if VERBOSE:
        for sym, why in sorted(excluded.items()):
            print(f"[excluded] {sym}: {why}")
    span = ""
    if symbols:
        lo, hi = min(prices[x] for x in symbols), max(prices[x] for x in symbols)
        span = f"; prices from {lo:.8g} to {hi:,.8g}"
    print(f"Filter: {len(bases)} Binance spot {QUOTE} pairs, {len(excluded)} excluded as not utility "
          f"(memes, stablecoins), {len(ranked)} utility coins above the 24h floor (${MIN_24H_QUOTE_VOLUME:,.0f}); "
          f"scanning {len(symbols)}{span}")
    return symbols


def tradingview_url(symbol):
    layout = f"{TV_LAYOUT_ID}/" if TV_LAYOUT_ID else ""
    return f"https://www.tradingview.com/chart/{layout}?symbol={TV_EXCHANGE}%3A{symbol}&interval=1"


def format_alert(symbol, candle_ms, price, stats=None):
    base = symbol[: -len(QUOTE)]
    t = time.strftime("%H:%M", time.localtime(candle_ms / 1000))
    lines = [f"🟢 {symbol}: EMA{FAST} crossed ABOVE EMA{SLOW} (bullish)",
             f"1m candle {t} | close {price}"]
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

    found = 0
    skipped = Counter()
    for symbol, crosses in scan_pool.map(lambda sym: check_symbol(sym, alerted), candidates):
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


def main():
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

    alerted = set()
    symbols, last_refresh = [], 0
    scan_pool = ThreadPoolExecutor(max_workers=SCAN_WORKERS)
    warm_pool = ThreadPoolExecutor(max_workers=WARM_WORKERS)
    alert_pool = ThreadPoolExecutor(max_workers=2)

    notify("Scanner started.")
    while True:
        if not symbols or time.time() - last_refresh > REFRESH_SYMBOLS_EVERY:
            try:
                symbols = get_symbols()
                last_refresh = time.time()
            except Exception as e:
                print(f"[warn] could not load symbols: {e}")
                time.sleep(5)
                continue
            if not symbols:
                print("[warn] 0 symbols after filtering - lower MIN_24H_QUOTE_VOLUME")
                time.sleep(30)
                continue
            print(f"Watching {len(symbols)} {QUOTE} pairs")
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
