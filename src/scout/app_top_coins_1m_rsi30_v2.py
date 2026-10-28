"""1m spot scanner: alert when RSI(14) crosses down through 30.

Signals use closed 1m candles and standard Wilder RSI smoothing. An alert is created only
when RSI was above the configured level on the previous closed candle and is at/below it
on the current closed candle. It will not repeat every minute while RSI remains oversold;
a new alert requires RSI to recover above the level and cross down again. This is a signal
scanner, not an auto-trader. Test after fees and slippage before using for live trading.

By default, dynamically scans high-volume Binance spot USDT utility pairs. A pair must
clear a 24-hour notional-volume floor and prove that its hourly volume remains sustained;
one-off spikes and meme-tagged assets are excluded. The universe refreshes every five
minutes. Set DYNAMIC_UNIVERSE=0 and MAJORS=BTC,ETH,... to use a fixed curated list instead.

Run modes:
    python app_top_coins_1m_rsi30.py            scan forever
    python app_top_coins_1m_rsi30.py --test     send one test popup
    python app_top_coins_1m_rsi30.py --screen   rank the current universe once and exit
    python app_top_coins_1m_rsi30.py --tags     show Binance sector tags and exit
    python app_top_coins_1m_rsi30.py --gainers  only alert on coins up over 24h
"""
import hmac
import json
import logging
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
from pydantic import BaseModel, Field
from requests.adapters import HTTPAdapter

# ───────────────────────────── configuration ─────────────────────────────

QUOTE = "USDT"


def _opt_float(name):
    raw = os.getenv(name, "").strip()
    return float(raw) if raw else None


# Dynamic mode is the default. It selects sustained high-dollar-volume utility coins,
# rather than retaining yesterday's static watchlist. Set DYNAMIC_UNIVERSE=0 for a fixed MAJORS list.
# In fixed mode, MAJORS can be overridden with a comma-separated list and listed coins bypass
# the general meme/sector exclusions.
DYNAMIC_UNIVERSE = os.getenv("DYNAMIC_UNIVERSE", "1") == "1"
MAJORS_MODE = not DYNAMIC_UNIVERSE
DEFAULT_MAJORS = (
    "BTC,ETH,XRP,BNB,SOL,TRX,DOGE,ADA,HYPE,LINK,SUI,AVAX,TON,XLM,BCH,LTC,HBAR,"
    "DOT,UNI,NEAR,ETC,AAVE,FIL,ICP,ATOM,ARB,OP,APT,INJ,SEI,RENDER,FET,TAO,"
    "VET,ALGO,POL,GRT,CRV,ENA,PENDLE,WLD,LDO,KAS,XMR,ZEC,SHIB,PEPE,BONK,FLOKI,WIF,"
    "JUP,TIA,ONDO,MKR,SAND,MANA"
)
MAJORS = [c.strip().upper() for c in os.getenv("MAJORS", DEFAULT_MAJORS).split(",") if c.strip()]
# TOP_COINS remains supported; MOVER_COUNT is clearer for the dynamic universe.
TOP_COINS = int(os.getenv("TOP_COINS", "0"))
MOVER_COUNT = max(1, int(os.getenv("MOVER_COUNT", str(TOP_COINS or 80))))
# A $2M floor leaves enough liquid utility pairs to build a varied watchlist while still excluding the
# thin tail of Binance's market. Favor actively traded daily gainers so an RSI-30 touch is a pullback
# candidate, not a coin already collapsing.
MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "2000000"))
MIN_ABS_CHANGE_24H = float(os.getenv("MIN_ABS_CHANGE_24H", "1"))
MOVER_DIRECTION = os.getenv("MOVER_DIRECTION", "up").strip().lower()
if MOVER_DIRECTION not in {"both", "up", "down"}:
    raise ValueError("MOVER_DIRECTION must be one of: both, up, down")
# A rolling 24h ticker can remain large after a one-off pump. Before choosing the watchlist, compare the
# last six completed hourly candles with the preceding 18. This keeps coins with sustained trader
# attention and rejects volume that has faded.
ATTENTION_CANDIDATES = max(MOVER_COUNT, int(os.getenv("ATTENTION_CANDIDATES", "200")))
ATTENTION_RECENT_HOURS = max(2, int(os.getenv("ATTENTION_RECENT_HOURS", "6")))
ATTENTION_BASELINE_HOURS = max(2, int(os.getenv("ATTENTION_BASELINE_HOURS", "18")))
MIN_RECENT_HOURLY_QUOTE_VOLUME = float(os.getenv("MIN_RECENT_HOURLY_QUOTE_VOLUME", "100000"))
MIN_VOLUME_RETENTION = float(os.getenv("MIN_VOLUME_RETENTION", "0.45"))
MAX_SINGLE_HOUR_VOLUME_SHARE = float(os.getenv("MAX_SINGLE_HOUR_VOLUME_SHARE", "0.70"))
MIN_ACTIVE_ATTENTION_HOURS = max(1, int(os.getenv("MIN_ACTIVE_ATTENTION_HOURS", "4")))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "0"))

# Screening (all off by default). Market cap in USD, changes in percent. Empty/0 = off.
MIN_MARKET_CAP = float(os.getenv("MIN_MARKET_CAP", "0"))      # e.g. 50000000
MAX_MARKET_CAP = float(os.getenv("MAX_MARKET_CAP", "0"))      # e.g. 5000000000 to skip mega caps
# Gainers-only mode (--gainers, or GAINERS_ONLY=1): alert only on coins up over the last 24h.
# Raise the bar with MIN_CHANGE_24H, e.g. MIN_CHANGE_24H=3 for coins up at least 3%.
GAINERS_MODE = "--gainers" in sys.argv or os.getenv("GAINERS_ONLY", "0") == "1"
MIN_CHANGE_24H = _opt_float("MIN_CHANGE_24H")                 # e.g. -5 skips coins down more than 5% on the day
if GAINERS_MODE and MIN_CHANGE_24H is None:
    MIN_CHANGE_24H = 0.0
MAX_CHANGE_24H = _opt_float("MAX_CHANGE_24H")                 # e.g. 40 skips coins already up more than 40%
MIN_VOL_SURGE = float(os.getenv("MIN_VOL_SURGE", "0"))        # e.g. 1.5 = last hour at least 1.5x its normal volume
SHOW_MARKET_CAP = os.getenv("SHOW_MARKET_CAP", "0") == "1"    # show cap in --screen even with no cap filter
SCREEN_MODE = "--screen" in sys.argv
USE_MCAP = MIN_MARKET_CAP > 0 or MAX_MARKET_CAP > 0 or SHOW_MARKET_CAP or SCREEN_MODE
MCAP_PAGES = int(os.getenv("MCAP_PAGES", "4"))                # 250 coins per page, ranked by market cap
MCAP_TTL = 6 * 3600
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
COINGECKO_KEY = os.getenv("COINGECKO_API_KEY", "").strip()    # optional free demo key, raises rate limits
MCAP_CACHE = Path(os.getenv("MCAP_CACHE", Path(__file__).resolve().parent / "market_caps.json"))
SCREEN_TOP = int(os.getenv("SCREEN_TOP", "30"))

# RSI oversold-cross signal settings.
RSI_PERIOD = max(2, int(os.getenv("RSI_PERIOD", "14")))
RSI_LEVEL = float(os.getenv("RSI_LEVEL", "30"))
if not 0 < RSI_LEVEL < 100:
    raise ValueError("RSI_LEVEL must be between 0 and 100 (exclusive)")
INTERVAL = "1m"
INTERVAL_MINUTES = 1
INTERVAL_MS = INTERVAL_MINUTES * 60_000
LOOKBACK = 3   # closed candles re-checked each scan, so a delayed scan can still catch up
# Wilder smoothing forgets its seed after a few multiples of the period; fewer candles than this
# would give an RSI that has not settled yet.
MIN_CANDLES = RSI_PERIOD * 5 + LOOKBACK + 1
CANDLES = min(1000, max(MIN_CANDLES, int(os.getenv("CANDLES", "300"))))
LIQUIDITY_WINDOW = 60
# Candle-based quality gates. They reuse candles already fetched for the RSI, so they cost no requests.
# A sideways-box gate makes little sense for an oversold signal, so it defaults to off here.
MIN_ACTIVE_MINUTES = int(os.getenv("MIN_ACTIVE_MINUTES", "30"))
MIN_BOX_PCT = float(os.getenv("MIN_BOX_PCT", "0"))
MIN_HOURLY_QUOTE_VOLUME = float(os.getenv("MIN_HOURLY_QUOTE_VOLUME", "0"))
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0"))
VERBOSE = os.getenv("VERBOSE", "0") == "1"
REQS_PER_SEC = float(os.getenv("REQS_PER_SEC", "30"))
WEIGHT_SOFT_LIMIT = int(os.getenv("WEIGHT_SOFT_LIMIT", "4800"))
REFRESH_SYMBOLS_EVERY = int(os.getenv("REFRESH_SYMBOLS_EVERY", "300" if DYNAMIC_UNIVERSE else "3600"))
SCAN_DELAY = float(os.getenv("SCAN_DELAY", "1"))
SCAN_WORKERS = int(os.getenv("SCAN_WORKERS", "16"))
ALERT_WORKERS = int(os.getenv("ALERT_WORKERS", "8"))

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
EXCLUDE_MEMES = os.getenv("EXCLUDE_MEMES", "1") == "1"
EXCLUDE_TAGS = {t.strip().lower() for t in
                os.getenv("EXCLUDE_TAGS", "bstocks,tcommodities,stablecoin" +
                          (",meme,memes" if EXCLUDE_MEMES else "")).split(",") if t.strip()}
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

# ───────────────────────────── shared state ─────────────────────────────

logging.basicConfig(level=logging.DEBUG if VERBOSE else logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("rsi30")

BASE = BINANCE_BASES[0]
session = requests.Session()
# requests keeps only 10 connections per host by default. With SCAN_WORKERS threads in flight the rest
# were opened, used once and thrown away, so most calls paid a fresh TCP + TLS handshake.
_adapter = HTTPAdapter(pool_connections=10, pool_maxsize=SCAN_WORKERS + 4)
session.mount("https://", _adapter)
session.mount("http://", _adapter)
local_session = requests.Session()
local_session.trust_env = False

# These four are rebuilt by get_symbols() and REPLACED (never cleared in place), so alert threads that
# are still formatting a message during a universe refresh always see a complete, consistent snapshot.
RANK = {}       # symbol -> (rank, 24h quote volume, universe size)
CHANGE = {}     # symbol -> 24h price change %
MCAP = {}       # symbol -> market cap in USD (only when USE_MCAP and CoinGecko knows the coin)
ATTENTION = {}  # symbol -> sustained hourly-volume metrics used for dynamic ranking

_actions_ok = None
_warned = set()


def _warn_once(message):
    if message not in _warned:
        _warned.add(message)
        log.warning(message)


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


def _save_cache(path, payload):
    """Write a JSON cache atomically; a read-only folder must never crash the scanner."""
    tmp = path.with_name(path.name + ".tmp")
    try:
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        tmp.replace(path)
    except OSError as e:
        _warn_once(f"could not write cache {path.name}: {e}")


def _load_cache(path):
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
    except (OSError, ValueError):
        return None


# ───────────────────────────── notifications ─────────────────────────────

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
    log.info("alert: %s (popup %s)", alert.text.splitlines()[0], "shown" if popup else "NOT shown")
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
    log.warning("could not start the local alert server on %s:%s (port in use?). If another alert server is "
                "running there it will be used; otherwise popups are shown directly.", ALERT_HOST, ALERT_PORT)
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
            log.warning("alert server not reachable at %s (%s); showing popups directly instead.",
                        LOCAL_SERVER_URL, e)
        return False
    _server_warned = False
    return shown


def notify(text, title="RSI scanner", url=""):
    print(text, flush=True)
    if USE_SERVER and send_local_server(title, text, url):
        return
    try:
        send_desktop(title, text, url)
    except Exception as e:
        log.warning("send_desktop: %s", e)


# ───────────────────────────── Binance HTTP ─────────────────────────────

_clock_offset_ms = 0   # server time minus local time, so "is this candle closed?" is judged on Binance's clock


def clock_ms():
    return int(time.time() * 1000) + _clock_offset_ms


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
_base_lock = threading.Lock()


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


def _next_base(failed):
    """Rotate to the next mirror once `failed` stops answering (only the first thread to notice switches)."""
    global BASE
    with _base_lock:
        if BASE == failed and len(BINANCE_BASES) > 1:
            BASE = BINANCE_BASES[(BINANCE_BASES.index(failed) + 1) % len(BINANCE_BASES)]
            log.warning("Binance endpoint %s is not answering; switching to %s", failed, BASE)


def get(path, **params):
    global _cooldown_until, _ban_until
    if time.time() < _ban_until:
        raise RuntimeError(f"Binance IP ban active until {time.strftime('%H:%M:%S', time.localtime(_ban_until))}; "
                           f"not sending requests")
    last_error = "unknown error"
    for attempt in range(1, 4):
        wait = _cooldown_until - time.time()
        if wait > 0:
            time.sleep(wait)
        limiter.wait()
        base = BASE
        try:
            r = session.get(base + path, params=params, timeout=10)
        except (requests.ConnectionError, requests.Timeout) as e:
            last_error = type(e).__name__
            _next_base(base)
            time.sleep(0.5 * attempt)
            continue
        headers = getattr(r, "headers", None) or {}
        if r.status_code == 418:
            pause = retry_after(headers, 120)
            _ban_until = time.time() + pause
            log.warning("Binance has temporarily banned this IP (HTTP 418) for %.0fs; "
                        "all requests are paused until then", pause)
            raise RuntimeError("IP banned by Binance (HTTP 418)")
        if r.status_code == 429:
            last_error = "rate limited (429)"
            pause = min(max(retry_after(headers, 5 * attempt), 1), 120)
            with _cooldown_lock:
                if time.time() + pause > _cooldown_until + 1:
                    log.warning("Binance rate limit (429): pausing all requests for %.0fs", pause)
                _cooldown_until = max(_cooldown_until, time.time() + pause)
            continue
        if r.status_code in (403, 451):
            raise RuntimeError(f"HTTP {r.status_code} from Binance (is your location restricted?)")
        if r.status_code >= 500:
            last_error = f"HTTP {r.status_code}"
            time.sleep(0.5 * attempt)
            continue
        r.raise_for_status()
        try:
            used = int(headers.get("X-MBX-USED-WEIGHT-1M", 0))
        except (TypeError, ValueError):
            used = 0
        if used >= WEIGHT_SOFT_LIMIT:
            with _cooldown_lock:
                until = (int(time.time() // 60) + 1) * 60 + 1
                if until > _cooldown_until:
                    log.warning("Binance request weight is %s/6000 this minute; slowing down until the next "
                                "minute", used)
                _cooldown_until = max(_cooldown_until, until)
        return r.json()
    raise RuntimeError(f"{path}: failed after 3 tries ({last_error})")


def sync_clock():
    """Measure the offset between this computer's clock and Binance's, compensating for request latency."""
    global _clock_offset_ms
    try:
        t0 = time.time()
        server_ms = int(get("/api/v3/time")["serverTime"])
        t1 = time.time()
    except Exception as e:
        _warn_once(f"could not read Binance server time ({e}); using the local clock")
        return
    _clock_offset_ms = server_ms - int((t0 + t1) / 2 * 1000)
    if abs(_clock_offset_ms) > 1000:
        log.warning("local clock differs from Binance by %.1fs; compensating so unfinished candles "
                    "are never treated as closed", _clock_offset_ms / 1000)


# ───────────────────────────── indicators ─────────────────────────────

def _quote_volume(row):
    return float(row[7]) if len(row) > 7 else float(row[5]) * float(row[4])


def rsi_series(closes, period=14):
    """Return Wilder-smoothed RSI values, aligned to closes (None during warm-up)."""
    out = [None] * len(closes)
    if period < 2 or len(closes) <= period:
        return out

    def to_rsi(gain, loss):
        if loss == 0:
            return 100.0 if gain > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + gain / loss)

    gains = losses = 0.0
    for i in range(1, period + 1):
        change = closes[i] - closes[i - 1]
        gains += max(change, 0.0)
        losses += max(-change, 0.0)
    avg_gain, avg_loss = gains / period, losses / period
    out[period] = to_rsi(avg_gain, avg_loss)
    for i in range(period + 1, len(closes)):
        change = closes[i] - closes[i - 1]
        avg_gain = ((period - 1) * avg_gain + max(change, 0.0)) / period
        avg_loss = ((period - 1) * avg_loss + max(-change, 0.0)) / period
        out[i] = to_rsi(avg_gain, avg_loss)
    return out


def closed_rows(rows, now_ms=None):
    """Drop the still-forming candle. Returns None when there is too little history for a settled RSI."""
    now_ms = clock_ms() if now_ms is None else now_ms
    if rows and int(rows[-1][0]) + INTERVAL_MS > now_ms:
        rows = rows[:-1]
    return rows if len(rows) >= MIN_CANDLES else None


def _crosses(rows, closes):
    """Fresh RSI crosses within the last LOOKBACK closed candles: previous RSI > level, current RSI <= level."""
    n = len(rows)
    values = rsi_series(closes, RSI_PERIOD)
    found = []
    for i in range(max(RSI_PERIOD + 1, n - LOOKBACK), n):
        prev_rsi, curr_rsi = values[i - 1], values[i]
        if prev_rsi is None or curr_rsi is None:
            continue
        if prev_rsi > RSI_LEVEL and curr_rsi <= RSI_LEVEL:
            found.append({
                "signal": "rsi_touch",
                "index": i,
                "candle_ms": int(rows[i][0]),
                "price": closes[i],
                "reject": None,
                "stats": {"rsi": curr_rsi, "previous_rsi": prev_rsi},
            })
    return found


def find_crosses(rows, now_ms=None):
    rows = closed_rows(rows, now_ms)
    if rows is None:
        return []
    return _crosses(rows, [float(r[4]) for r in rows])


# ───────────────────────────── candle-based gates ─────────────────────────────

def liquidity_stats(rows, i):
    """Traded value, active minutes and high/low box over the LIQUIDITY_WINDOW minutes ending at candle i."""
    start = int(rows[i][0]) - max(LIQUIDITY_WINDOW - INTERVAL_MINUTES, 0) * 60_000
    traded = 0.0
    active = 0
    high, low = 0.0, float("inf")
    j = i
    while j >= 0 and int(rows[j][0]) >= start:
        r = rows[j]
        if float(r[5]) > 0:
            active += INTERVAL_MINUTES
        traded += _quote_volume(r)
        if float(r[3]) > 0:
            high, low = max(high, float(r[2])), min(low, float(r[3]))
        j -= 1
    close = float(rows[i][4])
    box = (high - low) / close * 100 if close > 0 and low != float("inf") else 0.0
    return {"traded": traded, "active": active, "box_pct": box}


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


def volume_surge(rows, i, traded):
    """Volume of the last LIQUIDITY_WINDOW minutes divided by the average same-length window before it.

    Uses only candles already fetched (CANDLES minutes), so it costs no extra request.
    None if there is no baseline.
    """
    total = sum(_quote_volume(r) for r in rows[: i + 1])
    window_candles = max(1, (LIQUIDITY_WINDOW + INTERVAL_MINUTES - 1) // INTERVAL_MINUTES)
    before_candles = i + 1 - window_candles
    baseline = (total - traded) / before_candles * window_candles if before_candles > 0 else 0.0
    return traded / baseline if baseline > 0 else None


def surge_reject(surge):
    if MIN_VOL_SURGE <= 0:
        return None
    if surge is None:
        return "low surge: no earlier volume to compare against"
    if surge < MIN_VOL_SURGE:
        return (f"low surge: last {LIQUIDITY_WINDOW} min volume is only {surge:.1f}x its normal level "
                f"(min {MIN_VOL_SURGE:g}x)")
    return None


def candle_reject(rows, cross):
    """Liquidity and volume-surge gates evaluated on the signal candle, using candles already in memory."""
    stats = liquidity_stats(rows, cross["index"])
    cross["stats"]["traded_window"] = stats["traded"]
    reason = liquidity_reject(stats)
    if reason:
        return reason
    if MIN_VOL_SURGE > 0:
        return surge_reject(volume_surge(rows, cross["index"], stats["traded"]))
    return None


# ───────────────────────────── per-signal verification ─────────────────────────────

def spread_pct(symbol):
    t = get("/api/v3/ticker/bookTicker", symbol=symbol)
    bid, ask = float(t["bidPrice"]), float(t["askPrice"])
    if bid <= 0 or ask <= 0:
        return float("inf")
    return (ask - bid) / ((ask + bid) / 2) * 100


def change_24h(symbol):
    return float(get("/api/v3/ticker/24hr", symbol=symbol)["priceChangePercent"])


def change_reject(change):
    """Why a 24h price change fails the gainer / weak-day / overextended gates, or None."""
    if MIN_CHANGE_24H is not None and (change < MIN_CHANGE_24H or (GAINERS_MODE and change <= 0)):
        if GAINERS_MODE:
            need = "above 0%" if MIN_CHANGE_24H <= 0 else f"at least {MIN_CHANGE_24H:g}%"
            return f"not a gainer: 24h change {change:+.1f}% (needs {need})"
        return f"weak day: 24h change {change:+.1f}% (min {MIN_CHANGE_24H:+g}%)"
    if MAX_CHANGE_24H is not None and change > MAX_CHANGE_24H:
        return f"overextended: 24h change {change:+.1f}% (max {MAX_CHANGE_24H:+g}%)"
    return None


def verify(symbol, cross):
    """Gates that may need data beyond the candles. Sets cross['reject'] to a reason, or leaves it None.

    A reason starting with 'unverified' means the data was unavailable, not that the coin failed.
    """
    stats = cross["stats"]
    if MIN_CHANGE_24H is not None or MAX_CHANGE_24H is not None:
        # The universe refresh already holds every watched coin's 24h change (at most a few minutes old,
        # irrelevant for a 24h figure), so only fall back to a request for coins it does not know.
        change = CHANGE.get(symbol)
        if change is None:
            try:
                change = change_24h(symbol)
            except Exception as e:
                cross["reject"] = f"unverified: 24h change unavailable ({e})"
                return
        stats["change_24h"] = change
        cross["reject"] = change_reject(change)
        if cross["reject"]:
            return
    if MAX_SPREAD_PCT <= 0:
        return
    try:
        stats["spread_pct"] = spread_pct(symbol)
    except Exception as e:
        cross["reject"] = f"unverified: spread unavailable ({e})"
        return
    if stats["spread_pct"] > MAX_SPREAD_PCT:
        shown = "no bids or no asks" if stats["spread_pct"] == float("inf") else f"{stats['spread_pct']:.2f}%"
        cross["reject"] = f"wide spread: {shown} (max {MAX_SPREAD_PCT}%)"


def check_symbol(symbol, handled=frozenset()):
    """Fetch candles, find fresh RSI crosses and run every gate. Returns (symbol, crosses) or (symbol, None) on error."""
    try:
        rows = closed_rows(get("/api/v3/klines", symbol=symbol, interval=INTERVAL, limit=CANDLES))
        if rows is None:   # too little history (new listing): nothing to evaluate yet
            return symbol, []
        closes = [float(r[4]) for r in rows]
        crosses = [c for c in _crosses(rows, closes) if (symbol, c["candle_ms"]) not in handled]
        for c in crosses:
            c["reject"] = candle_reject(rows, c)
            if c["reject"] is None:
                verify(symbol, c)
        return symbol, crosses
    except Exception as e:
        log.warning("%s: %s", symbol, e)
        return symbol, None


# ───────────────────────────── universe selection ─────────────────────────────

def load_tags():
    if not USE_BINANCE_TAGS:
        _warn_once("USE_BINANCE_TAGS=0: tokenized stocks/ETFs and tagged coins cannot be excluded "
                   "(they are only filtered through Binance sector tags)")
        return {}
    cached = _load_cache(TAGS_CACHE)
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
        if tags:
            _save_cache(TAGS_CACHE, {"ts": time.time(), "tags": {k: sorted(v) for k, v in tags.items()}})
        else:
            _warn_once("Binance returned no sector tags; using the built-in meme list and the 1000x/1M rule only")
        return tags
    except Exception as e:
        _warn_once(f"Binance sector tags unavailable ({e}); using the built-in meme list and the 1000x/1M rule only")
        return {k: set(v) for k, v in cached["tags"].items()} if cached else {}


def load_market_caps():
    """Ticker -> market cap in USD for CoinGecko's top MCAP_PAGES * 250 coins, cached for MCAP_TTL.

    Returns {} if unavailable. Tickers are not unique across coins, so the largest cap wins for a shared ticker.
    """
    cached = _load_cache(MCAP_CACHE)
    if cached and time.time() - cached.get("ts", 0) < MCAP_TTL:
        return cached["caps"]
    headers = {"User-Agent": "Mozilla/5.0"}
    if COINGECKO_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_KEY
    try:
        caps = {}
        for page in range(1, MCAP_PAGES + 1):
            r = session.get(COINGECKO_URL, headers=headers, timeout=15,
                            params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": 250, "page": page})
            r.raise_for_status()
            coins = r.json()
            for coin in coins:
                ticker = str(coin.get("symbol") or "").upper()
                cap = float(coin.get("market_cap") or 0)
                if ticker and cap > caps.get(ticker, 0.0):
                    caps[ticker] = cap
            if len(coins) < 250:
                break
            time.sleep(2)  # the keyless tier is rate limited by IP
        _save_cache(MCAP_CACHE, {"ts": time.time(), "caps": caps})
        return caps
    except Exception as e:
        _warn_once(f"market caps unavailable ({e}); MIN/MAX_MARKET_CAP are ignored until CoinGecko responds"
                   + (" (using an older cached copy)" if cached else ""))
        return cached["caps"] if cached else {}


def cap_reason(cap, caps_loaded):
    """Why a coin fails the market cap filter, or None. Coins missing from CoinGecko's list count as small."""
    if not (MIN_MARKET_CAP or MAX_MARKET_CAP) or not caps_loaded:
        return None
    if cap is None:
        return "market cap unknown (outside CoinGecko's top coins)" if MIN_MARKET_CAP else None
    if MIN_MARKET_CAP and cap < MIN_MARKET_CAP:
        return f"market cap ${cap:,.0f} below ${MIN_MARKET_CAP:,.0f}"
    if MAX_MARKET_CAP and cap > MAX_MARKET_CAP:
        return f"market cap ${cap:,.0f} above ${MAX_MARKET_CAP:,.0f}"
    return None


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
    if EXCLUDE_MEMES and base in MEME_SEED:
        return "meme coin (built-in list)"
    return None


_attention_cache = {}   # symbol -> (hour bucket, metrics)
HOUR_MS = 3_600_000


def attention_metrics(symbol):
    """Return completed-hour volume metrics, or None when the pair lacks enough history.

    The metrics only use COMPLETED hours, so they cannot change until the next hour closes. They are cached
    per hour, which cuts the dynamic universe's refresh from ~200 requests every 5 minutes to ~200 per hour.
    """
    now = clock_ms()
    bucket = now // HOUR_MS
    hit = _attention_cache.get(symbol)
    if hit and hit[0] == bucket:
        return hit[1]
    rows = get("/api/v3/klines", symbol=symbol, interval="1h",
               limit=ATTENTION_RECENT_HOURS + ATTENTION_BASELINE_HOURS + 1)
    # Binance includes the currently forming hour, which must not count as a quiet hour.
    rows = [row for row in rows if len(row) > 7 and int(row[6]) < now]
    needed = ATTENTION_RECENT_HOURS + ATTENTION_BASELINE_HOURS
    stats = None
    if len(rows) >= needed:
        volumes = [float(row[7]) for row in rows[-needed:]]
        baseline, recent = volumes[:-ATTENTION_RECENT_HOURS], volumes[-ATTENTION_RECENT_HOURS:]
        recent_total = sum(recent)
        recent_average = recent_total / len(recent)
        baseline_average = sum(baseline) / len(baseline)
        stats = {
            "recent_average": recent_average,
            "retention": recent_average / baseline_average if baseline_average > 0 else 0.0,
            "peak_share": max(recent) / recent_total if recent_total > 0 else 1.0,
            "active_hours": sum(volume > 0 for volume in recent),
        }
    _attention_cache[symbol] = (bucket, stats)
    return stats


def attention_reason(stats):
    """Why hourly volume does not show durable trader attention, or None."""
    if stats is None:
        return "not enough completed hourly volume history"
    if stats["recent_average"] < MIN_RECENT_HOURLY_QUOTE_VOLUME:
        return (f"recent hourly volume ${stats['recent_average']:,.0f} below "
                f"${MIN_RECENT_HOURLY_QUOTE_VOLUME:,.0f}")
    if stats["retention"] < MIN_VOLUME_RETENTION:
        return (f"recent volume retained only {stats['retention']:.0%} of its prior "
                f"{ATTENTION_BASELINE_HOURS}h baseline")
    if stats["peak_share"] > MAX_SINGLE_HOUR_VOLUME_SHARE:
        return (f"one hour supplied {stats['peak_share']:.0%} of recent volume "
                f"(max {MAX_SINGLE_HOUR_VOLUME_SHARE:.0%})")
    if stats["active_hours"] < MIN_ACTIVE_ATTENTION_HOURS:
        return f"traded in only {stats['active_hours']} of the last {ATTENTION_RECENT_HOURS} hours"
    return None


def sustained_attention(symbols):
    """Fetch a bounded set of hourly histories concurrently and retain durable activity only."""
    accepted, rejected = {}, Counter()
    with ThreadPoolExecutor(max_workers=min(SCAN_WORKERS, len(symbols))) as pool:
        futures = {pool.submit(attention_metrics, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                stats = future.result()
                reason = attention_reason(stats)
            except Exception as e:
                stats, reason = None, f"hourly volume unavailable ({type(e).__name__})"
            if reason:
                rejected[reason] += 1
            else:
                accepted[symbol] = stats
    return accepted, rejected


def fmt_price(price):
    text = f"{price:,.8f}".rstrip("0").rstrip(".")
    return text if text not in ("", "0") else f"{price:.3g}"


def fmt_big(value):
    for limit, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if value >= limit:
            return f"${value / limit:.2f}{suffix}"
    return f"${value:,.0f}"


def _passes_move_filter(change):
    if MIN_ABS_CHANGE_24H <= 0:
        return True
    if abs(change) < MIN_ABS_CHANGE_24H:
        return False
    if MOVER_DIRECTION == "up":
        return change >= MIN_ABS_CHANGE_24H
    if MOVER_DIRECTION == "down":
        return change <= -MIN_ABS_CHANGE_24H
    return True


def get_symbols():
    global BLOCKLIST, RANK, CHANGE, MCAP, ATTENTION
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
    volumes = {t["symbol"]: float(t.get("quoteVolume") or 0) for t in tickers}
    prices = {t["symbol"]: float(t.get("lastPrice") or 0) for t in tickers}
    changes = {t["symbol"]: float(t.get("priceChangePercent") or 0) for t in tickers}
    tags = {} if MAJORS_MODE else load_tags()
    caps = load_market_caps() if USE_MCAP else {}
    if (MIN_MARKET_CAP or MAX_MARKET_CAP) and USE_MCAP and not caps:
        _warn_once("no market cap data loaded, so the market cap filter is not being applied")
    excluded, screened, eligible = {}, {}, []
    if MAJORS_MODE:
        listed = set(bases.values())
        missing = [c for c in MAJORS if c not in listed]
        if missing:
            _warn_once(f"majors not found as Binance spot {QUOTE} pairs (skipped): {', '.join(missing)}")
    for symbol, base in bases.items():
        if MAJORS_MODE and base not in MAJORS:
            continue
        if volumes.get(symbol, 0) < MIN_24H_QUOTE_VOLUME:
            continue
        price = prices.get(symbol, 0)
        if (MIN_PRICE and price < MIN_PRICE) or (MAX_PRICE and price > MAX_PRICE):
            continue
        if MAJORS_MODE:
            why = f"in {BLOCKLIST_FILE.name}" if base in BLOCKLIST else None   # you picked these; only honor the blocklist
        else:
            why = non_utility_reason(base, tags.get(base, ()))
        if why:
            excluded[symbol] = why
            continue
        why = cap_reason(caps.get(base), bool(caps))
        if why:
            screened[symbol] = why
        else:
            eligible.append(symbol)

    attention, attention_rejected = {}, 0
    if DYNAMIC_UNIVERSE:
        eligible = [s for s in eligible if _passes_move_filter(changes.get(s, 0.0))]
        # Start with the most liquid utility pairs. Price movement never changes the default ranking,
        # so a short-lived pump cannot buy its way into the watchlist.
        candidates = sorted(eligible, key=lambda s: (-volumes[s], s))[:ATTENTION_CANDIDATES]
        attention, reasons = sustained_attention(candidates) if candidates else ({}, Counter())
        attention_rejected = sum(reasons.values())
        # Recent hourly volume is the primary signal. Retention rewards a coin whose attention is holding
        # or growing rather than a one-day volume print fading out.
        ranked = sorted(attention, key=lambda s: (-attention[s]["recent_average"] * min(attention[s]["retention"], 2.0),
                                                  -volumes[s], s))
    else:
        ranked = sorted(eligible, key=lambda s: (-volumes[s], s))

    # Replace, never mutate: alert threads may still be reading the previous snapshot.
    RANK = {sym: (i + 1, volumes[sym], len(ranked)) for i, sym in enumerate(ranked)}
    CHANGE = {sym: changes.get(sym, 0.0) for sym in ranked}
    MCAP = {sym: caps[bases[sym]] for sym in ranked if bases[sym] in caps}
    ATTENTION = {sym: attention[sym] for sym in ranked} if DYNAMIC_UNIVERSE else {}

    symbols = ranked[:MOVER_COUNT] if DYNAMIC_UNIVERSE else (ranked[:TOP_COINS] if TOP_COINS > 0 else ranked)
    if VERBOSE:
        for sym, why in sorted({**excluded, **screened}.items()):
            log.debug("[excluded] %s: %s", sym, why)
    span = ""
    if symbols:
        lo, hi = min(prices[x] for x in symbols), max(prices[x] for x in symbols)
        span = f"; prices from {fmt_price(lo)} to {fmt_price(hi)}"
    if MAJORS_MODE:
        log.info("Majors only: scanning %d of %d listed coins: %s", len(symbols), len(MAJORS),
                 ", ".join(x[:-len(QUOTE)] for x in symbols))
        return symbols
    if DYNAMIC_UNIVERSE:
        direction = {"both": "up or down", "up": "up", "down": "down"}[MOVER_DIRECTION]
        log.info("Dynamic utility universe: %d Binance spot %s pairs, %d excluded as non-crypto or low quality, "
                 "%d sustained-attention utility coins%s (volume floor $%s; %d faded/spiky candidates removed); "
                 "scanning top %d by recent volume and retention%s",
                 len(bases), QUOTE, len(excluded), len(ranked),
                 f" moving at least {MIN_ABS_CHANGE_24H:g}% {direction} in 24h" if MIN_ABS_CHANGE_24H > 0 else "",
                 f"{MIN_24H_QUOTE_VOLUME:,.0f}", attention_rejected, len(symbols), span)
        return symbols
    log.info("Filter: %d Binance spot %s pairs, %d excluded as non-crypto or low quality, %d coins above the "
             "24h floor ($%s)%s; scanning %d%s",
             len(bases), QUOTE, len(excluded), len(ranked) + len(screened), f"{MIN_24H_QUOTE_VOLUME:,.0f}",
             f", {len(screened)} removed by the market cap filter" if screened else "", len(symbols), span)
    return symbols


# ───────────────────────────── alerts and scanning ─────────────────────────────

def tradingview_url(symbol):
    layout = f"{TV_LAYOUT_ID}/" if TV_LAYOUT_ID else ""
    return f"https://www.tradingview.com/chart/{layout}?symbol={TV_EXCHANGE}%3A{symbol}&interval=1"


def format_alert(symbol, candle_ms, price, stats=None):
    stats = stats or {}
    base = symbol[: -len(QUOTE)]
    t = time.strftime("%H:%M", time.localtime(candle_ms / 1000))
    lines = [f"🟠 {symbol}: RSI({RSI_PERIOD}) touched/crossed DOWN through {RSI_LEVEL:g}",
             f"1m candle {t} | close {fmt_price(price)}"]
    rsi, previous_rsi = stats.get("rsi"), stats.get("previous_rsi")
    if rsi is not None:
        lines.append(f"RSI({RSI_PERIOD}): {rsi:.2f}" +
                     (f" (previous closed candle {previous_rsi:.2f})" if previous_rsi is not None else ""))
    rank = RANK.get(symbol)
    if rank:
        lines.append(f"24h quote volume ${rank[1]:,.0f} (rank #{rank[0]} of {rank[2]})")
    change = stats.get("change_24h", CHANGE.get(symbol))
    if change is not None:
        lines.append(f"24h price change: {change:+.1f}%")
    attention = ATTENTION.get(symbol)
    if attention:
        lines.append(f"Sustained attention: ${attention['recent_average']:,.0f}/h over last "
                     f"{ATTENTION_RECENT_HOURS}h; {attention['retention']:.0%} of prior-volume pace")
    lines.append("Oversold-condition alert only; this is not a confirmed buy signal.")
    lines += [tradingview_url(symbol), f"https://www.binance.com/en/trade/{base}_{QUOTE}?type=spot"]
    return "\n".join(lines)


def send_alert(symbol, cross):
    # Runs in a thread pool whose futures nobody reads, so an exception here would vanish silently.
    try:
        notify(format_alert(symbol, cross["candle_ms"], cross["price"], cross["stats"]),
               title="1m RSI 30 touch", url=tradingview_url(symbol))
    except Exception:
        log.exception("could not deliver the alert for %s", symbol)


def scan_interval(symbols, scan_pool, alert_pool, alerted):
    """Check every watched coin's closed candles once. Returns counters for the one-line scan summary."""
    # highest 24h volume first, so the coins that matter are fetched and alerted first
    candidates = sorted(symbols, key=lambda sym: RANK.get(sym, (1 << 30, 0, 0))[0])
    seen = frozenset(alerted)
    futures = {scan_pool.submit(check_symbol, sym, seen): sym for sym in candidates}

    found = errors = 0
    skipped = Counter()
    for future in as_completed(futures):
        try:
            symbol, crosses = future.result()
        except Exception as e:
            symbol, crosses = futures[future], None
            log.warning("%s: %s", symbol, e)
        if crosses is None:
            errors += 1
            continue
        for cross in crosses:
            key = (symbol, cross["candle_ms"])
            if key in alerted:
                continue
            found += 1
            reject = cross["reject"]
            if reject and reject.startswith("unverified"):
                # Not marked as handled: the cross stays inside the LOOKBACK window, so the next scan retries it.
                skipped["unverified"] += 1
                log.debug("[skip] %s: %s (will retry next scan)", symbol, reject)
                continue
            alerted.add(key)
            if reject:
                skipped[reject.split(":")[0]] += 1
                log.debug("[skip] %s: %s", symbol, reject)
                continue
            alert_pool.submit(send_alert, symbol, cross)

    cutoff = clock_ms() - 3_600_000
    alerted.difference_update({k for k in alerted if k[1] < cutoff})
    return {"candidates": len(candidates), "found": found, "errors": errors, "skipped": skipped}


def sleep_until_next_candle():
    """Sleep until just after the next 1m candle closes, measured on Binance's clock."""
    time.sleep((INTERVAL_MS - clock_ms() % INTERVAL_MS) / 1000 + SCAN_DELAY)


# ───────────────────────────── utility commands ─────────────────────────────

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
        print(f"  {coin}: {sorted(tags.get(coin, [])) or 'no tags'} -> "
              f"{non_utility_reason(coin, tags.get(coin, ())) or 'kept as utility'}")


def show_screen():
    """Print the top SCREEN_TOP coins (by 24h volume, after all coin filters) ranked on four measures."""
    pick_base()
    sync_clock()
    top = get_symbols()[:SCREEN_TOP]
    if not top:
        print("No coins passed the filters.")
        return

    def surge_for(symbol):
        try:
            rows = closed_rows(get("/api/v3/klines", symbol=symbol, interval=INTERVAL, limit=CANDLES))
            if rows is None:
                return symbol, None
            i = len(rows) - 1
            return symbol, volume_surge(rows, i, liquidity_stats(rows, i)["traded"])
        except Exception:
            return symbol, None

    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        surge = {s: v for s, v in pool.map(surge_for, top) if v is not None}

    def passes(s):
        change = CHANGE.get(s, 0.0)
        if change_reject(change):
            return False
        return not (MIN_VOL_SURGE > 0 and surge.get(s, 0.0) < MIN_VOL_SURGE)

    checked = len(top)
    top = [s for s in top if passes(s)]
    if not top:
        print(f"None of the top {checked} coins by volume pass your change / volume surge filters.")
        return

    measures = {
        "volume": {s: RANK[s][1] for s in top},
        "change": {s: CHANGE[s] for s in top if s in CHANGE},
        "cap": {s: MCAP[s] for s in top if s in MCAP},
        "surge": surge,
    }

    def percentiles(values):
        order = sorted(values, key=values.get)
        last = max(len(order) - 1, 1)
        return {s: k / last for k, s in enumerate(order)}

    pct = {name: percentiles(v) for name, v in measures.items() if len(v) > 1}
    score = {s: sum(p.get(s, 0.5) for p in pct.values()) / max(len(pct), 1) for s in top}
    ranked = sorted(top, key=lambda s: (-score[s], s))

    print(f"\n{len(top)} of the top {checked} coins by 24h volume pass your filters, ranked by average percentile "
          f"of {', '.join(pct) or 'volume'} (higher = stronger on all of them)\n")
    print(f"{'#':>3}  {'coin':<10} {'score':>5}  {'24h volume':>10}  {'24h chg':>8}  {'mcap':>9}  {'vol surge':>9}")
    for n, s in enumerate(ranked, 1):
        cap = fmt_big(MCAP[s]) if s in MCAP else "-"
        sg = f"{surge[s]:.1f}x" if s in surge else "-"
        print(f"{n:>3}  {s[:-len(QUOTE)]:<10} {score[s] * 100:>5.0f}  {fmt_big(RANK[s][1]):>10}  "
              f"{CHANGE.get(s, 0.0):>+7.1f}%  {cap:>9}  {sg:>9}")
    print(f"\nvol surge = last {LIQUIDITY_WINDOW} min volume vs the average {LIQUIDITY_WINDOW} min window "
          f"in the previous ~{CANDLES // 60} h. This is a ranking aid, not a buy signal.")


# ───────────────────────────── main loop ─────────────────────────────

def main():
    if "--screen" in sys.argv:
        try:
            return show_screen()
        except Exception as e:
            print(f"[error] {e}")
            return
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
    sync_clock()

    alerted = set()
    symbols, last_refresh = [], 0.0
    scan_pool = ThreadPoolExecutor(max_workers=SCAN_WORKERS)
    alert_pool = ThreadPoolExecutor(max_workers=ALERT_WORKERS)
    try:
        scope = "dynamic high-volume utility coins" if DYNAMIC_UNIVERSE else "curated top coins"
        notify(f"{scope.title()} 1m RSI({RSI_PERIOD}) {RSI_LEVEL:g} touch scanner started.")
        while True:
            if not symbols or time.time() - last_refresh > REFRESH_SYMBOLS_EVERY:
                try:
                    symbols = get_symbols()
                    last_refresh = time.time()
                    sync_clock()
                except Exception as e:
                    log.warning("could not load symbols: %s", e)
                    time.sleep(5)
                    continue
                if not symbols:
                    log.warning("0 symbols after filtering - lower MIN_24H_QUOTE_VOLUME")
                    time.sleep(30)
                    continue
                log.info("Watching %d %s pairs", len(symbols), QUOTE)
            sleep_until_next_candle()
            started = time.time()
            info = scan_interval(symbols, scan_pool, alert_pool, alerted)
            total = sum(info["skipped"].values())
            detail = ", ".join(f"{n} {why}" for why, n in info["skipped"].most_common())
            log.info("Scan took %.1fs | %d coins checked | %d RSI crosses, %d skipped%s%s",
                     time.time() - started, info["candidates"], info["found"], total,
                     f" ({detail})" if total else "",
                     f" | {info['errors']} fetch errors" if info["errors"] else "")
    finally:
        scan_pool.shutdown(wait=False, cancel_futures=True)
        alert_pool.shutdown(wait=True)   # let queued alerts finish; they are the whole point of the program


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
