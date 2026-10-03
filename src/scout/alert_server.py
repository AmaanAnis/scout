"""
Local alert server: receives alerts from the scanner and shows them live in your browser.
No third-party service involved.

    pip install fastapi uvicorn
    python alert_server.py                 # then open http://127.0.0.1:8000

Scanner side (in another terminal):
    export LOCAL_SERVER_URL="http://127.0.0.1:8000/alert"
    python mexc_ema_cross_alert.py         # or mexc_agent_scanner.py

Environment variables:
    ALERT_HOST     default 127.0.0.1 (this computer only). Use 0.0.0.0 to open it to your Wi-Fi/LAN.
    ALERT_PORT     default 8000
    ALERT_TOKEN    if set, POST /alert requires header  X-Token: <value>  (set the same on the scanner)
    ALERT_HISTORY  default alerts.jsonl (history file, reloaded on restart)
"""
import asyncio
import hmac
import json
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

TOKEN = os.getenv("ALERT_TOKEN", "")
HISTORY_FILE = Path(os.getenv("ALERT_HISTORY", "alerts.jsonl"))
MAX_HISTORY = 200

history = deque(maxlen=MAX_HISTORY)      # newest alerts, oldest first
subscribers = set()                      # one asyncio.Queue per open browser tab


@asynccontextmanager
async def lifespan(app):
    # on startup: reload the last alerts from disk
    if HISTORY_FILE.exists():
        for line in HISTORY_FILE.read_text(encoding="utf-8").splitlines()[-MAX_HISTORY:]:
            try:
                history.append(json.loads(line))
            except ValueError:
                pass
    yield


app = FastAPI(title="Local alert server", lifespan=lifespan)


class Alert(BaseModel):
    title: str = Field(default="Alert", max_length=200)
    text: str = Field(max_length=2000)


@app.post("/alert", status_code=201)
async def post_alert(alert: Alert, x_token: str = Header(default="")):
    """The scanner calls this. We store the alert and push it to every open browser."""
    if TOKEN and not hmac.compare_digest(x_token.encode(), TOKEN.encode()):
        raise HTTPException(status_code=401, detail="bad token")

    item = {"title": alert.title, "text": alert.text, "ts": time.time()}
    history.append(item)
    with HISTORY_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(item) + "\n")

    for queue in list(subscribers):
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:        # a stuck browser must never block the others
            pass
    return {"delivered_to": len(subscribers)}


@app.get("/alerts")
async def get_alerts():
    return list(history)


@app.get("/events")
async def events():
    """Server-Sent Events: a one-way stream that the browser keeps open."""
    queue = asyncio.Queue(maxsize=100)
    subscribers.add(queue)

    async def stream():
        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"data: {json.dumps(item)}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"           # keeps the connection alive
        finally:
            subscribers.discard(queue)           # tab closed -> forget it

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MEXC alerts</title>
<style>
  body{font-family:system-ui,sans-serif;background:#111;color:#eee;margin:0 auto;padding:16px;max-width:720px}
  header{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px}
  h1{font-size:18px;margin:0;flex:1}
  #dot{width:10px;height:10px;border-radius:50%;background:#888}
  #dot.on{background:#3ddc84} #dot.off{background:#ff5252}
  button{background:#2a2a2a;color:#eee;border:1px solid #444;border-radius:6px;padding:8px 12px;cursor:pointer}
  .alert{border-left:4px solid #888;background:#1b1b1b;padding:10px 12px;margin:8px 0;border-radius:6px;overflow-wrap:anywhere}
  .alert.up{border-color:#3ddc84} .alert.down{border-color:#ff5252}
  .time{color:#888;font-size:12px;margin-bottom:4px}
  a{color:#7ab7ff}
  #empty{color:#888}
</style>
</head>
<body>
<header>
  <span id="dot" title="connection"></span>
  <h1>MEXC EMA cross alerts</h1>
  <button id="enable">Enable sound &amp; popups</button>
</header>
<div id="list"></div>
<div id="empty">Waiting for alerts...</div>

<script>
const list = document.getElementById("list");
const empty = document.getElementById("empty");
const dot = document.getElementById("dot");
let audio = null;

// Browsers only allow sound after a click, so the button creates the audio context.
document.getElementById("enable").onclick = async () => {
  audio = new (window.AudioContext || window.webkitAudioContext)();
  await audio.resume();
  if (window.isSecureContext && "Notification" in window) Notification.requestPermission();
  beep(660);
  document.getElementById("enable").textContent = "Sound on";
};

function beep(freq) {
  if (!audio) return;
  const osc = audio.createOscillator(), gain = audio.createGain();
  osc.frequency.value = freq;
  osc.connect(gain); gain.connect(audio.destination);
  gain.gain.setValueAtTime(0.25, audio.currentTime);
  gain.gain.exponentialRampToValueAtTime(0.001, audio.currentTime + 0.5);
  osc.start(); osc.stop(audio.currentTime + 0.5);
}

function render(a, live) {
  empty.style.display = "none";
  const up = a.text.includes("ABOVE"), down = a.text.includes("BELOW");
  const box = document.createElement("div");
  box.className = "alert" + (up ? " up" : down ? " down" : "");

  const time = document.createElement("div");
  time.className = "time";
  time.textContent = new Date(a.ts * 1000).toLocaleTimeString();
  box.appendChild(time);

  // textContent (never innerHTML) so alert text can't inject HTML/JS into the page
  a.text.split("\\n").forEach(line => {
    const row = document.createElement("div");
    if (line.startsWith("https://www.mexc.com/")) {
      const link = document.createElement("a");
      link.href = line; link.textContent = line; link.target = "_blank"; link.rel = "noopener";
      row.appendChild(link);
    } else {
      row.textContent = line;
    }
    box.appendChild(row);
  });

  list.prepend(box);
  while (list.children.length > 200) list.lastChild.remove();

  if (live) {
    beep(up ? 880 : 440);                       // high beep = up cross, low beep = down cross
    if (window.isSecureContext && "Notification" in window && Notification.permission === "granted") {
      new Notification(a.title, { body: a.text.split("\\n")[0] });
    }
  }
}

fetch("/alerts").then(r => r.json()).then(items => {
  items.forEach(a => render(a, false));        // history: no sound
  const es = new EventSource("/events");       // then listen for new ones
  es.onopen = () => dot.className = "on";
  es.onerror = () => dot.className = "off";    // EventSource reconnects by itself
  es.onmessage = e => render(JSON.parse(e.data), true);
});
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return PAGE


if __name__ == "__main__":
    uvicorn.run(app, host=os.getenv("ALERT_HOST", "127.0.0.1"), port=int(os.getenv("ALERT_PORT", "8000")))
