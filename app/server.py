"""
MOJ Discovery — server.py

Scan pipeline:

  1. DOMAIN SWEEP
     Probe fl{min_fl}–fl{max_fl}.moveonjoy.com concurrently.
     Both http AND https are tested in parallel per domain.
     Every responsive scheme is kept as an independent endpoint.

  2. DISCOVERY
     For each active domain+scheme, fetch the server root and scrape
     every */index.m3u8 URL found in the response body.
     Falls through to a small list of known playlist paths if root is empty.

  3. VERIFICATION
     Each discovered URL is verified with ffprobe before inclusion.
     Concurrency and timeout are configurable.

  4. WRITE
     All live URLs written to output.m3u8, sorted by channel name.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime as dt
from typing import TypedDict

import aiofiles
import zoneinfo
from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.environ.get("MOJ_DATA_DIR",   os.path.join(BASE_DIR, "data"))
WEB_DIR     = os.environ.get("MOJ_WEB_DIR",    os.path.join(BASE_DIR, "web"))
STATIC_DIR  = os.path.join(WEB_DIR, "static")
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "output.m3u8")
LOG_FILE    = os.path.join(DATA_DIR, "logs", "app.log")

for _d in [DATA_DIR, os.path.join(DATA_DIR, "logs"), STATIC_DIR]:
    os.makedirs(_d, exist_ok=True)

# ── Timezone ───────────────────────────────────────────────────────────────────
_TZ      = os.environ.get("TZ", "America/Chicago")
_local_tz = zoneinfo.ZoneInfo(_TZ)

def _now() -> str:
    return dt.now(_local_tz).strftime("%Y-%m-%d %H:%M:%S")

def _now_t() -> str:
    return dt.now(_local_tz).strftime("%H:%M:%S")

# ── Logging ────────────────────────────────────────────────────────────────────
class _TZFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        return (
            dt.fromtimestamp(record.created, tz=zoneinfo.ZoneInfo("UTC"))
            .astimezone(_local_tz)
            .strftime(datefmt or "%Y-%m-%d %H:%M:%S")
        )

_fmt = _TZFormatter("%(asctime)s %(levelname)-8s %(message)s")
for _h in (logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()):
    _h.setFormatter(_fmt)
    logging.root.addHandler(_h)
logging.root.setLevel(logging.INFO)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG: dict = {
    "min_fl":             2,
    "max_fl":             200,
    "auto_cycle":         False,
    "scan_interval":      14400,   # seconds between auto-scans
    "domain_timeout":     5,       # curl connect+max-time for domain probe
    "domain_concurrency": 40,      # concurrent domain probes
    "stream_timeout":     8,       # ffprobe timeout per stream
    "stream_concurrency": 20,      # concurrent ffprobe calls
    "probe_delay_ms":     0,       # optional ms delay between probes (throttle)
    "playlist_url":       "",      # optional external M3U to seed known streams
}

def load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE) as f:
                return {**DEFAULT_CONFIG, **json.load(f)}
        except Exception as e:
            log.warning(f"Config load failed, using defaults: {e}")
    return DEFAULT_CONFIG.copy()

def save_config(cfg: dict) -> None:
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)

config: dict = load_config()

# ── Types ──────────────────────────────────────────────────────────────────────
class StreamEntry(TypedDict):
    url:    str
    name:   str
    domain: str
    scheme: str

class DomainEntry(TypedDict):
    domain: str
    scheme: str

# ── App State ──────────────────────────────────────────────────────────────────
def _fresh_state() -> dict:
    return {
        "phase":         "idle",
        "running":       False,
        "progress":      0,
        "total":         0,
        "lastCycleTime": None,
        "nextCycleIn":   "-",
        "activeDomains": [],
        "results":       {},
        "stats": {
            "domains_active":   0,
            "streams_found":    0,
            "streams_verified": 0,
            "streams_failed":   0,
            "probes_fired":     0,
        },
        "scanLog": [],
    }

state: dict          = _fresh_state()
_websockets: list    = []
_cycle_lock          = asyncio.Lock()
_stop_event          = asyncio.Event()

# ── WebSocket Broadcast ────────────────────────────────────────────────────────
def _broadcast(msg: dict) -> None:
    for ws in _websockets[:]:
        asyncio.create_task(_safe_send(ws, msg))

async def _safe_send(ws: WebSocket, msg: dict) -> None:
    try:
        await ws.send_json(msg)
    except Exception:
        if ws in _websockets:
            _websockets.remove(ws)

def slog(msg: str, level: str = "info") -> None:
    """Append to scan log, write to file/console, push via WebSocket."""
    entry = {"ts": _now_t(), "msg": msg, "level": level}
    state["scanLog"].append(entry)
    if len(state["scanLog"]) > 5000:
        state["scanLog"] = state["scanLog"][-5000:]
    getattr(log, level)(f"[Cycle] {msg}")
    _broadcast({"type": "log",   "entry": entry})
    _broadcast({"type": "state", "data":  state})

# ── HTTP User-Agent ────────────────────────────────────────────────────────────
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# ── index.m3u8 regexes ────────────────────────────────────────────────────────
_ABS_M3U8 = re.compile(r'https?://[^\s"\'<>]*/index\.m3u8', re.IGNORECASE)
_REL_M3U8 = re.compile(r'(/[^\s"\'<>]*/index\.m3u8)',       re.IGNORECASE)

# Fallback paths when root body has no index.m3u8 links
_FALLBACK_PATHS = [
    "/playlist.m3u8",
    "/all.m3u8",
    "/channels.m3u8",
    "/live.m3u8",
    "/get.php",
]

# ── Subprocess Runner ──────────────────────────────────────────────────────────
async def _run(
    *args: str,
    timeout: int = 10,
    capture_stdout: bool = True,
) -> tuple[int, str]:
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE if capture_stdout else asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout)
        return proc.returncode, (out or b"").decode(errors="replace")
    except asyncio.TimeoutError:
        if proc:
            try: proc.kill()
            except Exception: pass
        return -1, ""
    except Exception as exc:
        log.debug(f"_run: {exc}")
        return -1, ""

# ── HTTP Fetch ─────────────────────────────────────────────────────────────────
async def _fetch(url: str, timeout: int = 10) -> tuple[int, str]:
    """
    Fetch a URL with curl. Returns (http_status_code, body).
    --insecure accepts both http and https without cert validation.
    """
    rc, out = await _run(
        "curl", "-sL",
        "--connect-timeout", "4",
        "--max-time",        str(timeout),
        "--insecure",
        "-A", _UA,
        "-w", "\n__STATUS__%{http_code}",
        url,
        timeout=timeout + 5,
    )
    if rc != 0:
        return 0, ""
    body, _, code = out.rpartition("\n__STATUS__")
    try:
        return int(code.strip()), body
    except ValueError:
        return 0, ""

# ── Stream Verification ────────────────────────────────────────────────────────
async def _verify(url: str, sem: asyncio.Semaphore) -> bool:
    """
    Use ffprobe to confirm the stream is live and decodable.
    Accepts http and https, HLS and MPEG-TS, no SSL restrictions.
    """
    t     = max(5, min(int(config.get("stream_timeout", 8)), 30))
    delay = max(0, int(config.get("probe_delay_ms", 0)))

    async with sem:
        if _stop_event.is_set():
            return False
        rc, _ = await _run(
            "ffprobe",
            "-v",               "quiet",
            "-timeout",         str(t * 1_000_000),
            "-probesize",       "100000",
            "-analyzeduration", "2000000",
            "-user_agent",      _UA,
            "-allowed_extensions", "ALL",
            "-i",               url,
            timeout=t + 5,
            capture_stdout=False,
        )
        state["stats"]["probes_fired"] += 1
        if delay:
            await asyncio.sleep(delay / 1000.0)

    return rc == 0

# ── M3U Parser ─────────────────────────────────────────────────────────────────
def _is_m3u(text: str) -> bool:
    return bool(re.search(r"#EXT(?:M3U|INF|-X-STREAM-INF)", text))

def _name_from_url(url: str) -> str:
    """Derive a display name from the second-to-last URL path segment."""
    parts = [p for p in url.split("?")[0].rstrip("/").split("/") if p]
    raw   = parts[-2] if len(parts) >= 2 else (parts[-1] if parts else url)
    return raw.replace("_", " ").replace("-", " ").title()

def _strip_tags(name: str) -> str:
    """Remove parenthetical tags like (MOJ), (MOJ-R), (HD)."""
    return re.sub(r"\s*\([^)]*\)\s*", " ", name).strip()

def _parse_m3u(text: str, base_url: str = "") -> list[dict]:
    """
    Parse M3U/M3U8 content. Returns [{url, name}, ...].
    Handles #EXTINF, #EXT-X-STREAM-INF, and bare http URLs.
    """
    results: list[dict] = []
    seen:    set[str]   = set()
    lines = text.splitlines()

    def _abs(u: str) -> str:
        u = u.strip()
        return u if u.startswith("http") else base_url.rstrip("/") + "/" + u.lstrip("/")

    def _add(url: str, name: str) -> None:
        url = _abs(url)
        if url and url not in seen:
            seen.add(url)
            results.append({
                "url":  url,
                "name": _strip_tags(name).strip() or _name_from_url(url),
            })

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("#EXTINF") or line.startswith("#EXT-X-STREAM-INF"):
            name = ""
            if line.startswith("#EXTINF"):
                m = re.search(r",(.+)$", line)
                name = m.group(1).strip() if m else ""
            for j in range(i + 1, min(i + 5, len(lines))):
                nxt = lines[j].strip()
                if nxt and not nxt.startswith("#"):
                    _add(nxt, name)
                    i = j
                    break
        elif line.startswith("http") and "index.m3u8" in line.lower():
            _add(line, "")
        i += 1

    return results

# ── Domain Probe ───────────────────────────────────────────────────────────────
async def _probe_domain(fl: int, sem: asyncio.Semaphore) -> list[DomainEntry]:
    """
    Probe both http and https for fl{fl}.moveonjoy.com in parallel.
    Returns one DomainEntry per responsive scheme — no preference between them.
    Any HTTP response code > 0 means the server is reachable.
    """
    domain  = f"fl{fl}.moveonjoy.com"
    timeout = int(config.get("domain_timeout", 5))
    active  = []

    async def _check(scheme: str) -> None:
        rc, code = await _run(
            "curl", "-sI",
            "--connect-timeout", "3",
            "--max-time",        str(timeout),
            "--insecure",
            "-A",  _UA,
            "-o",  "/dev/null",
            "-w",  "%{http_code}",
            f"{scheme}://{domain}/",
            timeout=timeout + 3,
        )
        if rc == 0 and code.strip().isdigit() and int(code.strip()) > 0:
            active.append({"domain": domain, "scheme": scheme})

    async with sem:
        await asyncio.gather(_check("https"), _check("http"))

    return active

# ── Stream Discovery ───────────────────────────────────────────────────────────
async def _discover(base_url: str) -> list[dict]:
    """
    Fetch base_url root, scrape every */index.m3u8 URL from the response.
    Falls through _FALLBACK_PATHS if root yields nothing.
    Returns [{url, name}, ...].
    """
    found:   list[dict] = []
    seen:    set[str]   = set()
    visited: set[str]   = set()

    def _add(url: str) -> None:
        url = url.strip().rstrip("\"',>")
        if url and url not in seen:
            seen.add(url)
            found.append({"url": url, "name": _name_from_url(url)})

    def _scrape(body: str) -> None:
        for m in _ABS_M3U8.finditer(body):
            _add(m.group(0))
        for m in _REL_M3U8.finditer(body):
            _add(base_url.rstrip("/") + m.group(1))

    for path in ["/"] + _FALLBACK_PATHS:
        if _stop_event.is_set():
            break
        url = base_url.rstrip("/") + path
        if url in visited:
            continue
        visited.add(url)

        status, body = await _fetch(url, timeout=10)
        if status not in range(200, 400) or not body:
            continue

        _scrape(body)

        # If body is itself an M3U, parse it properly too
        if _is_m3u(body):
            for e in _parse_m3u(body, base_url):
                if "index.m3u8" in e["url"] and e["url"] not in seen:
                    seen.add(e["url"])
                    found.append(e)

        if found:
            break

    return found

# ── Helpers ────────────────────────────────────────────────────────────────────
def _domain_of(url: str) -> str:
    m = re.search(r"https?://([^/:]+)", url)
    return m.group(1) if m else ""

def _scheme_of(url: str) -> str:
    return "https" if url.startswith("https") else "http"

def _store(url: str, name: str, domain: str, scheme: str) -> None:
    state["results"][url] = {
        "url": url, "name": name, "domain": domain, "scheme": scheme
    }

# ── Scan Phases ────────────────────────────────────────────────────────────────
async def _phase_domains() -> list[DomainEntry]:
    """Sweep all fl domains; collect every responsive host+scheme pair."""
    state["phase"] = "domains"
    mn = int(config["min_fl"])
    mx = int(config["max_fl"])
    slog(f"━━━ Domain sweep fl{mn}–fl{mx} ({mx - mn + 1} candidates) ━━━")

    sem     = asyncio.Semaphore(int(config.get("domain_concurrency", 40)))
    results = await asyncio.gather(*[_probe_domain(fl, sem) for fl in range(mn, mx + 1)])
    active: list[DomainEntry] = [e for sub in results for e in sub]

    if not active:
        raise RuntimeError("No active domains found.")

    state["activeDomains"]          = sorted({e["domain"] for e in active})
    state["stats"]["domains_active"] = len(state["activeDomains"])

    scheme_summary = {}
    for e in active:
        scheme_summary.setdefault(e["domain"], []).append(e["scheme"])

    slog(
        f"Found {len(state['activeDomains'])} domain(s), "
        f"{len(active)} total endpoint(s) "
        f"({sum(1 for e in active if e['scheme']=='https')} https / "
        f"{sum(1 for e in active if e['scheme']=='http')} http)"
    )
    _broadcast({"type": "state", "data": state})
    return active


async def _phase_discover(domains: list[DomainEntry]) -> None:
    """Discover and verify streams from every active domain+scheme pair."""
    state["phase"] = "discovering"
    sem       = asyncio.Semaphore(int(config.get("stream_concurrency", 20)))
    seen_urls: set[str] = set()

    # ── Optional: seed from external playlist ─────────────────────────────────
    playlist_url = config.get("playlist_url", "").strip()
    if playlist_url:
        slog("━━━ Seeding from external playlist ━━━")
        rc, body = await _run(
            "curl", "-sL", "-k", "-A", _UA,
            "--max-time", "30",
            playlist_url,
            timeout=35,
        )
        if rc == 0 and body and _is_m3u(body):
            entries = [e for e in _parse_m3u(body) if "index.m3u8" in e["url"]]
            slog(f"  {len(entries)} index.m3u8 URL(s) from external playlist")
            state["total"] += len(entries)
            _broadcast({"type": "state", "data": state})

            async def _seed(entry: dict) -> None:
                if _stop_event.is_set() or entry["url"] in seen_urls:
                    state["progress"] += 1
                    return
                if await _verify(entry["url"], sem):
                    seen_urls.add(entry["url"])
                    _store(entry["url"], entry["name"],
                           _domain_of(entry["url"]), _scheme_of(entry["url"]))
                    state["stats"]["streams_verified"] += 1
                    state["stats"]["streams_found"]    += 1
                else:
                    state["stats"]["streams_failed"] += 1
                state["progress"] += 1
                _broadcast({"type": "state", "data": state})

            await asyncio.gather(*[_seed(e) for e in entries])
            slog(f"  Seeded {state['stats']['streams_verified']} live stream(s)")
        else:
            slog("  External playlist unavailable or not M3U", "warning")

    # ── Discover per domain+scheme ─────────────────────────────────────────────
    slog("━━━ Discovering streams ━━━")

    for entry in domains:
        if _stop_event.is_set():
            slog("  Scan stopped by user.")
            break

        domain = entry["domain"]
        scheme = entry["scheme"]
        base   = f"{scheme}://{domain}"

        streams = await _discover(base)
        if not streams:
            continue

        new = [s for s in streams if s["url"] not in seen_urls]
        if not new:
            slog(f"  [{domain}/{scheme}] {len(streams)} found — all already seen")
            continue

        slog(f"  [{domain}/{scheme}] {len(new)} new stream(s) — verifying …")
        state["total"] += len(new)
        _broadcast({"type": "state", "data": state})

        async def _check(s: dict, _d=domain, _sc=scheme) -> None:
            if _stop_event.is_set() or s["url"] in seen_urls:
                state["progress"] += 1
                return
            live = await _verify(s["url"], sem)
            seen_urls.add(s["url"])
            if live:
                _store(s["url"], s["name"], _d, _sc)
                state["stats"]["streams_verified"] += 1
                state["stats"]["streams_found"]    += 1
            else:
                state["stats"]["streams_failed"] += 1
            state["progress"] += 1
            _broadcast({"type": "state", "data": state})

        await asyncio.gather(*[_check(s) for s in new])

        ok   = sum(1 for s in new if s["url"] in state["results"])
        fail = len(new) - ok
        slog(f"  [{domain}/{scheme}] ✓ {ok}  ✗ {fail}")

    _broadcast({"type": "state", "data": state})


async def _phase_write() -> None:
    """Write all verified streams to output.m3u8, sorted by name."""
    lines = ["#EXTM3U\n"]
    for url, info in sorted(state["results"].items(), key=lambda x: x[1]["name"].lower()):
        name = info["name"] or _name_from_url(url)
        lines.append(
            f'#EXTINF:-1 tvg-name="{name}" group-title="MOJ",{name}\n'
            f'{url}\n'
        )

    async with aiofiles.open(OUTPUT_FILE, "w", encoding="utf-8", newline="\n") as f:
        await f.write("".join(lines))

    count = len(state["results"])
    state["lastCycleTime"] = _now()
    slog(f"✓ Complete — {count} stream(s) written to output.m3u8")

# ── Main Cycle ─────────────────────────────────────────────────────────────────
async def run_cycle() -> None:
    if _cycle_lock.locked():
        log.warning("Cycle already running — ignoring.")
        return

    async with _cycle_lock:
        _stop_event.clear()
        saved_next = state.get("nextCycleIn", "-")
        state.clear()
        state.update(_fresh_state())
        state["running"]     = True
        state["nextCycleIn"] = saved_next
        _broadcast({"type": "state", "data": state})

        try:
            domains = await _phase_domains()
            await _phase_discover(domains)
            if not _stop_event.is_set():
                await _phase_write()
        except RuntimeError as exc:
            slog(str(exc), "error")
        except Exception as exc:
            log.exception(f"run_cycle error: {exc}")
            slog(f"Unexpected error: {exc}", "error")
        finally:
            state["running"] = False
            state["phase"]   = "idle"
            _broadcast({"type": "state", "data": state})

# ── Scheduler ──────────────────────────────────────────────────────────────────
async def _scheduler() -> None:
    while True:
        if not config.get("auto_cycle", False):
            state["nextCycleIn"] = "disabled"
            _broadcast({"type": "state", "data": state})
            await asyncio.sleep(15)
            continue

        interval = int(config.get("scan_interval", 14400))
        for remaining in range(interval, 0, -1):
            if not config.get("auto_cycle", False):
                break
            h, r = divmod(remaining, 3600)
            m, s = divmod(r, 60)
            state["nextCycleIn"] = f"{h:02d}:{m:02d}:{s:02d}"
            _broadcast({"type": "state", "data": state})
            await asyncio.sleep(1)
        else:
            log.info("Auto-cycle triggered by scheduler.")
            asyncio.create_task(run_cycle())

# ── FastAPI App ────────────────────────────────────────────────────────────────
@asynccontextmanager
async def _lifespan(app: FastAPI):
    asyncio.create_task(_scheduler())
    yield

app = FastAPI(title="MOJ Discovery", version="2.0.0", lifespan=_lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"])
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", response_class=HTMLResponse)
async def root():
    async with aiofiles.open(os.path.join(WEB_DIR, "index.html")) as f:
        return HTMLResponse(await f.read())


@app.get("/playlist", summary="Download the generated M3U8 playlist")
async def get_playlist():
    if not os.path.exists(OUTPUT_FILE):
        raise HTTPException(404, "Playlist not ready — run a scan first.")
    return FileResponse(OUTPUT_FILE, media_type="application/x-mpegurl",
                        filename="moj.m3u8")


@app.get("/api/state", summary="Full application state")
async def api_state():
    return JSONResponse(state)


@app.get("/api/config", summary="Current configuration")
async def api_get_config():
    return JSONResponse(config)


@app.post("/api/config", summary="Update configuration")
async def api_post_config(request: Request):
    global config
    config.update(await request.json())
    save_config(config)
    return {"status": "ok"}


@app.post("/api/scan/start", summary="Start a scan cycle")
@app.post("/api/run")
async def api_scan_start():
    if state["running"]:
        raise HTTPException(409, "Scan already running.")
    asyncio.create_task(run_cycle())
    return {"status": "started"}


@app.post("/api/scan/stop", summary="Stop a running scan")
async def api_scan_stop():
    if not state["running"]:
        raise HTTPException(409, "No scan is running.")
    _stop_event.set()
    state["running"] = False
    state["phase"]   = "idle"
    _broadcast({"type": "state", "data": state})
    slog("Scan stopped by user.", "warning")
    return {"status": "stopped"}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    _websockets.append(ws)
    try:
        await ws.send_json({"type": "state", "data": state})
        while True:
            await ws.receive_text()
    except Exception:
        pass
    finally:
        if ws in _websockets:
            _websockets.remove(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8080, reload=False, log_config=None)
