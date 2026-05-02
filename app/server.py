import re
import asyncio
import aiohttp
import aiofiles
import json
from datetime import datetime as dt
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent          # /opt/moj
WEB_DIR     = BASE_DIR / "web"
DATA_DIR    = Path("/app")                   # volume-mounted persistent data
CONFIG_FILE = DATA_DIR / "config.json"
OUTPUT_FILE = DATA_DIR / "output.m3u"

app = FastAPI()
app.mount("/static", StaticFiles(directory=str(WEB_DIR / "static")), name="static")

# ── STATE ─────────────────────────────────────────────────────────────────────
state = {
    "running":       False,
    "phase":         "idle",
    "progress":      0,
    "total":         0,
    "results":       {},          # channel_name -> {url, domain, status, method}
    "activeDomains": [],
    "scanLog":       [],
    "stats": {
        "streams_found":    0,
        "streams_verified": 0,
        "streams_failed":   0,
        "streams_healed":   0,    # dead streams that were replaced with a working URL
    },
    "moj": {
        "input":     0,
        "processed": 0,
        "working":   0,
        "failed":    0,
    },
}

config = {}
_ws_clients: set = set()

# ── WebSocket broadcast ────────────────────────────────────────────────────────
async def broadcast(payload: dict):
    dead = set()
    msg  = json.dumps({"type": "state", "data": payload})
    for ws in _ws_clients:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    _ws_clients.difference_update(dead)

async def push_state():
    await broadcast(state)

# ── LOG ───────────────────────────────────────────────────────────────────────
MAX_LOG = 500

def slog(msg: str):
    ts   = dt.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    state["scanLog"].append(line)
    if len(state["scanLog"]) > MAX_LOG:
        state["scanLog"] = state["scanLog"][-MAX_LOG:]
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(push_state())
    except Exception:
        pass

# ── CONFIG ────────────────────────────────────────────────────────────────────
def load_config():
    global config
    defaults = {
        "min_fl": 2, "max_fl": 200,
        "healthcheck_enabled": True, "healthcheck_interval": 86400,
        "healthcheck_batch": 5, "stream_timeout": 10, "domain_timeout": 5,
        "stream_concurrency": 15, "domain_concurrency": 50, "probe_delay_ms": 200,
        "slug_source": "both", "playlist_url": "",
        "path_patterns": [
            "/{slug}/index.m3u8", "/{slug}/index.ts", "/{slug}.m3u8", "/{slug}.ts",
            "/live/{slug}/index.m3u8", "/live/{slug}/index.ts",
            "/stream/{slug}/index.m3u8", "/stream/{slug}.m3u8", "/{slug}/index.m3u",
        ],
        "scan_interval": 14400, "auto_cycle": True,
    }
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE) as f:
                defaults.update(json.load(f))
        except Exception as e:
            print(f"warning: could not load config: {e}")
    config = defaults

def save_config():
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)

# ── NORMALIZATION ─────────────────────────────────────────────────────────────
def normalize(s: str) -> str:
    s = re.sub(r"\(.*?\)", "", s.lower())
    return re.sub(r"[^a-z0-9]", "", s)

def tokenize(s: str):
    s = re.sub(r"\(.*?\)", "", s.lower())
    return set(re.sub(r"[^a-z0-9]+", " ", s).split())

ALIASES = {
    "espn2": ["espn 2", "espn2hd"],
    "foxsports1": ["fs1"],
    "nbcsn": ["nbc sports"],
}

def expand_alias(tokens: set):
    expanded = set(tokens)
    joined   = "".join(tokens)
    for k, vals in ALIASES.items():
        if k in joined:
            for v in vals:
                expanded.update(tokenize(v))
    return expanded

def score_match(channel_name: str, slug: str) -> float:
    n1, n2 = normalize(channel_name), normalize(slug)
    if n1 == n2:
        return 100.0
    t1 = expand_alias(tokenize(channel_name))
    t2 = expand_alias(tokenize(slug))
    if not t1 or not t2:
        return 0.0
    score = len(t1 & t2) / len(t1 | t2)
    if n1 in n2 or n2 in n1:
        score += 0.5
    return score

def find_best_match(channel_name: str, pool: dict, exclude_status: list = None) -> str | None:
    """
    Find the best-matching key in pool for channel_name.
    exclude_status: skip entries whose status is in this list (e.g. ["dead"]).
    """
    best_slug, best_score = None, 0.0
    for slug, info in pool.items():
        if exclude_status and info.get("status") in exclude_status:
            continue
        s = score_match(channel_name, slug)
        if s > best_score:
            best_score, best_slug = s, slug
    return best_slug if best_score >= 0.3 else None

# ── MOJ PLAYLIST LOADER ───────────────────────────────────────────────────────
async def load_moj_channels() -> dict:
    """
    Load all (MOJ) entries from the configured playlist URL.

    Returns:
        dict keyed by normalized name:
            {
              "name": "ESPN (MOJ)",
              "original_url": "http://...existing.m3u8",   # URL from the source playlist
            }

    The original_url is the CURRENT url from the user's playlist. We keep it
    so we can health-check it first and only re-scan if it's dead.
    """
    url = config.get("playlist_url", "")
    if not url:
        slog("warning: no playlist_url configured — set it in Settings")
        return {}
    moj     = {}
    current = None
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as r:
                text = await r.text()
    except Exception as e:
        slog(f"failed to fetch playlist from {url}: {e}")
        return {}
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            name = line.split(",", 1)[-1].strip()
            if "(MOJ)" in name:
                slug    = normalize(name)
                current = slug
                moj[slug] = {"name": name, "original_url": None}
            else:
                current = None
        elif line.startswith("http") and current:
            moj[current]["original_url"] = line
            current = None
    slog(f"loaded {len(moj)} MOJ channels from playlist")
    return moj

# ── DOMAIN DISCOVERY ──────────────────────────────────────────────────────────
async def discover_active_domains(session) -> list:
    min_fl = config.get("min_fl", 2)
    max_fl = config.get("max_fl", 200)
    sem    = asyncio.Semaphore(config.get("domain_concurrency", 50))
    active = []

    async def check(fl: int):
        domain  = f"fl{fl}.moveonjoy.com"
        timeout = aiohttp.ClientTimeout(total=config.get("domain_timeout", 5))
        async with sem:
            try:
                async with session.get(f"https://{domain}/", timeout=timeout, allow_redirects=True) as r:
                    if r.status < 500:
                        active.append(domain)
            except Exception:
                pass

    slog(f"probing fl{min_fl} to fl{max_fl}.moveonjoy.com …")
    tasks = [asyncio.create_task(check(fl)) for fl in range(min_fl, max_fl + 1)]
    chunk = 30
    for i in range(0, len(tasks), chunk):
        await asyncio.gather(*tasks[i:i + chunk])
        state["activeDomains"] = sorted(active)
        state["progress"]      = i + chunk
        await push_state()

    slog(f"found {len(active)} active domains")
    return sorted(active)

# ── SLUG PROBE ON A SINGLE DOMAIN ─────────────────────────────────────────────
async def probe_slugs_on_domain(session, domain: str, channels: list) -> dict:
    """Try path patterns for each channel name on one domain. Returns {name: url}."""
    patterns = config.get("path_patterns", ["/{slug}/index.m3u8"])
    timeout  = aiohttp.ClientTimeout(total=config.get("stream_timeout", 10))
    sem      = asyncio.Semaphore(config.get("stream_concurrency", 15))
    found    = {}

    async def probe(channel: str, pattern: str):
        slug = normalize(channel)
        url  = f"https://{domain}{pattern.replace('{slug}', slug)}"
        async with sem:
            try:
                async with session.get(url, timeout=timeout) as r:
                    if r.status == 200:
                        ct = r.headers.get("Content-Type", "")
                        if "mpegurl" in ct or "octet" in ct or url.endswith((".m3u8", ".m3u", ".ts")):
                            return channel, url
            except Exception:
                pass
        return None

    results = await asyncio.gather(*[probe(ch, pat) for ch in channels for pat in patterns])
    for res in results:
        if res:
            ch, url = res
            if ch not in found:
                found[ch] = url
    return found

# ── HTTP STREAM HEALTH CHECK ──────────────────────────────────────────────────
async def probe_url(url: str) -> bool:
    """Check if a stream URL is alive. Tries ffprobe first, falls back to HTTP HEAD."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "quiet", "-timeout", "5000000", "-i", url,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            proc.kill()
            return False
        return proc.returncode == 0
    except FileNotFoundError:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.head(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    return r.status < 400
        except Exception:
            return False

# ── MAIN SCAN ─────────────────────────────────────────────────────────────────
scan_lock = asyncio.Lock()

async def run_scan():
    if state["running"]:
        slog("scan already in progress")
        return

    async with scan_lock:
        state.update({
            "running": True, "phase": "loading",
            "progress": 0, "total": 0,
            "scanLog": [], "results": {},
            "stats": {"streams_found": 0, "streams_verified": 0,
                      "streams_failed": 0, "streams_healed": 0},
        })
        await push_state()

        try:
            connector = aiohttp.TCPConnector(limit=100, ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:

                # ── Phase 1: Load MOJ channels from the imported playlist ──
                slog("phase 1: loading MOJ channels from playlist")
                state["phase"] = "loading"
                await push_state()

                moj_map = await load_moj_channels()
                # moj_map: {slug -> {name, original_url}}

                if not moj_map:
                    slog("no MOJ channels loaded — check playlist_url in Settings")
                    return

                # Build the working results dict seeded with current URLs
                # status = "imported" means we haven't checked it yet
                results = {}
                for slug, info in moj_map.items():
                    results[info["name"]] = {
                        "url":    info["original_url"],
                        "domain": _domain_from_url(info["original_url"]),
                        "status": "imported",
                        "method": "playlist",
                    }

                state["results"] = results
                state["total"]   = len(results)
                await push_state()

                # ── Phase 2: Health-check all imported URLs ────────────────
                slog(f"phase 2: health-checking {len(results)} imported MOJ streams")
                state["phase"]    = "healthcheck"
                state["progress"] = 0
                await push_state()

                batch = config.get("healthcheck_batch", 5)
                items = list(results.items())
                dead_channels = []   # channel names whose stream is broken

                for i in range(0, len(items), batch):
                    chunk = items[i:i + batch]
                    oks   = await asyncio.gather(*[probe_url(info["url"]) for _, info in chunk if info["url"]])

                    for j, (ch_name, info) in enumerate(chunk):
                        if not info["url"]:
                            results[ch_name]["status"] = "no_url"
                            dead_channels.append(ch_name)
                            state["stats"]["streams_failed"] += 1
                        elif oks[j]:
                            results[ch_name]["status"] = "verified"
                            state["stats"]["streams_verified"] += 1
                        else:
                            results[ch_name]["status"] = "dead"
                            dead_channels.append(ch_name)
                            state["stats"]["streams_failed"] += 1

                    state["progress"] = i + len(chunk)
                    state["results"]  = results
                    await push_state()
                    await asyncio.sleep(config.get("probe_delay_ms", 200) / 1000)

                slog(f"health check done: {state['stats']['streams_verified']} live, "
                     f"{len(dead_channels)} dead/missing")

                # ── Phase 3: Domain discovery (only if there are dead streams) ──
                if dead_channels:
                    slog(f"phase 3: {len(dead_channels)} streams need new URLs — scanning domains")
                    state["phase"]    = "discovering"
                    state["progress"] = 0
                    state["total"]    = 200   # approximate
                    await push_state()

                    domains = await discover_active_domains(session)
                    state["activeDomains"] = domains
                    state["total"]         = len(domains)

                    if not domains:
                        slog("no active moveonjoy.com domains found — cannot heal dead streams")
                    else:
                        # ── Phase 4: Re-scan dead channels across all domains ──
                        slog(f"phase 4: re-scanning {len(dead_channels)} dead channels across "
                             f"{len(domains)} domains")
                        state["phase"]    = "scanning"
                        state["progress"] = 0
                        await push_state()

                        for i, domain in enumerate(domains):
                            # Only probe channels that are still dead/missing
                            still_dead = [ch for ch in dead_channels
                                          if results[ch]["status"] in ("dead", "no_url")]
                            if not still_dead:
                                slog("all dead streams healed — stopping domain sweep early")
                                break

                            found = await probe_slugs_on_domain(session, domain, still_dead)
                            for ch_name, url in found.items():
                                slog(f"  healed: {ch_name}  →  {url}")
                                results[ch_name] = {
                                    "url":    url,
                                    "domain": domain,
                                    "status": "healed",
                                    "method": "rescan",
                                }
                                state["stats"]["streams_healed"]   += 1
                                state["stats"]["streams_verified"] += 1
                                state["stats"]["streams_failed"]   -= 1

                            state["progress"] = i + 1
                            state["results"]  = results
                            if found:
                                slog(f"  {domain}: healed {len(found)}")
                            await push_state()

                        healed_total = state["stats"]["streams_healed"]
                        still_broken = len([c for c in dead_channels
                                            if results[c]["status"] in ("dead", "no_url")])
                        slog(f"rescan done: {healed_total} healed, {still_broken} still broken")
                else:
                    slog("all imported streams are live — no domain scan needed")

                # ── Phase 5: Write output playlist ────────────────────────
                slog("phase 5: writing output playlist")
                state["phase"] = "writing"
                await push_state()

                await _write_playlist(results)
                state["results"] = results

                v = state["stats"]["streams_verified"]
                f = state["stats"]["streams_failed"]
                h = state["stats"]["streams_healed"]
                slog(f"done — {v} working ({h} healed), {f} still broken")

        except Exception as e:
            slog(f"scan error: {e}")
        finally:
            state["running"]  = False
            state["phase"]    = "idle"
            state["progress"] = state["total"]
            await push_state()


def _domain_from_url(url: str | None) -> str:
    if not url:
        return ""
    try:
        from urllib.parse import urlparse
        return urlparse(url).hostname or ""
    except Exception:
        return ""


# ── PLAYLIST WRITE ────────────────────────────────────────────────────────────
async def _write_playlist(results: dict):
    """
    Write output.m3u containing every MOJ channel that has a working URL.
    Channels whose status is 'dead' or 'no_url' are excluded.
    """
    lines    = ["#EXTM3U\n"]
    working  = 0
    excluded = 0

    for ch_name, info in results.items():
        if not info.get("url") or info.get("status") in ("dead", "no_url"):
            excluded += 1
            continue
        lines.append(
            f'#EXTINF:-1 tvg-id="{normalize(ch_name)}" '
            f'tvg-name="{ch_name}" group-title="MOJ",{ch_name}\n'
        )
        lines.append(info["url"] + "\n")
        working += 1

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        await f.write("".join(lines))

    state["moj"] = {
        "input":     len(results),
        "processed": len(results),
        "working":   working,
        "failed":    excluded,
    }
    slog(f"playlist written: {working} channels, {excluded} excluded (dead/no_url)")


# ── API ROUTES ────────────────────────────────────────────────────────────────
@app.get("/")
def index():
    return FileResponse(str(WEB_DIR / "index.html"))

@app.get("/api/state")
def api_state():
    return state

@app.get("/api/config")
def api_get_config():
    return config

@app.post("/api/config")
async def api_save_config(body: dict):
    config.update(body)
    save_config()
    return {"ok": True}

@app.post("/api/scan/start")
async def api_scan_start():
    if state["running"]:
        return {"ok": False, "reason": "already running"}
    asyncio.ensure_future(run_scan())
    return {"ok": True}

@app.post("/api/scan/stop")
async def api_scan_stop():
    state["running"] = False
    state["phase"]   = "idle"
    await push_state()
    return {"ok": True}

# Legacy alias used by the HTML's triggerRun()
@app.post("/api/run")
async def api_run():
    return await api_scan_start()

@app.get("/playlist")
async def playlist():
    if OUTPUT_FILE.exists():
        return FileResponse(str(OUTPUT_FILE), media_type="application/x-mpegurl")
    return PlainTextResponse("#EXTM3U\n# No playlist generated yet — run a scan\n",
                              media_type="application/x-mpegurl")

@app.get("/api/channels")
async def api_get_channels():
    channels_file = DATA_DIR / "channels.txt"
    if channels_file.exists():
        lines = [l.strip() for l in channels_file.read_text().splitlines()
                 if l.strip() and not l.startswith("#")]
    else:
        lines = []
    return {"channels": lines}

@app.post("/api/channels")
async def api_save_channels(body: dict):
    channels = body.get("channels", [])
    channels_file = DATA_DIR / "channels.txt"
    channels_file.parent.mkdir(parents=True, exist_ok=True)
    channels_file.write_text("\n".join(channels) + "\n")
    return {"ok": True, "count": len(channels)}

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        await websocket.send_text(json.dumps({"type": "state", "data": state}))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(websocket)

# ── STARTUP ───────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    load_config()
    slog("MOJFixxer ready")
    if config.get("auto_cycle", True):
        interval = config.get("scan_interval", 14400)
        async def loop():
            await asyncio.sleep(5)
            while True:
                await run_scan()
                slog(f"next auto-scan in {interval}s")
                await asyncio.sleep(interval)
        asyncio.ensure_future(loop())
