"""
MOJ Discovery — server.py

Stream discovery pipeline (per active domain):

  1 — WILDCARD M3U SCAN  (https then http)
    Try a broad list of well-known M3U endpoints (all.m3u8, playlist.m3u8,
    get.php, player_api.php, etc.).  Every #EXTINF + URL pair is collected
    regardless of channel name — no slug list required.  One hit captures
    every variant automatically ("Starz", "Starz Movies", "Starz East", …).

  2 — SLUG BRUTE-FORCE  (fallback only)
    Used only when the wildcard scan yields nothing.
    Pulls slugs from channels.txt and/or remote playlist URL,
    tries each path pattern per slug until first hit wins.

All discovered URLs are verified with ffprobe before inclusion.
"""

import os
import re
import json
import asyncio
import logging
from datetime import datetime as dt
from contextlib import asynccontextmanager
import zoneinfo

from fastapi import FastAPI, WebSocket, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import aiofiles

# ── Paths ──────────────────────────────────────────────────────────────────────
SRC_DIR       = "/opt/moj"
DATA_DIR      = "/app"
WEB_DIR       = os.path.join(SRC_DIR,  "web")
STATIC_DIR    = os.path.join(WEB_DIR,  "static")
CONFIG_FILE   = os.path.join(DATA_DIR, "config.json")
OUTPUT_FILE   = os.path.join(DATA_DIR, "output.m3u8")
LOG_FILE      = os.path.join(DATA_DIR, "logs", "app.log")
CHANNELS_FILE = os.path.join(DATA_DIR, "channels.txt")

for _d in [DATA_DIR, os.path.join(DATA_DIR, "logs"), STATIC_DIR]:
    os.makedirs(_d, exist_ok=True)

# ── Timezone / Logging ─────────────────────────────────────────────────────────
TZ_NAME  = os.getenv("TZ", "America/Chicago")
local_tz = zoneinfo.ZoneInfo(TZ_NAME) if TZ_NAME else zoneinfo.ZoneInfo("UTC")

class _LocalFmt(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        return (
            dt.fromtimestamp(record.created, tz=zoneinfo.ZoneInfo("UTC"))
            .astimezone(local_tz)
            .strftime(datefmt or "%Y-%m-%d %H:%M:%S")
        )

_fmt = _LocalFmt("%(asctime)s %(levelname)-8s %(message)s")
_fh  = logging.FileHandler(LOG_FILE)
_sh  = logging.StreamHandler()
for _h in (_fh, _sh):
    _h.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_fh, _sh])
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG: dict = {
    "min_fl":             2,
    "max_fl":             200,
    "auto_cycle":         False,
    "scan_interval":      14400,
    "stream_timeout":     8,
    "domain_timeout":     5,
    "stream_concurrency": 10,
    "domain_concurrency": 40,
    "probe_delay_ms":     200,
    "slug_source":        "both",   # "channels" | "playlist" | "both"
    "playlist_url":       "",
    "path_patterns": [
        "/{slug}/index.m3u8",
        "/{slug}/index.m3u",
        "/{slug}.m3u8",
    ],
}

# Wildcard M3U scan paths — tried in order, https then http, per domain.
# The first path that returns valid M3U/JSON wins; all channels inside
# are collected automatically with no slug list needed.
_PLAYLIST_PATHS = [
    # Broad "give me everything" endpoints
    "/get.php",                                   # Xtream Codes no-auth
    "/all.m3u8",
    "/all.m3u",
    "/playlist.m3u8",
    "/playlist.m3u",
    "/channels.m3u8",
    "/channels.m3u",
    "/live.m3u8",
    "/live.m3u",
    "/index.m3u8",
    "/index.m3u",
    # Sub-path variants
    "/live/index.m3u8",
    "/live/playlist.m3u8",
    "/live/all.m3u8",
    "/stream/index.m3u8",
    "/stream/playlist.m3u8",
    "/streams.m3u8",
    "/streams.m3u",
    "/feed.m3u8",
    "/output.m3u8",
    "/list.m3u8",
    "/list.m3u",
    # Xtream / panel API variants
    "/player_api.php?action=get_live_streams",
    "/get.php?type=m3u_plus",
    "/get.php?output=m3u8",
    "/get.php?output=ts",
]

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

# ── App State ──────────────────────────────────────────────────────────────────
_INIT_STATS: dict = {
    "domains_crawled":  0,
    "streams_found":    0,
    "streams_verified": 0,
    "streams_failed":   0,
    "probes_fired":     0,
    "discovery_method": {},   # domain → method used ("directory"|"playlist"|"brute")
}

state: dict = {
    "phase":         "idle",
    "running":       False,
    "progress":      0,
    "total":         0,
    "probes_fired":  0,
    "lastCycleTime": None,
    "nextCycleIn":   "-",
    "activeDomains": [],
    "domainSchemes": {},
    "results":       {},      # slug → {url, name, domain, method, verified}
    "stats":         _INIT_STATS.copy(),
    "scanLog":       [],
}

_websockets: list[WebSocket] = []
_cycle_lock = asyncio.Lock()

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
    entry = {"ts": dt.now(local_tz).strftime("%H:%M:%S"), "msg": msg}
    state["scanLog"].append(entry)
    if len(state["scanLog"]) > 2000:
        state["scanLog"] = state["scanLog"][-2000:]
    getattr(log, level)(f"[Cycle] {msg}")
    _broadcast({"type": "log",   "entry": entry})
    _broadcast({"type": "state", "data": state})

# ── Constants ──────────────────────────────────────────────────────────────────
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# ── Generic subprocess runner ──────────────────────────────────────────────────
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
        log.debug(f"subprocess error: {exc}")
        return -1, ""

# ── HTTP Fetch Helper ──────────────────────────────────────────────────────────
async def _fetch(url: str, timeout: int = 10) -> tuple[int, str]:
    """Fetch a URL with curl, return (http_status_code, body_text)."""
    rc, out = await _run(
        "curl", "-s", "-L",
        "--connect-timeout", "4",
        "--max-time", str(timeout),
        "--insecure",
        "-A", _UA,
        "-w", "\n__STATUS__%{http_code}",
        url,
        timeout=timeout + 4,
    )
    if rc != 0:
        return 0, ""
    # Split body from status sentinel
    body, _, status_str = out.rpartition("\n__STATUS__")
    try:
        status = int(status_str.strip())
    except ValueError:
        status = 0
    return status, body

# ── Stream Verification (ffprobe) ─────────────────────────────────────────────
async def _verify_stream(url: str, sem: asyncio.Semaphore) -> bool:
    """
    Verify any stream URL is live and decodable using ffprobe.
    Handles HLS (.m3u8), MPEG-TS (.ts), RTMP, plain HTTP video streams.
    Works over HTTPS without any SSL workaround flags.
    """
    t     = max(5, min(int(config.get("stream_timeout", 8)), 30))
    delay = max(0, int(config.get("probe_delay_ms", 200)))

    async with sem:
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
        state["probes_fired"]           += 1
        state["stats"]["probes_fired"]  += 1
        if delay > 0:
            await asyncio.sleep(delay / 1000.0)

    return rc == 0

# ── Slug Normalization ─────────────────────────────────────────────────────────
def normalize_slug(raw: str) -> str:
    s = raw.strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")

def _expand_slugs(slugs: list[str]) -> list[str]:
    """
    Expand a base slug list into variants that cover numbered/suffixed channels.

    "ESPN"  →  ESPN, ESPN2, ESPN_2, ESPN3, ESPN_3, … ESPN9, ESPN_9,
               ESPNU, ESPN_U, ESPNNEWS, ESPN_NEWS, ESPNCLASSIC, ESPN_CLASSIC
    "STARZ" →  STARZ, STARZ2 … STARZ9, STARZEAST, STARZ_EAST,
               STARZMOVIES, STARZ_MOVIES, STARZCOMEDY, STARZ_COMEDY,
               STARZEDGE, STARZ_EDGE, STARZKIDS, STARZ_KIDS
    "HBO"   →  HBO, HBO2 … HBO9, HBOEAST, HBO_EAST, HBOWEST, HBO_WEST,
               HBOFAMILY, HBO_FAMILY, HBOSIGNATURE, HBO_SIGNATURE,
               HBOZONE, HBO_ZONE, HBOLATINO, HBO_LATINO

    Numbers 2-9 and a curated set of common suffixes are tried for every slug.
    Existing slugs that already have a numeric/suffix tail are kept as-is and
    are NOT expanded further (avoids STARZ2 → STARZ22 etc.).
    """
    COMMON_SUFFIXES = [
        # Geographic / schedule
        "EAST", "WEST", "HD",
        # Genre / brand
        "MOVIES", "COMEDY", "DRAMA", "ACTION", "KIDS", "FAMILY",
        "CLASSIC", "SIGNATURE", "EDGE", "ZONE", "LATINO", "NEWS",
        # Letter suffixes (ESPN U, etc.)
        "U",
    ]

    # Patterns that indicate a slug is already a variant — don't expand these
    import re as _re
    _already_variant = _re.compile(
        r'(?:' +
        '|'.join(_re.escape(s) for s in COMMON_SUFFIXES) +
        r'|\d)$'
    )

    seen:    set[str]  = set()
    result:  list[str] = []

    def _add(s: str) -> None:
        if s and s not in seen:
            seen.add(s)
            result.append(s)

    for slug in slugs:
        _add(slug)
        # Don't fan out from slugs that are already variants
        if _already_variant.search(slug):
            continue
        # Numeric variants 2-9
        for n in range(2, 10):
            _add(f"{slug}{n}")
            _add(f"{slug}_{n}")
        # Word suffix variants
        for suffix in COMMON_SUFFIXES:
            _add(f"{slug}{suffix}")
            _add(f"{slug}_{suffix}")

    return result


# ── Parsers ────────────────────────────────────────────────────────────────────
def _is_m3u(text: str) -> bool:
    return "#EXTM3U" in text or "#EXTINF" in text or "#EXT-X-STREAM-INF" in text

def parse_m3u_content(text: str, base_url: str) -> list[dict]:
    """
    Parse an M3U/M3U8 file — handles both:
      - Regular playlists (#EXTINF + URL)
      - HLS master manifests (#EXT-X-STREAM-INF + relative path)
    Returns list of {slug, url, name, method}.
    """
    results = []
    seen    = set()
    lines   = text.splitlines()

    def _abs(url: str) -> str:
        if url.startswith("http"):
            return url
        return base_url.rstrip("/") + "/" + url.lstrip("/")

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Regular playlist entry
        if line.startswith("#EXTINF"):
            name_m = re.search(r',(.+)$', line)
            name   = name_m.group(1).strip() if name_m else None
            # Skip (MOJ-R) markers
            if name:
                name = re.sub(r'\(MOJ-R\)', '', name).strip()
            # Look ahead for the URL
            for j in range(i + 1, min(i + 4, len(lines))):
                next_line = lines[j].strip()
                if next_line and not next_line.startswith("#"):
                    url  = _abs(next_line)
                    slug = _slug_from_url(url)
                    if slug and slug not in seen:
                        seen.add(slug)
                        results.append({
                            "slug":   slug,
                            "name":   name or slug.replace("_", " ").title(),
                            "url":    url,
                            "method": "playlist",
                        })
                    i = j
                    break

        # HLS master manifest entry
        elif line.startswith("#EXT-X-STREAM-INF"):
            for j in range(i + 1, min(i + 4, len(lines))):
                next_line = lines[j].strip()
                if next_line and not next_line.startswith("#"):
                    url  = _abs(next_line)
                    slug = _slug_from_url(url)
                    if slug and slug not in seen:
                        seen.add(slug)
                        results.append({
                            "slug":   slug,
                            "name":   slug.replace("_", " ").title(),
                            "url":    url,
                            "method": "playlist",
                        })
                    i = j
                    break

        # Bare URL line (no #EXTINF header) — accept any recognised stream extension
        elif line.startswith("http") and os.path.splitext(line.split("?")[0].lower())[1] in {
            ".m3u8", ".m3u", ".ts", ".mp4", ".mpd", ".flv", ".avi", ".mkv", ".mov",
        }:
            slug = _slug_from_url(line)
            if slug and slug not in seen:
                seen.add(slug)
                results.append({
                    "slug":   slug,
                    "name":   slug.replace("_", " ").title(),
                    "url":    line,
                    "method": "playlist",
                })

        i += 1

    return results

def parse_xtream_json(text: str, base_url: str) -> list[dict]:
    """
    Parse Xtream Codes JSON stream list.
    Constructs stream URLs from stream_id or direct_source field.
    """
    results = []
    seen    = set()
    try:
        data = json.loads(text)
        if not isinstance(data, list):
            return []
        for item in data:
            name      = item.get("name", "")
            stream_id = item.get("stream_id")
            direct    = item.get("direct_source", "")
            ext       = item.get("container_extension", "m3u8")

            if direct and direct.startswith("http"):
                url = direct
            elif stream_id:
                url = f"{base_url.rstrip('/')}/{stream_id}.{ext}"
            else:
                continue

            slug = normalize_slug(name) if name else _slug_from_url(url)
            if slug and slug not in seen:
                seen.add(slug)
                results.append({
                    "slug":   slug,
                    "name":   name or slug.replace("_", " ").title(),
                    "url":    url,
                    "method": "xtream",
                })
    except (json.JSONDecodeError, TypeError):
        pass
    return results

def _slug_from_url(url: str) -> str | None:
    """Extract a slug from a stream URL path."""
    # /SLUG/index.ext  or  /SLUG.ext
    m = re.search(r"/([A-Za-z0-9_\-]+)/[^/]*\.[a-z0-9]+$", url)
    if m:
        return normalize_slug(m.group(1))
    m = re.search(r"/([A-Za-z0-9_\-]+)\.[a-z0-9]{2,5}(?:\?|$)", url)
    if m:
        return normalize_slug(m.group(1))
    return None

# ── Domain Reachability ────────────────────────────────────────────────────────
async def _check_domain(fl: int, sem: asyncio.Semaphore) -> list[dict]:
    """
    Check both https and http for a domain.
    Returns a list of {domain, scheme} — one entry per responsive scheme.
    Both are kept because a domain may serve streams on one scheme only.
    """
    domain  = f"fl{fl}.moveonjoy.com"
    timeout = int(config.get("domain_timeout", 5))
    found   = []
    async with sem:
        for scheme in ("https", "http"):
            rc, code = await _run(
                "curl", "-s",
                "--connect-timeout", "3",
                "--max-time", str(timeout),
                "--insecure", "--location",
                "-A", _UA,
                "-o", "/dev/null",
                "-w", "%{http_code}",
                f"{scheme}://{domain}",
                timeout=timeout + 3,
            )
            if code.strip() in {"200", "301", "302", "403", "404"}:
                found.append({"domain": domain, "scheme": scheme})
    return found

# ── Stage 5: Slug Brute-Force (fallback) ──────────────────────────────────────
def _slugs_from_txt(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    slugs = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                slug = normalize_slug(line)
                if slug:
                    slugs.append(slug)
    return slugs

def _slugs_from_m3u_text(content: str) -> list[str]:
    slugs: set[str] = set()
    for url in re.findall(r"https?://[^\s]+", content):
        s = _slug_from_url(url)
        if s:
            slugs.add(s)
    for name in re.findall(r"#EXTINF:[^,]*,(.+)", content):
        s = normalize_slug(name.replace("(MOJ-R)", "").strip())
        if s:
            slugs.add(s)
    return list(slugs)


def _domain_from_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"https?://([^/:]+)", url)
    return m.group(1) if m else ""

def _strip_moj(name: str) -> str:
    """Remove (MOJ), (MOJ-R), and similar provenance tags from a channel name."""
    return re.sub(r"\s*\(MOJ[^)]*\)\s*", "", name, flags=re.IGNORECASE).strip()


async def _load_playlist_channels() -> list[dict]:
    """
    Fetch the remote playlist URL and return {slug, name, url} for every entry.
    Names have (MOJ)/(MOJ-R) stripped.  Returns [] if not configured or fetch fails.
    """
    url = config.get("playlist_url", "").strip()
    if not url:
        return []
    rc, body = await _run(
        "curl", "-L", "-k", "-s", "-f", "-A", _UA,
        "--max-time", "30", url,
        timeout=35, capture_stdout=True,
    )
    if rc != 0 or not body:
        slog(f"  Remote playlist fetch failed (exit {rc})", "warning")
        return []
    channels = parse_m3u_content(body, "")   # URLs are absolute
    # Strip (MOJ) tags from names
    for ch in channels:
        ch["name"] = _strip_moj(ch["name"])
    slog(f"  Remote playlist: {len(channels)} channel(s) parsed")
    return channels


async def _build_fallback_slugs(
    extra_slugs: list[str] | None = None,
) -> list[str]:
    source = config.get("slug_source", "both")
    seen:  set[str]  = set()
    slugs: list[str] = []

    def _add(new: list[str], label: str) -> None:
        added = sum(1 for s in new if s and s not in seen and not seen.add(s) and slugs.append(s) is None)  # noqa
        slog(f"  {label}: +{added} slug(s)  (total {len(slugs)})")

    # Dead playlist slugs go first — most likely to be healable via brute-force
    if extra_slugs:
        _add(extra_slugs, "dead playlist slugs")

    if source in ("channels", "both"):
        _add(_slugs_from_txt(CHANNELS_FILE), "channels.txt")

    if source in ("playlist", "both"):
        url = config.get("playlist_url", "").strip()
        if url:
            rc, body = await _run(
                "curl", "-L", "-k", "-s", "-f", "-A", _UA,
                "--max-time", "30", url,
                timeout=35, capture_stdout=True,
            )
            if rc == 0 and body:
                _add(_slugs_from_m3u_text(body), "remote playlist")
            else:
                slog(f"  Remote playlist fetch failed (exit {rc})", "warning")

    expanded = _expand_slugs(slugs)
    slog(f"  Slug expansion: {len(slugs)} base → {len(expanded)} total")
    return expanded

async def _brute_force_domain(
    base_url: str,
    slugs: list[str],
    sem: asyncio.Semaphore,
) -> list[dict]:
    """Try each (expanded) slug × pattern, return found streams."""
    patterns = config.get("path_patterns", DEFAULT_CONFIG["path_patterns"])
    found    = []

    async def _try_slug(slug: str) -> dict | None:
        for pattern in patterns:
            url = base_url.rstrip("/") + pattern.replace("{slug}", slug)
            ok  = await _verify_stream(url, sem)
            if ok:
                return {
                    "slug":   slug,
                    "name":   slug.replace("_", " ").title(),
                    "url":    url,
                    "method": "brute",
                }
        return None

    tasks   = [_try_slug(s) for s in slugs]
    results = await asyncio.gather(*tasks)
    found   = [r for r in results if r]
    return found

# ── m3u8 Scraper ─────────────────────────────────────────────────────────────────
# Match anything containing .m3u8 — absolute or relative, any path structure.

_M3U8_ABS_RE = re.compile(r'https?://[^\s"\'<>]*\.m3u8[^\s"\'<>]*', re.IGNORECASE)
_M3U8_REL_RE = re.compile(r'(/[^\s"\'<>]*\.m3u8[^\s"\'<>]*)',        re.IGNORECASE)


async def _discover_playlist(base_url: str) -> list[dict]:
    """
    Fetch the server root (and fallback paths), collect every URL containing
    .m3u8 found anywhere in the response — absolute or relative, any structure.
    Returns a stream entry for each unique URL found.
    """
    found:   list[dict] = []
    seen:    set[str]   = set()
    visited: set[str]   = set()

    def _name(url: str) -> str:
        # Best-effort display name from the URL path
        parts = [p for p in url.split("?")[0].rstrip("/").split("/") if p]
        return parts[-2].replace("_", " ").replace("-", " ").title() if len(parts) >= 2 else parts[-1] if parts else url

    def _scrape(body: str) -> None:
        for m in _M3U8_ABS_RE.finditer(body):
            url = m.group(0).strip().rstrip("\"',>")
            if url not in seen:
                seen.add(url)
                found.append({"url": url, "name": _name(url), "method": "m3u8_scan"})
        for m in _M3U8_REL_RE.finditer(body):
            url = (base_url.rstrip("/") + m.group(1)).strip().rstrip("\"',>")
            if url not in seen:
                seen.add(url)
                found.append({"url": url, "name": _name(url), "method": "m3u8_scan"})

    for path in ["/"] + _PLAYLIST_PATHS:
        url = base_url.rstrip("/") + path
        if url in visited:
            continue
        visited.add(url)

        status, body = await _fetch(url, timeout=10)
        if status != 200 or not body or len(body) < 10:
            continue

        _scrape(body)

        # Also parse if body is a proper M3U (catches #EXTINF entries)
        if _is_m3u(body):
            for s in parse_m3u_content(body, base_url):
                if s["url"] not in seen:
                    seen.add(s["url"])
                    found.append({"url": s["url"], "name": s["name"], "method": "m3u"})

        if found:
            break

    return found
# ── Per-Domain Discovery ───────────────────────────────────────────────────────
async def _discover_domain(
    domain: str,
    scheme: str,
    fallback_slugs: list[str],
    sem: asyncio.Semaphore,
) -> tuple[list[dict], str]:
    """
    Run the full discovery pipeline for one domain.
    Returns (stream_list, method_used).
    Always tries https first then http — HTTPS streams are never missed.
    """
    # Always try https first, then http.
    for s in ("https", "http"):
        base_url = f"{s}://{domain}"

        # Wildcard M3U scan — grab whatever the server exposes
        streams = await _discover_playlist(base_url)
        if streams:
            slog(f"  [{domain}] Wildcard scan ({s}) → {len(streams)} stream(s)")
            return streams, "playlist"

    # Stage 5 — Brute-force fallback (uses preferred scheme)
    base_url = f"{scheme}://{domain}"
    if fallback_slugs:
        slog(f"  [{domain}] Brute-force {len(fallback_slugs)} slug(s) …")
        streams = await _brute_force_domain(base_url, fallback_slugs, sem)
        if streams:
            slog(f"  [{domain}] Brute-force → {len(streams)} hit(s)")
        return streams, "brute"

    return [], "none"

# ── Verify Discovered Streams ──────────────────────────────────────────────────
async def _verify_all(
    streams: list[dict],
    sem: asyncio.Semaphore,
) -> list[dict]:
    """Verify all discovered stream URLs with ffprobe."""
    verified = []

    async def _check(entry: dict) -> dict | None:
        if await _verify_stream(entry["url"], sem):
            return entry
        return None

    results = await asyncio.gather(*[_check(e) for e in streams])
    verified = [r for r in results if r]
    return verified

# ── Scan Phases ────────────────────────────────────────────────────────────────
async def _phase_domains() -> list[dict]:
    state["phase"] = "domains"
    mn, mx = int(config["min_fl"]), int(config["max_fl"])
    slog(f"━━━ Domain sweep fl{mn}–fl{mx} ({mx - mn + 1} hosts) ━━━")
    sem = asyncio.Semaphore(int(config.get("domain_concurrency", 40)))
    raw_lists = await asyncio.gather(*[_check_domain(fl, sem) for fl in range(mn, mx + 1)])
    # Flatten: each call now returns a list (0–2 entries, one per live scheme)
    active = sorted(
        [entry for sublist in raw_lists for entry in sublist],
        key=lambda x: (x["domain"], x["scheme"])
    )
    # domainSchemes: domain → primary scheme (https preferred)
    state["domainSchemes"] = {
        r["domain"]: r["scheme"]
        for r in reversed(active)   # http first so https overwrites
    }
    # activeDomains: unique domain names for display
    state["activeDomains"] = sorted({r["domain"] for r in active})
    slog(f"Found {len(active)} active domain(s): {', '.join(r['domain'] for r in active)}")
    _broadcast({"type": "state", "data": state})
    if not active:
        raise RuntimeError("No active domains found.")
    return active

async def _phase_discover(domains: list[dict]) -> None:
    state["phase"] = "discovering"
    sem        = asyncio.Semaphore(int(config.get("stream_concurrency", 10)))
    seen_urls: set[str] = set()   # global dedup across domains
    dead_urls: list[dict] = []    # playlist entries whose URLs are broken/dead

    # ── Step A: health-check the imported MOJ playlist against known-good domains ──
    if config.get("slug_source", "both") in ("playlist", "both"):
        slog("━━━ Checking imported MOJ playlist ━━━")
        playlist_channels = await _load_playlist_channels()
        if playlist_channels:
            active_domains = {d["domain"] for d in domains}
            # Seed total so progress bar is meaningful during import
            state["total"] += len(playlist_channels)
            _broadcast({"type": "state", "data": state})

            async def _check_playlist_entry(ch: dict) -> None:
                url  = ch.get("url")
                name = ch.get("name") or (url.rsplit("/", 2)[-2].replace("_", " ").title() if url else "Unknown")
                if not url:
                    dead_urls.append(ch)
                    state["progress"] += 1
                    return
                alive = await _verify_stream(url, sem)
                if alive:
                    if url not in seen_urls:
                        seen_urls.add(url)
                        state["results"][url] = {
                            "url":    url,
                            "name":   name,
                            "domain": _domain_from_url(url),
                            "method": "imported",
                        }
                        state["stats"]["streams_verified"] += 1
                        state["stats"]["streams_found"]    += 1
                else:
                    dead_urls.append(ch)
                state["progress"] += 1

            await asyncio.gather(*[_check_playlist_entry(ch) for ch in playlist_channels])
            slog(
                f"  Playlist: {len(seen_urls)} alive (kept), "
                f"{len(dead_urls)} dead → queued for re-scan"
            )
            _broadcast({"type": "state", "data": state})

    # ── Step B: build fallback slug list (channels.txt only — discovery handles the rest) ──
    slog("━━━ Preparing fallback slug list ━━━")
    fallback_slugs = await _build_fallback_slugs(extra_slugs=None)
    slog(f"  Fallback: {len(fallback_slugs)} slug(s) ready")

    for domain_info in domains:
        domain   = domain_info["domain"]
        scheme   = domain_info["scheme"]
        base_url = f"{scheme}://{domain}"

        slog(f"━━━ Discovering {domain} ━━━")
        streams, method = await _discover_domain(domain, scheme, fallback_slugs, sem)

        if not streams:
            slog(f"  [{domain}] Nothing found.")
            continue

        state["stats"]["domains_crawled"] += 1
        state["stats"]["discovery_method"][domain] = method

        # Deduplicate: skip slugs already resolved by a previous domain
        new_streams = [s for s in streams if s["url"] not in seen_urls]
        skipped     = len(streams) - len(new_streams)
        if skipped:
            slog(f"  [{domain}] Skipping {skipped} stream(s) already found on earlier domain.")

        if not new_streams:
            continue

        state["total"] += len(new_streams)
        slog(f"  [{domain}] Verifying {len(new_streams)} stream(s) via ffprobe …")
        verified = await _verify_all(new_streams, sem)

        for entry in verified:
            url = entry["url"]
            if url in seen_urls:
                continue
            seen_urls.add(url)
            state["results"][url] = {
                "url":    url,
                "name":   entry.get("name", url.rsplit("/", 2)[-2].replace("_", " ").title()),
                "domain": domain,
                "method": method,
            }
            state["stats"]["streams_verified"] += 1
            state["progress"]                  += 1

        # Mark failed (discovered but not verified)
        failed_entries = [s for s in new_streams if s["url"] not in seen_urls]
        for entry in failed_entries:
            url = entry["url"]
            seen_urls.add(url)
            state["results"][url] = {
                "url":    None,
                "name":   entry.get("name", url.rsplit("/", 2)[-2].replace("_", " ").title()),
                "domain": domain,
                "method": method,
            }
            state["stats"]["streams_failed"] += 1
            state["progress"]                += 1

        state["stats"]["streams_found"] += len(verified)
        slog(
            f"  [{domain}] ✓ {len(verified)} verified  |  "
            f"{len(new_streams) - len(verified)} failed  |  "
            f"method: {method}"
        )
        _broadcast({"type": "state", "data": state})

async def _phase_write() -> None:
    lines       = ["#EXTM3U\n"]
    found_count = 0
    for slug, info in sorted(state["results"].items()):
        if not info.get("url"):
            continue
        name   = _strip_moj(info.get("name") or slug.replace("_", " ").title())
        extinf = (
            f'#EXTINF:-1 tvg-id="{slug}" tvg-name="{name}" '
            f'group-title="MOJ",{name}'
        )
        lines.append(extinf + "\n")
        lines.append(info["url"] + "\n")
        found_count += 1

    async with aiofiles.open(OUTPUT_FILE, "w", encoding="utf-8", newline="\n") as f:
        await f.write("".join(lines))

    state["lastCycleTime"] = dt.now(local_tz).strftime("%H:%M:%S")
    slog(f"✓ Scan complete — {found_count} working stream(s) written to output playlist.")

# ── Main Scan Cycle ────────────────────────────────────────────────────────────
async def run_cycle() -> None:
    if _cycle_lock.locked():
        log.warning("Cycle already running; skipping duplicate request.")
        return

    async with _cycle_lock:
        state.update({
            "running":      True,
            "results":      {},
            "stats":        _INIT_STATS.copy(),
            "scanLog":      [],
            "progress":     0,
            "probes_fired": 0,
            "total":        0,
        })
        _broadcast({"type": "state", "data": state})

        try:
            domains = await _phase_domains()
            await _phase_discover(domains)
            await _phase_write()
        except RuntimeError as exc:
            slog(str(exc), "error")
        except Exception as exc:
            log.exception(f"Unhandled error in run_cycle: {exc}")
            slog(f"Unexpected error: {exc}", "error")
        finally:
            state.update({"running": False, "phase": "idle"})
            _broadcast({"type": "state", "data": state})

# ── Scheduler ─────────────────────────────────────────────────────────────────
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
            h, rem = divmod(remaining, 3600)
            m, s   = divmod(rem, 60)
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
    yield   # no auto-run on startup

app = FastAPI(title="MOJ Discovery", lifespan=_lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"])
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/", response_class=HTMLResponse)
async def root():
    async with aiofiles.open(os.path.join(WEB_DIR, "index.html")) as f:
        return HTMLResponse(await f.read())

@app.get("/playlist")
async def get_playlist():
    if not os.path.exists(OUTPUT_FILE):
        raise HTTPException(404, "Playlist not ready yet.")
    return FileResponse(OUTPUT_FILE, media_type="application/x-mpegurl")

@app.get("/api/state")
async def api_state():
    return JSONResponse(state)

@app.get("/api/config")
async def api_get_config():
    return JSONResponse(config)

@app.post("/api/config")
async def api_post_config(request: Request):
    global config
    config.update(await request.json())
    save_config(config)
    return {"status": "ok"}

@app.get("/api/channels")
async def api_get_channels():
    if not os.path.exists(CHANNELS_FILE):
        return {"channels": []}
    with open(CHANNELS_FILE, encoding="utf-8", errors="replace") as f:
        lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    return {"channels": lines}

@app.post("/api/channels")
async def api_post_channels(request: Request):
    data = await request.json()
    lines = data.get("channels", [])
    async with aiofiles.open(CHANNELS_FILE, "w", encoding="utf-8") as f:
        await f.write("\n".join(lines) + "\n")
    return {"status": "ok", "count": len(lines)}

@app.post("/api/run")
@app.post("/api/scan/start")
async def api_run():
    if state["running"]:
        raise HTTPException(409, "A scan cycle is already running.")
    asyncio.create_task(run_cycle())
    return {"status": "started"}

@app.post("/api/scan/stop")
async def api_scan_stop():
    state["running"] = False
    state["phase"]   = "idle"
    _broadcast({"type": "state", "data": state})
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
    uvicorn.run(app, host="0.0.0.0", port=8080, log_config=None)
