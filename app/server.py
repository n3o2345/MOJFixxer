import re
import aiohttp
import aiofiles
from datetime import datetime as dt
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()
app.mount("/static", StaticFiles(directory="app/web/static"), name="static")

# ── STATE ─────────────────────────────────────────────────────────────────────
state = {
    "results": {},
    "moj": {
        "input": 0,
        "processed": 0,
        "working": 0,
        "failed": 0
    }
}

config = {
    "playlist_url": ""
}

OUTPUT_FILE = "output.m3u"


# ── LOG ───────────────────────────────────────────────────────────────────────
def slog(msg):
    print(msg)


# ── NORMALIZATION / MATCHING ENGINE ───────────────────────────────────────────
def normalize(s: str) -> str:
    s = s.lower()
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


def tokenize(s: str):
    s = s.lower()
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return set(s.split())


ALIASES = {
    "espn2": ["espn 2", "espn2hd"],
    "foxsports1": ["fs1"],
    "nbcsn": ["nbc sports"]
}


def expand_alias(tokens: set):
    expanded = set(tokens)
    joined = "".join(tokens)

    for k, vals in ALIASES.items():
        if k in joined:
            for v in vals:
                expanded.update(tokenize(v))

    return expanded


def score_match(channel_name: str, slug: str) -> float:
    n1 = normalize(channel_name)
    n2 = normalize(slug)

    if n1 == n2:
        return 100

    t1 = expand_alias(tokenize(channel_name))
    t2 = expand_alias(tokenize(slug))

    if not t1 or not t2:
        return 0

    overlap = len(t1 & t2)
    union = len(t1 | t2)

    score = overlap / union

    if n1 in n2 or n2 in n1:
        score += 0.5

    return score


def find_best_match(channel_name, results):
    best_slug = None
    best_score = 0

    for slug in results.keys():
        s = score_match(channel_name, slug)

        if s > best_score:
            best_score = s
            best_slug = slug

    if best_score < 0.3:
        return None

    return best_slug


# ── MOJ PLAYLIST LOADER ───────────────────────────────────────────────────────
async def load_moj_channels():
    url = config.get("playlist_url")
    if not url:
        return {}

    moj = {}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=15) as r:
                text = await r.text()
    except Exception as e:
        slog(f"⚠ Failed to load playlist: {e}")
        return {}

    current = None

    for line in text.splitlines():
        if line.startswith("#EXTINF"):
            name = line.split(",")[-1].strip()

            if "(MOJ)" in name:
                slug = normalize(name)
                current = slug
                moj[slug] = name
            else:
                current = None

        elif line.startswith("http") and current:
            current = None

    slog(f"✓ Loaded {len(moj)} MOJ channels")
    return moj


# ── PHASE WRITE (UPDATED) ─────────────────────────────────────────────────────
async def _phase_write():
    lines = ["#EXTM3U\n"]

    moj_channels = await load_moj_channels()

    found_count = 0
    processed = 0
    failed = 0

    for slug_key, channel_name in moj_channels.items():
        processed += 1

        matched_slug = find_best_match(channel_name, state["results"])

        if not matched_slug:
            failed += 1
            continue

        info = state["results"].get(matched_slug)

        if not info or not info.get("url"):
            failed += 1
            continue

        extinf = (
            f'#EXTINF:-1 tvg-id="{matched_slug}" '
            f'tvg-name="{channel_name}" group-title="MOJ",{channel_name}'
        )

        lines.append(extinf + "\n")
        lines.append(info["url"] + "\n")

        found_count += 1

    async with aiofiles.open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        await f.write("".join(lines))

    state["moj"] = {
        "input": len(moj_channels),
        "processed": processed,
        "working": found_count,
        "failed": failed
    }

    slog(f"✓ MOJ done: {found_count} working / {failed} failed")


# ── ROUTES ────────────────────────────────────────────────────────────────────
@app.get("/")
def index():
    return FileResponse("app/web/index.html")