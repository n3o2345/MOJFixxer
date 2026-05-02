import re
import aiohttp

async def load_playlist(url: str) -> str:
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as r:
            r.raise_for_status()
            return await r.text()

def parse_moj_channels(m3u_text: str) -> list:
    lines    = m3u_text.splitlines()
    channels = []
    current  = None

    for line in lines:
        if line.startswith("#EXTINF"):
            name = line.split(",", 1)[-1].strip()
            if "(MOJ)" in name:
                current = {"name": name, "url": None}
            else:
                current = None
        elif line.startswith("http") and current:
            current["url"] = line.strip()
            channels.append(current)
            current = None

    return channels
