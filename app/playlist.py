import re
import requests

def load_playlist(url):
    res = requests.get(url, timeout=10)
    res.raise_for_status()
    return res.text

def parse_moj_channels(m3u_text):
    lines = m3u_text.splitlines()

    channels = []
    current = None

    for line in lines:
        if line.startswith("#EXTINF"):
            name = line.split(",")[-1].strip()

            # ✅ FILTER ONLY (MOJ)
            if "(MOJ)" in name:
                current = {
                    "name": name,
                    "url": None
                }
            else:
                current = None

        elif line.startswith("http") and current:
            current["url"] = line.strip()
            channels.append(current)
            current = None

    return channels