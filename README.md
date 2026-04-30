# MOJFixxer

A self-hosted IPTV stream manager. Crawls `moveonjoy.com` domains to discover and health-check live streams, serving results as an M3U8 playlist through a FastAPI web interface.

[![Build and Push to GHCR](https://github.com/YOUR_USERNAME/mojfixxer/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/YOUR_USERNAME/mojfixxer/actions/workflows/docker-publish.yml)

---

## Features

- **Discovery** — one-time scan across all active `flN.moveonjoy.com` domains using directory listing, master playlists, Xtream API, and brute-force slug matching
- **Health Check** — tests every known channel with `ffprobe`; dead channels are automatically re-found across all active domains
- **Web UI** — real-time dashboard over WebSocket with live logs, channel table, and settings
- **Scheduled health checks** — optional auto-run on a configurable interval
- **Persistent data** — config, output playlist, and results survive container restarts via a named volume

---

## Quick Start

### Docker Compose (recommended)

```bash
# 1. Clone the repo (or just grab docker-compose.yml)
git clone https://github.com/YOUR_USERNAME/mojfixxer.git
cd mojfixxer

# 2. Pull and start
GITHUB_REPOSITORY=YOUR_USERNAME/mojfixxer docker compose up -d

# 3. Open the dashboard
open http://localhost:9001
```

The playlist is available at `http://localhost:9001/playlist`.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `TZ` | `America/Chicago` | Timezone for log timestamps |
| `HOST_PORT` | `9001` | Host port mapped to the container |


---

## Building Locally

```bash
docker build -t mojfixxer .
docker run -p 9001:8080 -v mojfixxer-data:/app mojfixxer
```

## TrueNAS / NAS Deployment

Map a host path instead of a named volume so data survives app removal:

```yaml
volumes:
  - /mnt/pool/mojfixxer/data:/app
```

Bind the port to a specific NIC if needed:

```yaml
ports:
  - "192.168.1.x:9001:8080"
```
