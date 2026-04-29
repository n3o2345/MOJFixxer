FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg curl ca-certificates openssl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip --quiet \
    && pip install --no-cache-dir fastapi "uvicorn[standard]" aiofiles websockets aiohttp

RUN mkdir -p /opt/moj/web/static /app/logs

# Copy source files
COPY app/server.py        /opt/moj/server.py
COPY app/web/index.html   /opt/moj/web/index.html
COPY app/web/static/      /opt/moj/web/static/

# Seed default data files only if not already present (volume mount will override)
COPY app/channels.txt     /opt/moj-defaults/channels.txt
COPY data/config.json     /opt/moj-defaults/config.json

# Entrypoint seeds /app on first run then launches the server
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENV PYTHONUNBUFFERED=1 TZ=America/Chicago

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD curl -sf http://localhost:8080/api/state || exit 1

EXPOSE 8080
WORKDIR /opt/moj
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["python", "server.py"]
