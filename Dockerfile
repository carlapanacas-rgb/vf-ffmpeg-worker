# BOFU Video Factory - FFmpeg worker for Render.com (Web Service, Docker runtime, Free plan: 512 MB RAM, 0.1 CPU).
FROM node:20-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg fonts-dejavu-core ca-certificates \
 && rm -rf /var/lib/apt/lists/*
USER node
ENV HOME=/home/node
WORKDIR /home/node/app
COPY --chown=node:node package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY --chown=node:node server.js ./
# Render sets PORT itself (default 10000). The free disk is ephemeral (wiped on sleep/restart/redeploy), so data goes to /tmp.
# WORKER_TOKEN and PUBLIC_BASE_URL come from the Render "Environment" settings at runtime - never put them here.
# Low-memory defaults for the 512 MB / 0.1 CPU free instance (measured: 18 s 720x1280 draft, peak ~307 MB, 0 OOM).
# Any of these can be overridden in the Render "Environment" tab. If you ever see "out of memory" restarts,
# add MAX_OUTPUT_HEIGHT=960 (540x960 drafts, peak ~207 MB).
ENV PORT=10000 DATA_DIR=/tmp/vf-data \
    MAX_CONCURRENT_RENDERS=1 FFMPEG_THREADS=1 SEGMENT_PRESET=ultrafast X264_PRESET=veryfast \
    SCENE_ANALYSIS_WIDTH=320 MAX_OUTPUT_HEIGHT=0 FFMPEG_TIMEOUT_MS=1800000 \
    FILE_TTL_HOURS=24 ARCHIVE_TTL_HOURS=48 \
    KEEPALIVE_SECONDS=300 KEEPALIVE_GRACE_MINUTES=45 \
    NODE_OPTIONS=--max-old-space-size=160
EXPOSE 10000
CMD ["node", "server.js"]
