# BOFU Video Factory: FFmpeg worker (Render.com Free)

A private API that n8n calls. It is not a web app. Every endpoint except `GET /health` and `GET /` needs the worker token,
sent as `X-Worker-Token: <token>` (what the n8n credential uses) or `Authorization: Bearer <token>`. No keys live in these files.
Setup steps for a non-developer: `RENDER_GUIDE.md` in the project.

## Files

| File | Purpose |
|---|---|
| `Dockerfile` | Node 20 + ffmpeg, runs as a normal user, port 10000, data in `/tmp`, low-memory settings. |
| `server.js` | The worker (identical to the main and Hugging Face copies). |
| `package.json` | One dependency (busboy, for uploads). |
| `render.yaml` | Optional Render Blueprint: free web service, Docker, health check `/health`. |
| `.dockerignore` | Keeps docs out of the image. Optional. |

## Endpoints

| Endpoint | What it does |
|---|---|
| `GET /health` | `{"ok":true,"service":"vf-ffmpeg-worker","token_ok":true}`. `token_ok:false` = `WORKER_TOKEN` missing or under 32 characters; everything protected then returns 401. |
| `GET /stats` | (token) Running/queued renders and the ffmpeg settings in effect. |
| `POST /scenes` | Scene cuts and keyframes of a reference video. |
| `POST /probe` | Playback info and QA frames. |
| `POST /archive` | Copies a clip before its 24 h provider link expires. |
| `POST /render`, `GET /render/:id` | Async 9:16 assembly (one at a time on Free). |
| `GET /files/<name>?sig=…` | Result files; needs the signature the worker adds, or the token. |

## Environment (Render → service → Environment)

| Name | Required | Value |
|---|---|---|
| `WORKER_TOKEN` | yes | 48 letters/digits. Same value in the n8n credential "FFmpeg Worker Auth" (header `X-Worker-Token`). |
| `PUBLIC_BASE_URL` | yes | `https://<service-name>.onrender.com`, no trailing slash. |
| `MAX_OUTPUT_HEIGHT` | no | `0` = 720×1280 (default). `960` = 540×960 drafts, less memory and ~30% faster. |
| `FFMPEG_THREADS`, `SEGMENT_PRESET`, `X264_PRESET`, `SCENE_ANALYSIS_WIDTH` | no | Defaults `1`, `ultrafast`, `veryfast`, `320`. |
| `MAX_CONCURRENT_RENDERS` | no | Default `1`. Keep 1 on 512 MB. |
| `KEEPALIVE_SECONDS`, `KEEPALIVE_GRACE_MINUTES` | no | Default `300`, `45`. Set `KEEPALIVE_SECONDS=0` to turn off. |
| `FILE_TTL_HOURS`, `ARCHIVE_TTL_HOURS` | no | Default `24`, `48`. |

`PORT` is set by Render (10000). Do not set it.

## Free-plan limits (checked on render.com/docs/free, Nov 2026)

- 512 MB RAM, 0.1 CPU, no credit card needed.
- Sleeps after 15 min without incoming requests; waking takes about 1 minute.
- The disk is wiped on sleep, restart and redeploy. Renders in progress are reported failed and n8n re-submits; archived clips fall back to the provider URL (valid 24 h).
- 750 free instance hours per month (enough for one service running all month); 5 GB outbound per month (about 1,000 drafts). Without a card, going over suspends the service until next month.
- The keep-alive: while a render is queued or running, and for 45 min after the last n8n call, the worker calls its own `PUBLIC_BASE_URL/health` every 5 min, so Render does not put it to sleep mid-job. When n8n stops calling, it sleeps normally.

## Measured (local, 512 MB / 0.1 CPU cgroup, SAMPLE media)

18 s 720×1280 draft with voiceover: about 95 s, peak about 304 MB, no out-of-memory. 30 s reference scene detection: about 49 s.
With `MAX_OUTPUT_HEIGHT=960`: about 66 s, peak about 207 MB.
