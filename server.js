// BOFU Video Factory - self-hosted FFmpeg worker. No third-party keys live here; Google Drive upload stays in n8n.
// Hosts that sleep after N idle minutes (Render free: 15 min without INBOUND requests) would kill a long render and wipe its files.
// KEEPALIVE_SECONDS>0: while a render is queued/running, and for KEEPALIVE_GRACE_MINUTES after the last authenticated request,
// the worker calls its own PUBLIC_BASE_URL/health (an inbound request for the host). When the pipeline is idle it is allowed to sleep.
// Endpoints. Everything except GET /health and GET / needs "Authorization: Bearer $WORKER_TOKEN" (or "X-Worker-Token: $WORKER_TOKEN");
// GET /files/* needs the bearer token OR the ?sig= signature the worker adds to every file URL it returns
// (HMAC of the file name with WORKER_TOKEN), so OpenAI vision / Shotstack / n8n can download results without the token.
//   POST /scenes   multipart: file, threshold(0.3), keyframes(middle) -> {duration,width,height,audio_present,scenes:[{start,end,keyframe_url}]}
//   POST /probe    {url, frames}         -> {ok,duration,width,height,has_audio,frame_urls[]}
//   POST /archive  {url, key}            -> {archived_url}   (copies a provider clip before its 24 h URL expires)
//   GET  /stats    (token) -> {running,queued,keepalives,...} low-memory settings in effect
//   POST /render   JSON spec, or multipart with field "spec" (JSON) + optional file "audio" (voiceover)
//                  -> {id,status}; GET /render/:id -> {id,status:queued|running|done|failed,url,duration,width,height,has_audio,error}
//   GET  /files/<name>?sig=...  signed download of generated files (deleted after FILE_TTL_HOURS / ARCHIVE_TTL_HOURS)
//   Render clips may carry "fallback_url" (e.g. the provider URL of an archived clip) used if "url" cannot be downloaded.
'use strict';
const http = require('http'); const fs = require('fs'); const fsp = fs.promises; const path = require('path'); const crypto = require('crypto');
const { spawn } = require('child_process');
let Busboy; try { Busboy = require('busboy'); } catch (e) { Busboy = null; }

const PORT = Number(process.env.PORT || 8080);
const TOKEN = process.env.WORKER_TOKEN || '';
const DATA = process.env.DATA_DIR || '/data';
const BASE = (process.env.PUBLIC_BASE_URL || `http://localhost:${PORT}`).replace(/\/$/, '');
const FILE_TTL_H = Number(process.env.FILE_TTL_HOURS || 72), ARCHIVE_TTL_H = Number(process.env.ARCHIVE_TTL_HOURS || 168);
const MAX_RENDERS = Math.max(1, Number(process.env.MAX_CONCURRENT_RENDERS || 2));
const MAX_DOWNLOAD = Number(process.env.MAX_DOWNLOAD_BYTES || 500 * 1024 * 1024);
const FONT = process.env.FONT_FILE || '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf';
// Low-memory / small-CPU tuning (defaults = full speed; the Render free package sets these in its Dockerfile):
const THREADS = Math.max(0, Number(process.env.FFMPEG_THREADS || 0));            // 0 = ffmpeg decides; 1 = least memory
const PRESET = process.env.X264_PRESET || 'veryfast';                              // final encode
const SEG_PRESET = process.env.SEGMENT_PRESET || PRESET;                           // intermediate per-clip encodes
const MAX_H = Math.max(0, Number(process.env.MAX_OUTPUT_HEIGHT || 0));             // e.g. 960 -> 540x960 output; 0 = as requested
const SCENE_W = Math.max(0, Number(process.env.SCENE_ANALYSIS_WIDTH || 0));        // downscale before scene scoring; 0 = full size
const KEEPALIVE_S = Math.max(0, Number(process.env.KEEPALIVE_SECONDS || 0)), KEEPALIVE_GRACE_MIN = Math.max(0, Number(process.env.KEEPALIVE_GRACE_MINUTES || 45));
let lastActivity = 0, keepalives = 0;
const FILES = path.join(DATA, 'files'), WORK = path.join(DATA, 'work'), JOBS = path.join(DATA, 'jobs');
for (const d of [FILES, WORK, JOBS]) fs.mkdirSync(d, { recursive: true });
const MIN_TOKEN = 32;
if (TOKEN.length < MIN_TOKEN) console.warn(`WARNING: WORKER_TOKEN is missing or shorter than ${MIN_TOKEN} characters - every protected endpoint refuses requests (fail closed)`);

const rid = (n = 12) => crypto.randomBytes(n).toString('hex');
const json = (res, code, obj) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(obj)); };
const authed = req => {
  if (TOKEN.length < MIN_TOKEN) return false;
  // "Authorization: Bearer <token>" or "X-Worker-Token: <token>" (use the latter behind proxies that use Authorization themselves, e.g. Hugging Face)
  const eq = (g, w) => { const a = Buffer.from(String(g || '')), b = Buffer.from(w); return a.length === b.length && crypto.timingSafeEqual(a, b); };
  return eq(req.headers.authorization, `Bearer ${TOKEN}`) || eq(req.headers['x-worker-token'], TOKEN);
};
const sign = name => crypto.createHmac('sha256', TOKEN || rid()).update('files/' + name).digest('hex').slice(0, 40);
const fileUrl = name => `${BASE}/files/${name}?sig=${sign(name)}`;
const sigOk = (name, sig) => { const a = Buffer.from(String(sig || '')), b = Buffer.from(sign(name)); return TOKEN.length >= MIN_TOKEN && a.length === b.length && crypto.timingSafeEqual(a, b); };

// ffmpeg with the thread limit applied to every input (decoder) and to the output (encoder / filters)
const ffArgs = args => { if (!THREADS) return args; const out = ['-filter_threads', String(THREADS)];
  args.forEach((a, i) => { if (a === '-i' || i === args.length - 1) out.push('-threads', String(THREADS)); out.push(a); }); return out; };
const ff = (args, o) => run('ffmpeg', ffArgs(args), o);
function run(cmd, args, { timeoutMs = Number(process.env.FFMPEG_TIMEOUT_MS || 1800000) } = {}) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'] }); let out = '', err = '';
    const t = setTimeout(() => { p.kill('SIGKILL'); reject(new Error(`${cmd} timed out`)); }, timeoutMs);
    p.stdout.on('data', d => { out += d; }); p.stderr.on('data', d => { err += d; if (err.length > 2e6) err = err.slice(-1e6); });
    p.on('close', code => { clearTimeout(t); code === 0 ? resolve({ out, err }) : reject(new Error(`${cmd} exited ${code}: ${err.slice(-800)}`)); });
  });
}
async function probe(file) {
  const { out } = await run('ffprobe', ['-v', 'error', '-print_format', 'json', '-show_format', '-show_streams', file], { timeoutMs: 60000 });
  const j = JSON.parse(out); const v = (j.streams || []).find(s => s.codec_type === 'video') || {};
  let w = Number(v.width || 0), h = Number(v.height || 0);
  const rot = Number((v.side_data_list || []).find(x => x.rotation !== undefined)?.rotation || v.tags?.rotate || 0);
  if (Math.abs(rot) === 90) [w, h] = [h, w];
  return { duration: +Number(j.format?.duration || v.duration || 0).toFixed(3), width: w, height: h, has_audio: (j.streams || []).some(s => s.codec_type === 'audio') };
}
async function download(url, dest, fallback) {
  try { return await download1(url, dest); }
  catch (e) { if (fallback && fallback !== url) return download1(fallback, dest); throw e; }
}
async function download1(url, dest) {
  const local = url.startsWith(BASE + '/files/') ? path.join(FILES, path.basename(new URL(url).pathname)) : null;
  if (local && fs.existsSync(local)) { await fsp.copyFile(local, dest); return dest; }
  if (!/^https?:\/\//.test(url)) throw new Error('only http(s) URLs are accepted');
  const r = await fetch(url, { redirect: 'follow' });
  if (!r.ok) throw new Error(`download ${r.status} for ${url.slice(0, 120)}`);
  const len = Number(r.headers.get('content-length') || 0); if (len > MAX_DOWNLOAD) throw new Error('file too large');
  // streamed straight to disk, so a large clip never sits in memory (matters on 512 MB hosts)
  let n = 0; const ws = fs.createWriteStream(dest);
  try {
    for await (const chunk of r.body) { n += chunk.length; if (n > MAX_DOWNLOAD) throw new Error('file too large'); if (!ws.write(chunk)) await new Promise(ok => ws.once('drain', ok)); }
  } catch (e) { ws.destroy(); await fsp.rm(dest, { force: true }); throw e; }
  await new Promise((ok, ko) => ws.end(err => err ? ko(err) : ok())); return dest;
}
function readBody(req, limit = 5e6) {
  return new Promise((resolve, reject) => { let b = ''; req.on('data', d => { b += d; if (b.length > limit) { reject(new Error('body too large')); req.destroy(); } }); req.on('end', () => resolve(b)); req.on('error', reject); });
}
function readMultipart(req, dir) {
  return new Promise((resolve, reject) => {
    if (!Busboy) return reject(new Error('busboy not installed'));
    const fields = {}, files = {}; const writes = [];
    const bb = Busboy({ headers: req.headers, limits: { fileSize: MAX_DOWNLOAD } });
    bb.on('field', (k, v) => { fields[k] = v; });
    bb.on('file', (k, stream, info) => { const f = path.join(dir, `${k}-${rid(4)}${path.extname(info.filename || '') || ''}`); files[k] = f;
      writes.push(new Promise((ok, ko) => stream.pipe(fs.createWriteStream(f)).on('finish', ok).on('error', ko))); });
    bb.on('close', () => Promise.all(writes).then(() => resolve({ fields, files }), reject)); bb.on('error', reject);
    req.pipe(bb);
  });
}
const wrap = (text, width) => { const words = String(text).replace(/\s+/g, ' ').trim().split(' '); const lines = []; let cur = '';
  for (const w of words) { if ((cur + ' ' + w).trim().length > width && cur) { lines.push(cur); cur = w; } else cur = (cur + ' ' + w).trim(); } if (cur) lines.push(cur); return lines.join('\n'); };
const esc = p => p.replace(/\\/g, '/').replace(/:/g, '\\:').replace(/'/g, "\\'");

// ------------------------------------------------------------------ /scenes
async function scenes(req, res) {
  const dir = path.join(WORK, 'scenes-' + rid(6)); await fsp.mkdir(dir, { recursive: true });
  try {
    const { fields, files } = await readMultipart(req, dir); if (!files.file) return json(res, 400, { error: 'multipart field "file" required' });
    const meta = await probe(files.file); const thr = Math.min(0.9, Math.max(0.05, Number(fields.threshold || 0.3)));
    const { err } = await ff(['-hide_banner', '-i', files.file, '-filter:v', `${SCENE_W ? `scale=${SCENE_W}:-2,` : ''}select='gt(scene,${thr})',showinfo`, '-an', '-f', 'null', '-']);
    const cuts = [...err.matchAll(/pts_time:([\d.]+)/g)].map(m => Number(m[1])).filter(t => t > 0.2 && t < meta.duration - 0.2);
    const bounds = [0, ...cuts, meta.duration]; const out = [];
    for (let i = 0; i < bounds.length - 1; i++) {
      const start = +bounds[i].toFixed(3), end = +bounds[i + 1].toFixed(3); if (end - start < 0.1) continue;
      const name = `kf-${rid(10)}.jpg`;
      await ff(['-y', '-ss', String((start + end) / 2), '-i', files.file, '-frames:v', '1', '-q:v', '3', path.join(FILES, name)]);
      out.push({ start, end, keyframe_url: fileUrl(name) });
    }
    json(res, 200, { duration: meta.duration, width: meta.width, height: meta.height, audio_present: meta.has_audio, scenes: out });
  } finally { fsp.rm(dir, { recursive: true, force: true }).catch(() => {}); }
}
// ------------------------------------------------------------------ /probe
async function probeUrl(req, res) {
  const { url, frames = 3 } = JSON.parse(await readBody(req) || '{}'); if (!url) return json(res, 400, { error: 'url required' });
  const dir = path.join(WORK, 'probe-' + rid(6)); await fsp.mkdir(dir, { recursive: true });
  try {
    const f = await download(url, path.join(dir, 'in.mp4')); const m = await probe(f); const n = Math.max(0, Math.min(8, Number(frames)));
    const frame_urls = [];
    for (let i = 0; i < n; i++) { const name = `fr-${rid(10)}.jpg`; await ff(['-y', '-ss', String(m.duration * (i + 1) / (n + 1)), '-i', f, '-frames:v', '1', '-q:v', '4', path.join(FILES, name)]); frame_urls.push(fileUrl(name)); }
    json(res, 200, { ok: m.duration > 0 && m.width > 0, ...m, frame_urls });
  } catch (e) { json(res, 200, { ok: false, error: e.message }); } finally { fsp.rm(dir, { recursive: true, force: true }).catch(() => {}); }
}
// ------------------------------------------------------------------ /archive
async function archive(req, res) {
  const { url, key = '' } = JSON.parse(await readBody(req) || '{}'); if (!url) return json(res, 400, { error: 'url required' });
  const name = `ar-${rid(10)}-${String(key).replace(/[^A-Za-z0-9_-]+/g, '_').slice(0, 60)}.mp4`;
  await download(url, path.join(FILES, name));
  json(res, 200, { archived_url: fileUrl(name) });
}
// ------------------------------------------------------------------ /render (async queue)
const jobs = new Map(); const queue = []; let running = 0;
const saveJob = j => { jobs.set(j.id, j); fs.writeFileSync(path.join(JOBS, j.id + '.json'), JSON.stringify(j)); };
const loadJob = id => jobs.get(id) || (fs.existsSync(path.join(JOBS, id + '.json')) ? JSON.parse(fs.readFileSync(path.join(JOBS, id + '.json'), 'utf8')) : null);
function validateSpec(s) {
  if (!s || !Array.isArray(s.clips) || !s.clips.length) throw new Error('spec.clips required');
  for (const c of s.clips) { if (!['video', 'image', 'text'].includes(c.type)) throw new Error('clip.type must be video|image|text'); if (!(Number(c.duration) > 0)) throw new Error('clip.duration must be > 0');
    if (c.type !== 'text' && !c.url) throw new Error('clip.url required'); }
  if (s.clips.reduce((a, c) => a + Number(c.duration), 0) > 180) throw new Error('total duration over 180 s');
}
async function renderJob(j) {
  const s = j.spec; let W = Number(s.output?.width || 720), H = Number(s.output?.height || 1280); const FPS = Number(s.output?.fps || 30);
  if (MAX_H && H > MAX_H) { W = Math.round(W * MAX_H / H / 2) * 2; H = MAX_H; }   // same 9:16 shape, fewer pixels
  const dir = path.join(WORK, 'render-' + j.id); await fsp.mkdir(dir, { recursive: true });
  try {
    const segs = []; const norm = `scale=${W}:${H}:force_original_aspect_ratio=increase,crop=${W}:${H},fps=${FPS},setsar=1,format=yuv420p`;
    for (const [i, c] of s.clips.entries()) {
      const out = path.join(dir, `seg${i}.mp4`); const d = String(Number(c.duration));
      const enc = ['-an', '-c:v', 'libx264', '-preset', SEG_PRESET, '-crf', '20', '-r', String(FPS), out];
      if (c.type === 'video') { const f = await download(c.url, path.join(dir, `in${i}`), c.fallback_url); await ff(['-y', '-i', f, '-t', d, '-vf', `${norm},tpad=stop_mode=clone:stop_duration=${d}`, ...enc]); }
      else if (c.type === 'image') { const f = await download(c.url, path.join(dir, `in${i}`), c.fallback_url); await ff(['-y', '-loop', '1', '-i', f, '-t', d, '-vf', norm, ...enc]); }
      else { const tf = path.join(dir, `card${i}.txt`); await fsp.writeFile(tf, wrap(c.text || ' ', 16));
        await ff(['-y', '-f', 'lavfi', '-i', `color=c=black:s=${W}x${H}:r=${FPS}`, '-t', d, '-vf', `drawtext=fontfile='${esc(FONT)}':textfile='${esc(tf)}':fontcolor=white:fontsize=${Math.round(W / 11)}:line_spacing=12:x=(w-text_w)/2:y=(h-text_h)/2,format=yuv420p`, ...enc]); }
      segs.push(out);
    }
    const list = path.join(dir, 'list.txt'); await fsp.writeFile(list, segs.map(f => `file '${f}'`).join('\n'));
    const joined = path.join(dir, 'joined.mp4'); await ff(['-y', '-f', 'concat', '-safe', '0', '-i', list, '-c', 'copy', joined]);
    const filters = [];
    for (const [i, o] of (s.overlays || []).entries()) {
      const tf = path.join(dir, `ov${i}.txt`); await fsp.writeFile(tf, wrap(o.text, 22));
      filters.push(`drawtext=fontfile='${esc(FONT)}':textfile='${esc(tf)}':fontcolor=white:fontsize=${Math.round(W / 16)}:line_spacing=8:box=1:boxcolor=black@0.45:boxborderw=14:x=(w-text_w)/2:y=h*0.12:enable='between(t,${Number(o.start)},${Number(o.end)})'`);
    }
    if (s.label) { const tf = path.join(dir, 'label.txt'); await fsp.writeFile(tf, String(s.label).slice(0, 40));
      filters.push(`drawtext=fontfile='${esc(FONT)}':textfile='${esc(tf)}':fontcolor=yellow:fontsize=${Math.round(W / 30)}:x=16:y=16:box=1:boxcolor=black@0.5:boxborderw=6`); }
    const name = `out-${rid(12)}.mp4`; const final = path.join(FILES, name);
    const args = ['-y', '-i', joined]; if (j.audio) args.push('-i', j.audio);
    if (filters.length) args.push('-vf', filters.join(','));
    args.push('-map', '0:v:0');
    if (j.audio) { const total = s.clips.reduce((a, c) => a + Number(c.duration), 0); args.push('-map', '1:a:0', '-c:a', 'aac', '-b:a', '128k', '-af', `apad,atrim=0:${total}`); }
    args.push('-c:v', 'libx264', '-preset', PRESET, '-crf', '21', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', final);
    await run('ffmpeg', args);
    const m = await probe(final);
    saveJob({ ...j, audio: undefined, status: 'done', url: fileUrl(name), ...m, finished_at: new Date().toISOString() });
  } catch (e) { saveJob({ ...j, audio: undefined, status: 'failed', error: e.message.slice(0, 1000), finished_at: new Date().toISOString() }); }
  finally { fsp.rm(dir, { recursive: true, force: true }).catch(() => {}); }
}
function pump() { while (running < MAX_RENDERS && queue.length) { const j = queue.shift(); running++; saveJob({ ...j, status: 'running' }); renderJob(j).finally(() => { running--; pump(); }); } }
async function render(req, res) {
  let spec, audio = null;
  if (String(req.headers['content-type'] || '').startsWith('multipart/')) {
    const dir = path.join(WORK, 'up-' + rid(6)); await fsp.mkdir(dir, { recursive: true });
    const { fields, files } = await readMultipart(req, dir); spec = JSON.parse(fields.spec || '{}'); audio = files.audio || null;
  } else spec = JSON.parse(await readBody(req) || '{}');
  try { validateSpec(spec); } catch (e) { return json(res, 400, { error: e.message }); }
  // idempotent: the same spec (+ same audio) returns the same render job instead of rendering twice
  const audioHash = audio ? crypto.createHash('sha1').update(fs.readFileSync(audio)).digest('hex') : '';
  const id = crypto.createHash('sha1').update(JSON.stringify(spec) + audioHash).digest('hex').slice(0, 24);
  const prev = loadJob(id);
  if (prev && prev.status !== 'failed') return json(res, 200, { id, status: prev.status, url: prev.url || '' });
  const j = { id, spec, audio, status: 'queued', created_at: new Date().toISOString() }; saveJob({ ...j, audio: undefined }); queue.push(j); pump();
  json(res, 202, { id, status: 'queued' });
}
// ------------------------------------------------------------------ cleanup
async function cleanup() {
  const now = Date.now();
  for (const f of await fsp.readdir(FILES).catch(() => [])) {
    const st = await fsp.stat(path.join(FILES, f)).catch(() => null); if (!st) continue;
    const ttl = (f.startsWith('ar-') ? ARCHIVE_TTL_H : FILE_TTL_H) * 3600e3; if (now - st.mtimeMs > ttl) await fsp.rm(path.join(FILES, f), { force: true });
  }
}
setInterval(() => cleanup().catch(() => {}), 3600e3).unref();
// A restart (deploy, crash, free-tier sleep) kills in-flight renders: report them as failed so n8n re-submits instead of waiting forever.
for (const f of fs.readdirSync(JOBS).filter(x => x.endsWith('.json'))) {
  try { const j = JSON.parse(fs.readFileSync(path.join(JOBS, f), 'utf8'));
    if (['queued', 'running'].includes(j.status)) fs.writeFileSync(path.join(JOBS, f), JSON.stringify({ ...j, status: 'failed', error: 'worker restarted during render - submit again' })); } catch (e) {}
}

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x');
  try {
    // public: liveness only (no counts, no config); token_ok tells you whether the WORKER_TOKEN secret is set correctly
    if (req.method === 'GET' && u.pathname === '/health') return json(res, 200, { ok: true, service: 'vf-ffmpeg-worker', token_ok: TOKEN.length >= MIN_TOKEN });
    if (req.method === 'GET' && u.pathname === '/') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('BOFU Video Factory FFmpeg worker. Private API: a bearer token is required.\n'); }
    if ((req.method === 'GET' || req.method === 'HEAD') && u.pathname.startsWith('/files/')) {
      const name = path.basename(u.pathname);
      if (!authed(req) && !sigOk(name, u.searchParams.get('sig'))) return json(res, 401, { error: 'unauthorized' });
      const f = path.join(FILES, name); if (!fs.existsSync(f)) return json(res, 404, { error: 'not found' });
      const st = fs.statSync(f); res.writeHead(200, { 'content-type': f.endsWith('.jpg') ? 'image/jpeg' : 'video/mp4', 'content-length': st.size });
      if (req.method === 'HEAD') return res.end(); return fs.createReadStream(f).pipe(res);
    }
    if (!authed(req)) return json(res, 401, { error: 'unauthorized' });
    if (req.method === 'GET' && u.pathname === '/stats') return json(res, 200, { running, queued: queue.length, keepalives, keepalive_seconds: KEEPALIVE_S, threads: THREADS, preset: PRESET, segment_preset: SEG_PRESET, max_output_height: MAX_H });   // monitoring; does not count as activity
    lastActivity = Date.now();
    if (req.method === 'POST' && u.pathname === '/scenes') return await scenes(req, res);
    if (req.method === 'POST' && u.pathname === '/probe') return await probeUrl(req, res);
    if (req.method === 'POST' && u.pathname === '/archive') return await archive(req, res);
    if (req.method === 'POST' && u.pathname === '/render') return await render(req, res);
    if (req.method === 'GET' && u.pathname.startsWith('/render/')) { const j = loadJob(path.basename(u.pathname)); if (!j) return json(res, 404, { error: 'not found' });
      const { spec, audio, ...pub } = j; return json(res, 200, pub); }
    json(res, 404, { error: 'not found' });
  } catch (e) { json(res, 500, { error: e.message.slice(0, 500) }); }
});
if (KEEPALIVE_S && process.env.PUBLIC_BASE_URL && /^https?:\/\//.test(BASE)) setInterval(() => {
  const busy = running > 0 || queue.length > 0, recent = Date.now() - lastActivity < KEEPALIVE_GRACE_MIN * 60e3;
  if (busy || recent) fetch(BASE + '/health').then(() => { keepalives++; }).catch(() => {});
}, KEEPALIVE_S * 1000).unref();
if (require.main === module) server.listen(PORT, () => console.log(`vf-ffmpeg-worker on :${PORT}${KEEPALIVE_S ? ` (keep-alive every ${KEEPALIVE_S}s while busy / ${KEEPALIVE_GRACE_MIN} min after activity)` : ''}`));
module.exports = { server };
