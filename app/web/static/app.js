// ── Config ────────────────────────────────────────────────────────────────────
const POLL_IDLE   = 3000;
const POLL_ACTIVE = 800;
const WS_RETRY    = 3000;

// ── State ─────────────────────────────────────────────────────────────────────
let ws          = null;
let wsConnected = false;
let pollTimer   = null;
let logCount    = 0;

let appState = null;

// ── WebSocket ─────────────────────────────────────────────────────────────────
function connectWS() {
  try {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(`${proto}//${location.host}/ws`);
    ws.onopen    = () => { wsConnected = true;  stopPolling(); setStatus("ws", true); };
    ws.onclose   = () => { wsConnected = false; startPolling(); setStatus("ws", false); setTimeout(connectWS, WS_RETRY); };
    ws.onerror   = () => { wsConnected = false; setStatus("ws", false); };
    ws.onmessage = e => { try { handleMessage(JSON.parse(e.data)); } catch(err) {} };
  } catch(e) {
    wsConnected = false;
    startPolling();
    setTimeout(connectWS, WS_RETRY);
  }
}

function setStatus(type, ok) {
  const el = document.getElementById("connStatus");
  if (!el) return;
  el.textContent  = ok ? "LIVE" : "POLL";
  el.style.color  = ok ? "var(--green)" : "var(--yellow)";
}

// ── Polling fallback ──────────────────────────────────────────────────────────
function startPolling() {
  if (pollTimer) return;
  pollOnce();
}
function stopPolling() {
  if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; }
}
async function pollOnce() {
  try {
    const res = await fetch("/api/state");
    if (res.ok) handleMessage({ type: "state", data: await res.json() });
  } catch(e) {}
  if (!wsConnected) {
    const active = appState && appState.running;
    pollTimer = setTimeout(pollOnce, active ? POLL_ACTIVE : POLL_IDLE);
  } else {
    pollTimer = null;
  }
}

// ── Message handler ───────────────────────────────────────────────────────────
function handleMessage(msg) {
  if (msg.type === "state") {
    appState = msg.data;
    renderState(appState);
  }
}

// ── Render ────────────────────────────────────────────────────────────────────
function renderState(s) {
  const stats    = s.stats    || {};
  const mappings = s.results  || {};
  const moj      = s.moj      || {};

  // ── Stat cards ──
  setText("statInput",    Object.keys(mappings).length);
  setText("statWorking",  stats.streams_verified || 0);
  setText("statFixed",    moj.working            || 0);
  setText("statFailed",   stats.streams_failed   || 0);
  setText("statDomains",  (s.activeDomains || []).length);
  setText("statSlug",     stats.streams_found    || 0);

  // ── MOJ summary ──
  setText("mojInput",     moj.input     || 0);
  setText("mojProcessed", moj.processed || 0);
  setText("mojWorking",   moj.working   || 0);
  setText("mojFailed",    moj.failed    || 0);

  // ── Phase badge ──
  const phaseEl = document.getElementById("phaseTag");
  if (phaseEl) {
    phaseEl.textContent = (s.phase || "idle").toUpperCase();
    phaseEl.className   = "phase-tag " + (s.running ? "active" : "");
  }

  // ── Scan button ──
  const startBtn = document.getElementById("btnStart");
  const stopBtn  = document.getElementById("btnStop");
  if (startBtn) startBtn.disabled = !!s.running;
  if (stopBtn)  stopBtn.disabled  = !s.running;

  // ── Progress bar ──
  const total = s.total || 0;
  const prog  = s.progress || 0;
  const pct   = total > 0 ? Math.min(100, Math.round((prog / total) * 100)) : (s.running ? 100 : 0);
  setStyle("progressFill", "width", pct + "%");
  setText("progressPct", pct + "%");

  // ── Domains list ──
  const dl = document.getElementById("domainList");
  if (dl) {
    dl.innerHTML = "";
    (s.activeDomains || []).forEach(d => {
      const li = document.createElement("li");
      li.textContent = d;
      dl.appendChild(li);
    });
  }

  // ── Active channels table ──
  const actb = document.getElementById("activeChannelsTbody");
  if (actb) {
    actb.innerHTML = "";
    const active = Object.entries(mappings).filter(([_, m]) => m.url && m.status !== "dead");
    setText("activeChannelCount", active.length);
    active.forEach(([name, m]) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${esc(name)}</td>
        <td class="url-cell"><a href="${esc(m.url)}" target="_blank" title="${esc(m.url)}">${truncUrl(m.url)}</a></td>
        <td>${esc(m.domain || "–")}</td>
        <td><span class="status-badge status-${m.status || 'found'}">${esc(m.status || "found")}</span></td>
      `;
      actb.appendChild(tr);
    });
  }

  // ── All mappings table ──
  renderMappings(mappings);

  // ── Logs (incremental) ──
  const logLines = s.scanLog || [];
  const logEl    = document.getElementById("scanLog");
  if (logEl) {
    if (logLines.length < logCount) {
      logCount = 0;
      logEl.innerHTML = "";
    }
    if (logLines.length > logCount) {
      const frag = document.createDocumentFragment();
      for (let i = logCount; i < logLines.length; i++) {
        const div = document.createElement("div");
        div.textContent = logLines[i];
        div.className   = classifyLog(logLines[i]);
        frag.appendChild(div);
      }
      logEl.appendChild(frag);
      logEl.scrollTop = logEl.scrollHeight;
      logCount = logLines.length;
    }
  }
}

function classifyLog(line) {
  if (line.includes("error") || line.includes("failed") || line.includes("✗")) return "log-line log-err";
  if (line.includes("warn") || line.includes("⚠"))                             return "log-line log-warn";
  if (line.includes("✓") || line.includes("found") || line.includes("loaded")) return "log-line log-ok";
  return "log-line";
}

// ── Mappings Table ────────────────────────────────────────────────────────────
function renderMappings(mappings) {
  const mtb = document.getElementById("mappingsTbody");
  if (!mtb) return;
  mtb.innerHTML = "";
  const entries = Object.entries(mappings);
  setText("mappingCount", entries.length);
  if (!entries.length) {
    mtb.innerHTML = '<tr><td colspan="5" class="empty-cell">No mappings yet — run a scan to populate</td></tr>';
    return;
  }
  entries.forEach(([name, m]) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${esc(name)}</td>
      <td class="url-cell"><span title="${esc(m.url || '')}">${truncUrl(m.url)}</span></td>
      <td>${esc(m.domain || "–")}</td>
      <td><span class="status-badge status-${m.status || 'found'}">${esc(m.status || "unknown")}</span></td>
      <td>${esc(m.method || "–")}</td>
    `;
    mtb.appendChild(tr);
  });
}

// ── Controls ──────────────────────────────────────────────────────────────────
async function startScan() {
  try {
    const r = await fetch("/api/scan/start", { method: "POST" });
    const d = await r.json();
    if (!d.ok) alert("Could not start scan: " + (d.reason || "unknown error"));
  } catch(e) {
    alert("Request failed: " + e);
  }
}

async function stopScan() {
  try { await fetch("/api/scan/stop", { method: "POST" }); } catch(e) {}
}

// ── Settings modal ────────────────────────────────────────────────────────────
async function openSettings() {
  try {
    const r    = await fetch("/api/config");
    const cfg  = await r.json();
    const wrap = document.getElementById("settingsModal");
    if (!wrap) return;
    document.getElementById("cfgPlaylistUrl").value       = cfg.playlist_url        || "";
    document.getElementById("cfgMinFl").value             = cfg.min_fl              ?? 2;
    document.getElementById("cfgMaxFl").value             = cfg.max_fl              ?? 200;
    document.getElementById("cfgScanInterval").value      = cfg.scan_interval       ?? 14400;
    document.getElementById("cfgStreamTimeout").value     = cfg.stream_timeout      ?? 10;
    document.getElementById("cfgStreamConcurrency").value = cfg.stream_concurrency  ?? 15;
    document.getElementById("cfgHealthcheck").checked     = !!cfg.healthcheck_enabled;
    document.getElementById("cfgAutoCycle").checked       = !!cfg.auto_cycle;
    wrap.style.display = "flex";
  } catch(e) { alert("Failed to load config: " + e); }
}

function closeSettings() {
  const wrap = document.getElementById("settingsModal");
  if (wrap) wrap.style.display = "none";
}

async function saveSettings() {
  const cfg = {
    playlist_url:        document.getElementById("cfgPlaylistUrl").value.trim(),
    min_fl:              parseInt(document.getElementById("cfgMinFl").value)             || 2,
    max_fl:              parseInt(document.getElementById("cfgMaxFl").value)             || 200,
    scan_interval:       parseInt(document.getElementById("cfgScanInterval").value)      || 14400,
    stream_timeout:      parseInt(document.getElementById("cfgStreamTimeout").value)     || 10,
    stream_concurrency:  parseInt(document.getElementById("cfgStreamConcurrency").value) || 15,
    healthcheck_enabled: document.getElementById("cfgHealthcheck").checked,
    auto_cycle:          document.getElementById("cfgAutoCycle").checked,
  };
  try {
    await fetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(cfg),
    });
    closeSettings();
  } catch(e) { alert("Failed to save config: " + e); }
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}
function setStyle(id, prop, val) {
  const el = document.getElementById(id);
  if (el) el.style[prop] = val;
}
function esc(s) {
  if (!s) return "–";
  return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}
function truncUrl(url) {
  if (!url) return "–";
  try {
    const u = new URL(url);
    return u.hostname + (u.pathname.length > 30 ? u.pathname.slice(0,30)+"…" : u.pathname);
  } catch { return url.length > 40 ? url.slice(0,40)+"…" : url; }
}

// ── Init ──────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  connectWS();
  startPolling();

  const s = document.getElementById("btnStart");
  const x = document.getElementById("btnStop");
  const g = document.getElementById("btnSettings");
  if (s) s.addEventListener("click", startScan);
  if (x) x.addEventListener("click", stopScan);
  if (g) g.addEventListener("click", openSettings);

  const saveBtn  = document.getElementById("btnSaveSettings");
  const closeBtn = document.getElementById("btnCloseSettings");
  if (saveBtn)  saveBtn.addEventListener("click",  saveSettings);
  if (closeBtn) closeBtn.addEventListener("click", closeSettings);
});
