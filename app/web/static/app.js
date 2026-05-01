// ── Config ────────────────────────────────────────────────────────────────────
const POLL_IDLE   = 3000;
const POLL_ACTIVE = 800;
const WS_RETRY    = 3000;

// ── State ─────────────────────────────────────────────────────────────────────
let ws          = null;
let wsConnected = false;
let pollTimer   = null;
let logCount    = 0;
let outputReady = false;
let modal       = null;

let state = null;

// ── WebSocket ─────────────────────────────────────────────────────────────────
function connectWS() {
  try {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(`${proto}//${location.host}/ws`);
    ws.onopen  = () => { wsConnected = true; stopPolling(); };
    ws.onclose = () => { wsConnected = false; startPolling(); setTimeout(connectWS, WS_RETRY); };
    ws.onerror = () => { wsConnected = false; };
    ws.onmessage = e => { try { handleMessage(JSON.parse(e.data)); } catch(err) {} };
  } catch(e) {
    wsConnected = false;
    startPolling();
    setTimeout(connectWS, WS_RETRY);
  }
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
    const active = state && state.running;
    pollTimer = setTimeout(pollOnce, active ? POLL_ACTIVE : POLL_IDLE);
  } else {
    pollTimer = null;
  }
}

// ── Message handler ───────────────────────────────────────────────────────────
function handleMessage(msg) {
  if (msg.type === "state") {
    state = msg.data;
    renderState(state);
  }
}

// ── Render ────────────────────────────────────────────────────────────────────
function renderState(s) {

  const stats = s.stats || {};
  const mappings = s.results || {};

  // ── Stats FIXED ──
  document.getElementById("statInput").textContent   = Object.keys(mappings).length || 0;
  document.getElementById("statWorking").textContent = stats.streams_verified || 0;
  document.getElementById("statFixed").textContent   = stats.streams_found || 0;
  document.getElementById("statFailed").textContent  = stats.streams_failed || 0;
  document.getElementById("statDomains").textContent = (s.activeDomains || []).length;
  document.getElementById("statSlug").textContent    = stats.streams_found || 0;

  // ── Progress ──
  if (s.total > 0) {
    const pct = Math.round((s.progress / s.total) * 100);
    document.getElementById("progressFill").style.width = pct + "%";
    document.getElementById("progressPct").textContent  = pct + "%";
  }

  // ── Domains ──
  const dl = document.getElementById("domainList");
  dl.innerHTML = "";

  (s.activeDomains || []).forEach(d => {
    const li = document.createElement("li");
    li.textContent = d;
    dl.appendChild(li);
  });

  // ── Active Channels FIXED ──
  const actb = document.getElementById("activeChannelsTbody");
  actb.innerHTML = "";

  const active = Object.entries(mappings).filter(([_, m]) => m.url);

  document.getElementById("activeChannelCount").textContent = active.length;

  active.forEach(([name, m]) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${name}</td>
      <td>${m.url}</td>
      <td>${m.domain || "–"}</td>
    `;
    actb.appendChild(tr);
  });

  // ── Mapping Table FIXED ──
  renderMappings(mappings);

  // ── Logs ──
  const logLines = s.scanLog || [];
  const logEl = document.getElementById("scanLog");

  if (logLines.length < logCount) {
    logCount = 0;
    logEl.innerHTML = "";
  }

  if (logLines.length > logCount) {
    for (let i = logCount; i < logLines.length; i++) {
      const div = document.createElement("div");
      div.textContent = logLines[i];
      logEl.appendChild(div);
    }
    logCount = logLines.length;
  }
}

// ── Mapping Table ─────────────────────────────────────────────────────────────
function renderMappings(mappings) {
  const mtb = document.getElementById("mappingsTbody");
  mtb.innerHTML = "";

  const entries = Object.entries(mappings);

  document.getElementById("mappingCount").textContent = entries.length;

  if (!entries.length) {
    mtb.innerHTML = '<tr><td colspan="5">No mappings yet</td></tr>';
    return;
  }

  entries.forEach(([name, m]) => {
    const tr = document.createElement("tr");

    tr.innerHTML = `
      <td>${name}</td>
      <td>${m.url || "–"}</td>
      <td>${m.domain || "–"}</td>
      <td>${m.status || "unknown"}</td>
      <td>${m.method || "–"}</td>
    `;

    mtb.appendChild(tr);
  });
}

// ── Init ──────────────────────────────────────────────────────────────────────
connectWS();
startPolling();