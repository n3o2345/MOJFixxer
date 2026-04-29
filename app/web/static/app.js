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
let state = null;

function handleMessage(msg) {
  switch (msg.type) {
    case "state":
      state = msg.data;
      renderState(msg.data);
      break;
    case "log":
      // handled via state.scanLog in renderState
      break;
    case "cycle_started":
      outputReady = false;
      showDownload(false);
      logCount = 0;
      document.getElementById("scanLog").innerHTML = "";
      break;
    case "cycle_complete":
      if (outputReady) showDownload(true);
      toast("Cycle complete", "success");
      break;
  }
}

// ── Phase labels ──────────────────────────────────────────────────────────────
const PHASE_LABELS = {
  idle:       "Idle",
  domains:    "Scanning Domains",
  importing:  "Importing Playlist",
  processing: "Processing Channels",
  complete:   "Complete",
  error:      "Error",
};
const PHASE_BADGE = {
  idle:       "badge-idle",
  domains:    "badge-active",
  importing:  "badge-active",
  processing: "badge-active",
  complete:   "badge-success",
  error:      "badge-error",
};

// ── Render ────────────────────────────────────────────────────────────────────
function renderState(s) {
  // Badge
  const badge = document.getElementById("statusBadge");
  badge.textContent = PHASE_LABELS[s.phase] || "Idle";
  badge.className   = "badge " + (PHASE_BADGE[s.phase] || "badge-idle");

  // Phase detail row
  const phaseRow = document.getElementById("phaseRow");
  if (s.running && s.phaseDetail) {
    phaseRow.style.display = "flex";
    document.getElementById("phaseLabel").textContent = s.phaseDetail;
  } else {
    phaseRow.style.display = "none";
  }

  // Countdown & last cycle
  document.getElementById("countdown").textContent = s.nextCycleIn || "–";
  document.getElementById("lastScan").textContent  = s.lastCycleTime || "Never";

  // Stats
  const st = s.stats || {};
  document.getElementById("statInput").textContent   = s.inputCount  || 0;
  document.getElementById("statWorking").textContent = st.working    || 0;
  document.getElementById("statFixed").textContent   = st.fixed      || 0;
  document.getElementById("statFailed").textContent  = st.failed     || 0;
  document.getElementById("statDomains").textContent = (s.activeDomains || []).length;
  document.getElementById("statSlug").textContent    = st.slugFound  || 0;

  // Progress bar
  const pWrap = document.getElementById("progressWrap");
  if (s.running && s.total > 0 && s.phase === "processing") {
    pWrap.style.display = "block";
    const pct = Math.round((s.progress / s.total) * 100);
    document.getElementById("progressFill").style.width    = pct + "%";
    document.getElementById("progressPct").textContent     = pct + "%";
    document.getElementById("progressChannel").textContent = s.currentChannel || "";
  } else {
    pWrap.style.display = "none";
  }

  // Phase text in log header
  document.getElementById("phaseText").textContent = PHASE_LABELS[s.phase] || "–";

  // Scanner dot
  document.getElementById("scanDot").className = s.running ? "dot dot-on" : "dot dot-off";

  // Current channel in log bar
  document.getElementById("scanCurrentChannel").textContent =
    (s.phase === "processing" && s.currentChannel) ? s.currentChannel : "–";

  // Scan progress fill (domain phase)
  if (s.phase === "domains") {
    const domains = s.activeDomains || [];
    const maxFl = (s.config && s.config.max_fl) || 200;
    document.getElementById("scanFill").style.width = (domains.length / maxFl * 100) + "%";
  } else if (s.phase === "processing" && s.total > 0) {
    document.getElementById("scanFill").style.width = (s.progress / s.total * 100) + "%";
  } else if (s.phase === "complete") {
    document.getElementById("scanFill").style.width = "100%";
  } else if (!s.running) {
    document.getElementById("scanFill").style.width = "0%";
  }

  // Run / stop buttons
  document.getElementById("runBtn").disabled  = s.running;
  document.getElementById("stopBtn").style.display = s.running ? "inline-flex" : "none";

  // Domain list
  const dl = document.getElementById("domainList");
  const domains = s.activeDomains || [];
  const schemes = s.domainSchemes || {};
  document.getElementById("domainBadge").textContent = domains.length;
  dl.innerHTML = "";
  if (!domains.length) {
    dl.innerHTML = '<li class="muted-li">No domains found yet</li>';
  } else {
    domains.forEach(d => {
      const li = document.createElement("li");
      const sc = schemes[d];
      const badge = sc === "https"
        ? '<span class="scheme-badge scheme-https">HTTPS 🔒</span>'
        : sc === "http"
          ? '<span class="scheme-badge scheme-http">HTTP 🔓</span>'
          : '';
      li.innerHTML = `<span>${esc(d)}</span>${badge}`;
      dl.appendChild(li);
    });
  }

  // Active Channels
  const actb = document.getElementById("activeChannelsTbody");
  const mappings = s.channelMappings || {};
  const active = Object.entries(mappings).filter(([,m]) => m.workingUrl);
  document.getElementById("activeChannelCount").textContent = active.length;
  actb.innerHTML = "";
  if (!active.length) {
    actb.innerHTML = '<tr><td colspan="3" class="muted">No active channels yet</td></tr>';
  } else {
    active.forEach(([name, m]) => {
      const scheme = m.scheme || (m.workingUrl.startsWith("https") ? "https" : "http");
      const proto = scheme === "https"
        ? '<span class="scheme-badge scheme-https">HTTPS 🔒</span>'
        : '<span class="scheme-badge scheme-http">HTTP 🔓</span>';
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><strong>${esc(name)}</strong></td>
        <td>${proto}</td>
        <td class="mono" style="font-size:.75rem;color:var(--muted)">${esc(m.workingDomain||"–")}</td>`;
      actb.appendChild(tr);
    });
  }

  // Cycle log
  const logLines = s.scanLog || [];
  if (logLines.length < logCount) {
    logCount = 0;
    document.getElementById("scanLog").innerHTML = "";
  }
  if (logLines.length > logCount) {
    for (let i = logCount; i < logLines.length; i++) appendLog(logLines[i]);
    logCount = logLines.length;
  }

  // Channel mappings table
  renderMappings(mappings);

  // Download button
  if (s.phase === "complete") {
    outputReady = true;
    showDownload(true);
  }
  if (s.running) showDownload(false);

  // Polling speed
  if (!wsConnected && pollTimer) {
    clearTimeout(pollTimer);
    pollTimer = setTimeout(pollOnce, s.running ? POLL_ACTIVE : POLL_IDLE);
  }

  // Reflect URL import in modal if open
  if (document.getElementById("settingsModal").style.display !== "none") {
    renderUrlImport(s.urlImport);
  }
}

function renderMappings(mappings) {
  const mtb = document.getElementById("mappingsTbody");
  const entries = Object.entries(mappings);
  document.getElementById("mappingCount").textContent = entries.length;
  mtb.innerHTML = "";
  if (!entries.length) {
    mtb.innerHTML = '<tr><td colspan="7" class="muted">No mappings yet — waiting for first cycle</td></tr>';
    return;
  }

  function schemeChip(status) {
    if (status === "working") return '<span class="chip chip-scheme chip-scheme-ok">\u2714</span>';
    if (status === "failed")  return '<span class="chip chip-scheme chip-scheme-fail">\u2718</span>';
    return '<span class="chip chip-scheme chip-scheme-untested">\u2013</span>';
  }

  function sourceBadge(m) {
    if (m.status === "failed") return '<span class="src-badge src-failed">Failed</span>';
    if (m.status === "fixed")  return '<span class="src-badge src-fixed">Fixed</span>';
    const noTests = !m.testedDomains || m.testedDomains.length === 0;
    const sameUrl = m.originalUrl && m.originalUrl === m.workingUrl;
    if (noTests && sameUrl) return '<span class="src-badge src-keyword">\u26a1 Found</span>';
    return '<span class="src-badge src-working">Working</span>';
  }

  const isNewlyFound = m => m.status === "working" &&
    (!m.testedDomains || !m.testedDomains.length) &&
    m.originalUrl === m.workingUrl;
  const isFixed   = m => m.status === "fixed";
  const isWorking = m => m.status === "working" && !isNewlyFound(m);
  const isFailed  = m => m.status === "failed";

  const sections = [
    { label: "\u26a1 Newly Found", cls: "hdr-found",   filter: isNewlyFound },
    { label: "\uD83D\uDD27 Fixed",  cls: "hdr-fixed",   filter: isFixed },
    { label: "\u2705 Working",      cls: "hdr-working", filter: isWorking },
    { label: "\u2717 Failed",       cls: "hdr-failed",  filter: isFailed },
  ];

  sections.forEach(({ label, cls, filter }) => {
    const group = entries.filter(([, m]) => filter(m))
                         .sort(([a], [b]) => a.localeCompare(b));
    if (!group.length) return;

    const hdr = document.createElement("tr");
    hdr.className = "mapping-section-hdr " + cls;
    hdr.innerHTML = `<td colspan="7">${label} <span class="section-count">${group.length}</span></td>`;
    mtb.appendChild(hdr);

    group.forEach(([name, m]) => {
      const urlHtml = m.workingUrl
        ? `<a href="${esc(m.workingUrl)}" target="_blank" class="mono mapping-url">${esc(m.workingUrl)}</a>`
        : '<span class="muted">\u2013</span>';
      const time = m.lastChecked ? new Date(m.lastChecked).toLocaleTimeString() : "\u2013";
      const tr = document.createElement("tr");
      if (isFailed(m))     tr.className = "row-failed";
      if (isNewlyFound(m)) tr.className = "row-found";
      tr.innerHTML = `
        <td><strong>${esc(name)}</strong></td>
        <td>${sourceBadge(m)}</td>
        <td>${schemeChip(m.httpsStatus)}</td>
        <td>${schemeChip(m.httpStatus)}</td>
        <td class="mono mapping-domain">${esc(m.workingDomain || "\u2013")}</td>
        <td>${urlHtml}</td>
        <td class="muted mapping-time">${time}</td>`;
      mtb.appendChild(tr);
    });
  });
}

// ── Log ───────────────────────────────────────────────────────────────────────
function appendLog(entry) {
  const box = document.getElementById("scanLog");
  const msg = entry.msg || "";
  let cls = "log-msg";
  if (msg.includes("✓") || msg.includes("Fixed") || msg.includes("Complete") || msg.includes("working")) cls = "log-ok";
  else if (msg.includes("ERROR") || msg.includes("Failed") || msg.includes("error"))  cls = "log-err";
  else if (msg.includes("━") || msg.includes("Step"))   cls = "log-info";
  else if (msg.includes("Stop") || msg.includes("WARNING")) cls = "log-warn";

  const div = document.createElement("div");
  div.className = "log-line";
  div.innerHTML = `<span class="log-ts">${entry.ts||""}</span><span class="${cls}">${esc(msg)}</span>`;
  box.appendChild(div);
  box.scrollTop = box.scrollHeight;
}

// ── UI helpers ────────────────────────────────────────────────────────────────
function showDownload(show) {
  document.getElementById("downloadBtn").style.display   = show ? "inline-flex" : "none";
  document.getElementById("hostedUrlWrap").style.display = show ? "flex" : "none";
  if (show) document.getElementById("hostedUrl").textContent = `${location.protocol}//${location.host}/playlist`;
}

function copyHostedUrl() {
  const url = `${location.protocol}//${location.host}/playlist`;
  navigator.clipboard.writeText(url)
    .then(() => toast("URL copied!", "success"))
    .catch(() => {
      const el = document.createElement("textarea");
      el.value = url; document.body.appendChild(el); el.select();
      document.execCommand("copy"); document.body.removeChild(el);
      toast("URL copied!", "success");
    });
}

function esc(s) {
  return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

function toast(msg, type="info", dur=4000) {
  const icons = { success:"✅", error:"❌", info:"ℹ️", warn:"⚠️" };
  const el = document.createElement("div");
  el.className = `toast toast-${type}`;
  el.innerHTML = `<span>${icons[type]||""}</span><span>${esc(msg)}</span>`;
  document.getElementById("toastContainer").appendChild(el);
  setTimeout(() => { el.style.opacity="0"; el.style.transition="opacity .3s"; setTimeout(()=>el.remove(),300); }, dur);
}

async function openSettings() {
  // Reset tabs
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
  document.querySelectorAll(".tab-pane").forEach(p => p.classList.remove("active"));
  document.querySelector('[data-tab="general"]').classList.add("active");
  document.getElementById("tab-general").classList.add("active");

  try {
    const [cfgRes, kwRes] = await Promise.all([fetch("/api/config"), fetch("/api/keywords")]);
    const cfg = await cfgRes.json();
    const kw  = await kwRes.json();

    document.getElementById("s_min_fl").value            = cfg.min_fl             ?? 2;
    document.getElementById("s_max_fl").value            = cfg.max_fl             ?? 200;
    document.getElementById("s_stream_timeout").value    = cfg.stream_timeout     ?? 12;
    document.getElementById("s_max_retries").value       = cfg.max_retries        ?? 3;
    document.getElementById("s_scan_interval").value     = cfg.scan_interval      ?? 14400;
    document.getElementById("s_custom_output_name").value= cfg.custom_output_name ?? "output.m3u8";
    document.getElementById("s_auto_cycle").checked      = cfg.auto_cycle !== false;
    document.getElementById("s_playlist_url").value      = cfg.playlist_url       ?? "";
    document.getElementById("s_keywords").value          = (kw.keywords || []).join("\n");
    updateKwCount();
  } catch(e) { toast("Could not load settings", "error"); }

  document.getElementById("importStatus").style.display = "none";
  modal.style.display = "flex";
}

function updateKwCount() {
  const n = document.getElementById("s_keywords").value.split("\n").filter(l=>l.trim()).length;
  document.getElementById("kwCount").textContent = n ? `${n} channel${n!==1?"s":""} loaded` : "";
}

function setImportStatus(status, icon, msg) {
  const colors = { fetching:"var(--accent)", merging:"var(--yellow)", done:"var(--green)", error:"var(--red)" };
  const box = document.getElementById("importStatus");
  box.style.display = "block";
  box.style.borderColor = colors[status] || "var(--border)";
  box.style.color       = colors[status] || "var(--text)";
  document.getElementById("importStatusIcon").textContent = icon + " ";
  document.getElementById("importStatusMsg").textContent  = msg;
}

function renderUrlImport(ui) {
  if (!ui || ui.status === "idle") return;
  const icons = { fetching:"⏳", merging:"🔀", done:"✅", error:"❌" };
  setImportStatus(ui.status, icons[ui.status]||"•", ui.message||"");
}

// ── Init ──────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  modal = document.getElementById("settingsModal");

  document.getElementById("settingsBtn").onclick       = openSettings;
  document.getElementById("modalClose").onclick        = () => modal.style.display = "none";
  document.getElementById("cancelSettingsBtn").onclick = () => modal.style.display = "none";
  modal.onclick = e => { if (e.target === modal) modal.style.display = "none"; };

  document.getElementById("runBtn").onclick = async () => {
    try {
      const res = await fetch("/api/run", { method: "POST" });
      const d = await res.json();
      if (!res.ok) toast(d.detail || "Could not start cycle", "error");
      else { toast("Cycle started", "info"); if (!wsConnected) startPolling(); }
    } catch(e) { toast("Network error: " + e.message, "error"); }
  };

  document.getElementById("stopBtn").onclick = async () => {
    try {
      await fetch("/api/stop", { method: "POST" });
      toast("Stop requested", "info");
    } catch(e) { toast("Could not stop", "error"); }
  };

  document.getElementById("downloadBtn").onclick = () => { window.location.href = "/api/output"; };

  document.getElementById("testImportBtn").onclick = async () => {
    const url = document.getElementById("s_playlist_url").value.trim();
    if (!url) { toast("Enter a playlist URL first", "warn"); return; }
    const btn = document.getElementById("testImportBtn");
    btn.disabled = true;
    btn.textContent = "⏳ Importing...";
    setImportStatus("fetching", "⏳", "Fetching playlist...");
    try {
      const res  = await fetch("/api/import-url", {
        method: "POST", headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ url }),
      });
      const data = await res.json();
      if (!res.ok) {
        setImportStatus("error", "❌", data.detail || "Import failed");
        toast(data.detail || "Import failed", "error");
      } else {
        setImportStatus("done", "✅", data.message || "Import complete");
        toast(data.message || "Import complete", "success");
      }
    } catch(e) {
      setImportStatus("error", "❌", "Network error: " + e.message);
    } finally {
      btn.disabled = false;
      btn.textContent = "⬇ Test Import Now";
    }
  };

  document.getElementById("s_keywords").addEventListener("input", updateKwCount);

  document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.onclick = () => {
      document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
      document.querySelectorAll(".tab-pane").forEach(p => p.classList.remove("active"));
      btn.classList.add("active");
      document.getElementById("tab-" + btn.dataset.tab).classList.add("active");
    };
  });

  document.getElementById("saveSettingsBtn").onclick = async () => {
    const cfg = {
      min_fl:             parseInt(document.getElementById("s_min_fl").value),
      max_fl:             parseInt(document.getElementById("s_max_fl").value),
      stream_timeout:     parseInt(document.getElementById("s_stream_timeout").value),
      max_retries:        parseInt(document.getElementById("s_max_retries").value),
      scan_interval:      parseInt(document.getElementById("s_scan_interval").value),
      custom_output_name: document.getElementById("s_custom_output_name").value.trim(),
      auto_cycle:         document.getElementById("s_auto_cycle").checked,
      playlist_url:       document.getElementById("s_playlist_url").value.trim(),
    };
    const keywords = document.getElementById("s_keywords").value
      .split("\n").map(k=>k.trim()).filter(Boolean);
    try {
      const results = await Promise.all([
        fetch("/api/settings", { method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(cfg) }),
        fetch("/api/keywords", { method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({keywords}) }),
      ]);
      modal.style.display = "none";
      toast(results.every(r=>r.ok) ? "Settings saved" : "Saved with errors",
            results.every(r=>r.ok) ? "success" : "warn");
    } catch(e) { toast("Save error: " + e.message, "error"); }
  };

  startPolling();
  connectWS();
});
