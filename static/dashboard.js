const elements = {
  status: document.getElementById("system-status"),
  statusText: document.getElementById("system-status-text"),
  sectionState: document.getElementById("section-state"),
  modelChip: document.getElementById("model-chip"),
  liveIndicator: document.getElementById("live-indicator"),
  liveLabel: document.getElementById("live-label"),
  transcript: document.getElementById("transcript-feed"),
  callTitle: document.getElementById("call-title"),
  timer: document.getElementById("call-timer"),
  callSid: document.getElementById("call-sid"),
  callModel: document.getElementById("call-model"),
  interruptions: document.getElementById("interruptions"),
  audioMeter: document.getElementById("audio-meter"),
  toolList: document.getElementById("tool-list"),
  toolCount: document.getElementById("tool-count"),
  architecture: document.getElementById("architecture"),
};

let dashboardState = null;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function compactJson(value) {
  const text = JSON.stringify(value ?? {});
  return text.length > 92 ? `${text.slice(0, 89)}…` : text;
}

function formatDuration(state) {
  const call = state?.call;
  if (!call?.started_at) return "00:00";
  const start = Date.parse(call.started_at);
  const end = call.ended_at ? Date.parse(call.ended_at) : Date.now();
  const totalSeconds = Math.max(0, Math.floor((end - start) / 1000));
  const minutes = String(Math.floor(totalSeconds / 60)).padStart(2, "0");
  const seconds = String(totalSeconds % 60).padStart(2, "0");
  return `${minutes}:${seconds}`;
}

function transcriptMarkup(state) {
  const turns = [...(state.transcript ?? [])];
  for (const [role, text] of Object.entries(state.partial_transcript ?? {})) {
    if (text) turns.push({ role, text, partial: true });
  }

  if (!turns.length) {
    return `
      <div class="empty-state" id="transcript-empty">
        <div class="empty-wave" aria-hidden="true"><i></i><i></i><i></i><i></i><i></i></div>
        <strong>Waiting for a conversation</strong>
        <span>Call your connected Twilio number to start the stream.</span>
      </div>`;
  }

  return turns
    .map((turn) => {
      const role = turn.role === "assistant" ? "assistant" : "user";
      const label = role === "assistant" ? "Gemini" : "Caller";
      return `
        <article class="transcript-turn" data-role="${role}" data-partial="${Boolean(turn.partial)}">
          <span class="turn-role">${label}</span>
          <p class="turn-copy">${escapeHtml(turn.text)}</p>
        </article>`;
    })
    .join("");
}

function toolsMarkup(tools) {
  if (!tools.length) return '<p class="tool-empty">Function calls will appear here.</p>';
  return [...tools]
    .reverse()
    .map((tool) => {
      const result =
        tool.status === "complete"
          ? `<p class="tool-result">OUT · ${escapeHtml(compactJson(tool.result))}</p>`
          : "";
      return `
        <article class="tool-event" data-status="${escapeHtml(tool.status)}">
          <header>
            <strong>${escapeHtml(tool.name)}</strong>
            <time>${tool.status === "running" ? "RUNNING" : `${tool.duration_ms ?? 0} MS`}</time>
          </header>
          <p>IN · ${escapeHtml(compactJson(tool.arguments))}</p>
          ${result}
        </article>`;
    })
    .join("");
}

function render(state) {
  dashboardState = state;
  const active = state.status === "live";
  const connecting = state.status === "connecting";
  const call = state.call;
  const tools = state.tools ?? [];
  const wasNearBottom =
    elements.transcript.scrollHeight - elements.transcript.scrollTop - elements.transcript.clientHeight <
    70;

  elements.status.dataset.tone = active ? "live" : "ready";
  elements.statusText.textContent = active
    ? "Call streaming"
    : connecting
      ? "Connecting call"
      : "Dashboard ready";
  elements.sectionState.textContent = active
    ? "STREAMING FROM SERVER"
    : connecting
      ? "CONNECTING TO GEMINI"
      : state.status === "ended"
        ? "CALL COMPLETE"
        : "WAITING FOR A CALL";
  elements.liveIndicator.dataset.live = String(active);
  elements.liveLabel.textContent = active ? "Live" : state.status === "ended" ? "Ended" : "Idle";
  elements.audioMeter.dataset.live = String(active);
  elements.architecture.dataset.status = state.status;
  elements.callTitle.textContent = active
    ? "Voice session live"
    : connecting
      ? "Connecting session"
      : state.status === "ended"
        ? "Call complete"
        : "No active call";
  elements.callSid.textContent = call?.call_sid ?? "—";
  elements.callModel.textContent = call?.model ?? "—";
  elements.interruptions.textContent = String(state.interruptions ?? 0);
  elements.modelChip.textContent = call?.model ?? "gemini-3.8-live";
  elements.timer.textContent = formatDuration(state);
  elements.transcript.innerHTML = transcriptMarkup(state);
  elements.toolList.innerHTML = toolsMarkup(tools);
  elements.toolCount.textContent = `${tools.length} ${tools.length === 1 ? "call" : "calls"}`;

  if (wasNearBottom) elements.transcript.scrollTop = elements.transcript.scrollHeight;
}

async function loadInitialState() {
  try {
    const response = await fetch("/api/state", { headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error(`State request failed: ${response.status}`);
    render(await response.json());
  } catch {
    elements.status.dataset.tone = "error";
    elements.statusText.textContent = "Dashboard unavailable";
  }
}

function connectEventStream() {
  const source = new EventSource("/api/events");
  source.addEventListener("state", (event) => render(JSON.parse(event.data)));
  source.addEventListener("open", () => {
    if (!dashboardState || dashboardState.status !== "live") {
      elements.status.dataset.tone = "ready";
      elements.statusText.textContent = "Dashboard ready";
    }
  });
  source.addEventListener("error", () => {
    elements.status.dataset.tone = "error";
    elements.statusText.textContent = "Reconnecting";
  });
}

setInterval(() => {
  if (dashboardState) elements.timer.textContent = formatDuration(dashboardState);
}, 1000);

loadInitialState();
connectEventStream();
