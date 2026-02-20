const API_URL = "/disparar";

// ===== NOVO: endpoints auxiliares =====
const STATUS_URL = (id) => `/campaign/${encodeURIComponent(id)}/status`;
const QUEUE_STATUS_URL = "/queue/status";
const QUEUE_PAUSE_URL = "/queue/pause";
const QUEUE_RESUME_URL = "/queue/resume";

const state = {
  tokens: [],
  selectedTokenId: null,
  isSending: false,
  isValidating: false,

  // ===== NOVO: campanha atual =====
  campaignId: null,
  pollTimer: null,
  lastStatus: null,
};

function uid() {
  return Math.random().toString(16).slice(2) + Date.now().toString(16);
}

function selectedToken() {
  return state.tokens.find((t) => t.id === state.selectedTokenId) || null;
}

function tokenExists(token) {
  return state.tokens.some((t) => t.token === token);
}

function setStatus(type, msg) {
  const el = document.getElementById("status");
  el.className = type;
  el.textContent = msg || "";
}

function setSending(flag) {
  state.isSending = flag;
  const btn = document.getElementById("btnEnviar");
  if (btn) {
    btn.disabled = flag;
    btn.textContent = flag ? "Enviando..." : "Enviar campanha";
  }
}

function setValidating(flag) {
  state.isValidating = flag;
  const btn = document.getElementById("btnAddToken");
  if (btn) {
    btn.disabled = flag;
    btn.textContent = flag ? "Validando token..." : "Adicionar token";
  }
}

function renderTokens() {
  const area = document.getElementById("tokensArea");
  area.innerHTML = "";

  if (state.tokens.length === 0) {
    area.innerHTML = `<span class="pill">Nenhum token adicionado</span>`;
    return;
  }

  state.tokens.forEach((t) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "tokenBtn" + (t.id === state.selectedTokenId ? " active" : "");
    btn.textContent = t.label || "Bot";
    btn.onclick = () => {
      state.selectedTokenId = t.id;
      renderTokens();
    };
    area.appendChild(btn);
  });
}

// ===== BOT INFO (GETME) =====
async function fetchBotLabel(token) {
  const res = await fetch(`https://api.telegram.org/bot${token}/getMe`);
  const data = await res.json().catch(() => null);

  if (!data || !data.ok) throw new Error(data?.description || "Token inválido (getMe).");

  const bot = data.result;
  if (bot?.username) return "@" + bot.username;
  return bot?.first_name || "Bot";
}

// ===== BOTÕES (até 4) =====
function readButtons() {
  const buttons = [];

  for (let i = 1; i <= 4; i++) {
    const textEl = document.getElementById(`b${i}Text`);
    const typeEl = document.getElementById(`b${i}Type`);
    const valueEl = document.getElementById(`b${i}Value`);
    if (!textEl || !typeEl || !valueEl) return [];

    const text = (textEl.value || "").trim();
    const type = (typeEl.value || "none").trim(); // none | url | start
    const value = (valueEl.value || "").trim();

    if (type === "none") continue;
    if (!text || !value) continue;

    buttons.push({ text, type, value });
  }

  return buttons.slice(0, 4);
}

function validateButtons(buttons) {
  for (const b of buttons) {
    if (b.type === "url") {
      if (!/^https?:\/\//i.test(b.value)) {
        return `Botão "${b.text}": URL deve começar com http:// ou https://`;
      }
    } else if (b.type === "start") {
      if (b.value.length > 64) {
        return `Botão "${b.text}": START muito longo (máx ~64 chars recomendado).`;
      }
    } else {
      return `Botão "${b.text}": tipo inválido.`;
    }
  }
  return null;
}

// ===== CSV helpers =====
function getCsvFile() {
  const el = document.getElementById("csvFile");
  return el?.files?.[0] || null;
}
function getIdColumn() {
  const el = document.getElementById("idColumn");
  const v = (el?.value || "").trim();
  return v || "chatId";
}
function validateCsvFile(csv) {
  if (!csv) return "Envie um CSV de leads.";
  const name = (csv.name || "").toLowerCase();
  if (!name.endsWith(".csv")) return "O arquivo de leads precisa ser .csv";
  if (csv.size > 10 * 1024 * 1024) return "CSV muito grande (máx 10MB).";
  return null;
}

// ===== helpers de UI =====
function clearDebug() {
  const debugEl = document.getElementById("debug");
  if (debugEl) debugEl.textContent = "";
}
function appendDebug(line) {
  const debugEl = document.getElementById("debug");
  if (!debugEl) return;
  debugEl.textContent = (debugEl.textContent || "") + line;
}

function isHttpUrl(u) {
  return /^https?:\/\//i.test(String(u || "").trim());
}

// ===== NOVO: helpers campanha/queue =====
async function fetchJson(url, opts) {
  const res = await fetch(url, opts);
  const data = await res.json().catch(() => null);
  if (!res.ok) {
    const apiMsg = data?.error || data?.message || (data ? JSON.stringify(data) : "");
    throw new Error(`HTTP ${res.status}${apiMsg ? " - " + apiMsg : ""}`);
  }
  return data;
}

function formatPct(p) {
  const pct = Math.round((Number(p || 0) * 100));
  return isFinite(pct) ? `${pct}%` : "0%";
}

function updateCampaignView(statusObj, queuePaused) {
  // Esses elementos só existem se você colar o bloco no index.html (instrução abaixo)
  const box = document.getElementById("campaignBox");
  if (!box) return;

  box.style.display = "block";

  const meta = document.getElementById("campaignMeta");
  if (meta) meta.textContent = `ID: ${statusObj.campaignId}`;

  const stTotal = document.getElementById("stTotal");
  const stSent = document.getElementById("stSent");
  const stFailed = document.getElementById("stFailed");
  const stPending = document.getElementById("stPending");
  const stProg = document.getElementById("stProg");
  const bar = document.getElementById("bar");
  const pausedInfo = document.getElementById("queuePausedInfo");

  if (stTotal) stTotal.textContent = `Total: ${statusObj.total}`;
  if (stSent) stSent.textContent = `Enviados: ${statusObj.sent}`;
  if (stFailed) stFailed.textContent = `Falhas: ${statusObj.failed}`;
  if (stPending) stPending.textContent = `Pendentes: ${statusObj.pending}`;

  const pctStr = formatPct(statusObj.progress);
  if (stProg) stProg.textContent = pctStr;
  if (bar) bar.style.width = pctStr;

  if (pausedInfo) pausedInfo.textContent = queuePaused ? "Fila está PAUSADA." : "Fila está RODANDO.";
}

async function pollCampaignOnce() {
  if (!state.campaignId) return;

  const st = await fetchJson(STATUS_URL(state.campaignId));
  let q = null;
  try {
    q = await fetchJson(QUEUE_STATUS_URL);
  } catch {
    q = { paused: null };
  }

  state.lastStatus = st;

  // Atualiza painel
  updateCampaignView(st, !!q?.paused);

  // Também escreve uma linha curta no debug (sem spam exagerado)
  appendDebug(
    `\n[status] total=${st.total} sent=${st.sent} failed=${st.failed} pending=${st.pending} progress=${formatPct(st.progress)}`
  );

  // se terminou, para polling
  if ((st.sent + st.failed) >= st.total && st.total > 0) {
    stopPolling();
    appendDebug("\n[status] campanha finalizada ✅\n");
  }
}

function startPolling(campaignId) {
  state.campaignId = campaignId;
  stopPolling(); // garante limpar timer anterior

  // dispara uma vez agora
  pollCampaignOnce().catch((e) => {
    appendDebug("\n[poll] erro: " + (e?.message || String(e)) + "\n");
  });

  // e depois a cada 1s
  state.pollTimer = setInterval(() => {
    pollCampaignOnce().catch((e) => {
      // não derruba tudo por erro temporário
      console.warn("poll error:", e);
    });
  }, 1000);
}

function stopPolling() {
  if (state.pollTimer) clearInterval(state.pollTimer);
  state.pollTimer = null;
}

async function pauseQueue() {
  await fetchJson(QUEUE_PAUSE_URL, { method: "POST" });
  await pollCampaignOnce();
}

async function resumeQueue() {
  await fetchJson(QUEUE_RESUME_URL, { method: "POST" });
  await pollCampaignOnce();
}

// ===== EVENTOS =====
document.getElementById("btnAddToken").addEventListener("click", async () => {
  const token = document.getElementById("tokenInput").value.trim();

  if (!token) return setStatus("err", "Cole um token.");
  if (!token.includes(":") || token.length < 30) return setStatus("err", "Token inválido.");
  if (tokenExists(token)) return setStatus("err", "Token já adicionado.");

  setValidating(true);
  setStatus("", "Validando token e buscando nome do bot...");

  let label = "Bot (sem nome)";
  try {
    label = await fetchBotLabel(token);
    setStatus("ok", "Bot conectado: " + label);
  } catch {
    setStatus("warn", "Token adicionado, mas não consegui buscar o nome do bot (rede/Telegram bloqueado).");
  }

  const id = uid();
  state.tokens.push({ id, label, token });
  state.selectedTokenId = id;

  document.getElementById("tokenInput").value = "";
  renderTokens();
  setValidating(false);
});

document.getElementById("btnRemoveToken").addEventListener("click", () => {
  if (!state.selectedTokenId) return setStatus("err", "Nenhum token selecionado.");

  state.tokens = state.tokens.filter((t) => t.id !== state.selectedTokenId);
  state.selectedTokenId = state.tokens[0]?.id || null;

  setStatus("ok", "Token removido.");
  renderTokens();
});

document.getElementById("btnEnviar").addEventListener("click", async () => {
  if (state.isSending) return;

  const tipo = document.getElementById("tipo").value;
  const mensagemTemplate = document.getElementById("mensagem").value || "";

  const limMax = Number(document.getElementById("limitMax").value || 1);
  const limMs = Number(document.getElementById("limitMs").value || 1100);

  const file = document.getElementById("arquivo")?.files?.[0] || null;
  const fileUrl = (document.getElementById("fileUrl")?.value || "").trim();

  const csv = getCsvFile();
  const idColumn = getIdColumn();
  const tokenObj = selectedToken();

  clearDebug();

  if (!tokenObj) return setStatus("err", "Adicione um token.");

  const csvErr = validateCsvFile(csv);
  if (csvErr) return setStatus("err", csvErr);

  if (!idColumn.trim()) return setStatus("err", 'Preencha "Coluna do ID" (ex: chatId).');

  if (tipo === "text" && !mensagemTemplate.trim()) {
    return setStatus("err", "Mensagem obrigatória para texto.");
  }

  if (!(limMax >= 1) || !(limMs >= 200)) {
    return setStatus("err", "Limite inválido. Use max >= 1 e intervalo >= 200ms.");
  }

  // mídia: exige URL ou upload (e não deixa os dois)
  if (tipo !== "text") {
    const hasUrl = !!fileUrl;
    const hasUpload = !!file;

    if (hasUrl && !isHttpUrl(fileUrl)) {
      return setStatus("err", "A URL do arquivo deve começar com http:// ou https://");
    }

    if (hasUrl && hasUpload) {
      return setStatus("err", "Escolha apenas UM: URL do arquivo OU Upload.");
    }

    if (!hasUrl && !hasUpload) {
      return setStatus("err", "Para mídia/documento: preencha a URL do arquivo OU selecione um arquivo (upload).");
    }
  }

  const buttons = readButtons();
  const btnErr = validateButtons(buttons);
  if (btnErr) return setStatus("err", btnErr);

  const form = new FormData();
  form.append("botToken", tokenObj.token);
  form.append("type", tipo);
  form.append("caption", mensagemTemplate);
  form.append("csv", csv);
  form.append("idColumn", idColumn);
  form.append("limitMax", String(limMax));
  form.append("limitMs", String(limMs));
  form.append("buttons", JSON.stringify(buttons));

  // Se tiver URL, manda URL e NÃO manda upload (URL tem prioridade)
  if (tipo !== "text") {
    if (fileUrl) form.append("fileUrl", fileUrl);
    else if (file) form.append("file", file);
  }

  setSending(true);
  setStatus("", "Enfileirando...");

  appendDebug(
    "POST " + API_URL + "\n" +
      "Bot: " + (tokenObj.label || "Bot") + "\n" +
      "CSV: " + csv.name + "\n" +
      "Coluna ID: " + idColumn + "\n" +
      "Tipo: " + tipo + "\n" +
      "Botões: " + (buttons.length || 0) + "\n" +
      "Rate: " + limMax + " a cada " + limMs + "ms\n" +
      (tipo !== "text"
        ? (fileUrl ? "URL: " + fileUrl + "\n" : (file ? "Upload: " + file.name + "\n" : ""))
        : "")
  );

  try {
    const res = await fetch(API_URL, { method: "POST", body: form });
    const data = await res.json().catch(() => null);

    if (!res.ok) {
      const apiMsg = data?.error || data?.message || (data ? JSON.stringify(data) : "");
      throw new Error(`HTTP ${res.status}${apiMsg ? " - " + apiMsg : ""}`);
    }

    setStatus(
      "ok",
      "Campanha enfileirada. Total: " +
        (data?.total ?? "?") +
        (data?.unique ? ` (únicos: ${data.unique})` : "")
    );

    if (data?.campaignId) {
      appendDebug("\n\ncampaignId: " + data.campaignId + "\n");
      startPolling(data.campaignId);
    } else {
      appendDebug("\n\n⚠️ Sem campaignId na resposta (não vou monitorar status).\n");
    }

    appendDebug("\n\nResposta:\n" + JSON.stringify(data, null, 2));
  } catch (err) {
    setStatus("err", "Erro ao enviar para API: " + (err?.message || String(err)));
    appendDebug("\n\nErro:\n" + (err?.message || String(err)));
  } finally {
    setSending(false);
  }
});

renderTokens();

// ===== NOVO: liga botões pause/resume se existirem no HTML =====
const btnPause = document.getElementById("btnPause");
const btnResume = document.getElementById("btnResume");
if (btnPause) btnPause.addEventListener("click", () => pauseQueue().catch((e) => setStatus("err", e.message)));
if (btnResume) btnResume.addEventListener("click", () => resumeQueue().catch((e) => setStatus("err", e.message)));
