const API_URL = "/disparar";

// ===== Campaign API =====
function campaignStatusUrl(id) {
  return `/campaign/${encodeURIComponent(id)}`;
}
function campaignPauseUrl(id) {
  return `/campaign/${encodeURIComponent(id)}/pause`;
}
function campaignResumeUrl(id) {
  return `/campaign/${encodeURIComponent(id)}/resume`;
}
function campaignCancelUrl(id) {
  return `/campaign/${encodeURIComponent(id)}/cancel`;
}

// ===== Persist (last campaign) =====
const LAST_CAMPAIGN_KEY = "lastCampaignId_v1";

function saveLastCampaignId(id) {
  try {
    if (id) localStorage.setItem(LAST_CAMPAIGN_KEY, String(id));
  } catch {}
}

function loadLastCampaignId() {
  try {
    const v = localStorage.getItem(LAST_CAMPAIGN_KEY);
    return v ? String(v) : null;
  } catch {
    return null;
  }
}

function clearLastCampaignId() {
  try {
    localStorage.removeItem(LAST_CAMPAIGN_KEY);
  } catch {}
}

const ALLOWED_INTERVALS = new Set([1000, 2000, 3000]);
const MAX_RATE_MAX = 25;

const state = {
  tokens: [],
  selectedTokenId: null,
  isSending: false,
  isValidating: false,

  // campaign
  campaignId: null,
  campaignPollTimer: null,
  lastCampaign: null,
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
  if (!el) return;
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
  if (!area) return;
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

// ✅ file_id do Telegram: não é URL. Pode conter letras/números/underscore/hífen.
// Ex comuns: "BAACAgQAAxkBA..." / "AgACAgQAAxkBA..." / "CQACAgQAA..." etc.
function isTelegramFileId(s) {
  const v = String(s || "").trim();
  if (!v) return false;
  if (isHttpUrl(v)) return false;
  // evita coisas muito curtas ou com espaços
  if (v.length < 10) return false;
  if (/\s/.test(v)) return false;
  // geralmente começa com letras e tem só chars "seguros"
  if (!/^[A-Za-z0-9_-]+$/.test(v)) return false;
  return true;
}

// ===== Campaign UI helpers =====
function setCampaignVisible(flag) {
  const box = document.getElementById("campaignBox");
  if (!box) return;
  box.style.display = flag ? "block" : "none";
}

function stopCampaignPolling() {
  if (state.campaignPollTimer) {
    clearInterval(state.campaignPollTimer);
    state.campaignPollTimer = null;
  }
}

async function fetchCampaignOnce(campaignId) {
  const res = await fetch(campaignStatusUrl(campaignId));
  const data = await res.json().catch(() => null);

  if (!res.ok) {
    const msg = data?.error || `HTTP ${res.status}`;
    if (res.status === 404) {
      clearLastCampaignId();
    }
    throw new Error(msg);
  }

  if (data?.campaign) return data.campaign;

  const meta = data || {};
  const total = Number(meta.total || 0);
  const sent = Number(meta.sent || 0);
  const failed = Number(meta.failed || 0);

  const canceledCount = Number(meta.canceledCount || meta.canceled_total || 0);

  const done = sent + failed + canceledCount;
  const pct = total > 0 ? Math.floor((done / total) * 100) : 0;

  const canceledFlag =
    String(meta.canceled || "0") === "1" ||
    String(meta.canceledFlag || "0") === "1";

  return {
    id: data?.campaignId || campaignId,
    paused: String(meta.paused || "0") === "1",
    canceledFlag,
    counts: { total, sent, failed, canceled: canceledCount, done, pct },
    raw: meta,
  };
}

function renderCampaign(c) {
  state.lastCampaign = c;

  const idEl = document.getElementById("campaignId");
  const badgeEl = document.getElementById("campaignPausedBadge");
  const pctEl = document.getElementById("campaignPct");
  const sentEl = document.getElementById("campaignSent");
  const failEl = document.getElementById("campaignFailed");
  const totalEl = document.getElementById("campaignTotal");
  const canceledEl = document.getElementById("campaignCanceled");
  const barEl = document.getElementById("campaignBar");
  const btnPause = document.getElementById("btnPauseResume");
  const btnCancel = document.getElementById("btnCancelCampaign");

  if (idEl) idEl.textContent = c.id || "-";

  const pct = Number(c.counts?.pct ?? 0);
  const sent = Number(c.counts?.sent ?? 0);
  const failed = Number(c.counts?.failed ?? 0);
  const total = Number(c.counts?.total ?? 0);
  const canceledCount = Number(c.counts?.canceled ?? 0);

  const done = Number(c.counts?.done ?? (sent + failed + canceledCount));
  const finished = total > 0 && done >= total;

  const isCanceled =
    !!c.canceled ||
    !!c.canceledFlag ||
    String(c.raw?.canceled || "0") === "1";

  if (badgeEl) {
    if (isCanceled) {
      badgeEl.textContent = "CANCELADA";
      badgeEl.className = "pill err";
    } else if (finished) {
      badgeEl.textContent = "FINALIZADA";
      badgeEl.className = "pill ok";
    } else if (c.paused) {
      badgeEl.textContent = "PAUSADA";
      badgeEl.className = "pill warn";
    } else {
      badgeEl.textContent = "RODANDO";
      badgeEl.className = "pill ok";
    }
  }

  if (pctEl) pctEl.textContent = `${Math.min(100, Math.max(0, pct))}%`;
  if (sentEl) sentEl.textContent = String(sent);
  if (failEl) failEl.textContent = String(failed);
  if (totalEl) totalEl.textContent = String(total);
  if (canceledEl) canceledEl.textContent = String(canceledCount);

  if (barEl) barEl.style.width = `${Math.min(100, Math.max(0, pct))}%`;

  if (btnPause) {
    btnPause.disabled = !c.id || finished || isCanceled;
    btnPause.textContent = c.paused ? "Retomar" : "Pausar";
    btnPause.dataset.paused = c.paused ? "1" : "0";
  }

  if (btnCancel) {
    btnCancel.disabled = !c.id || finished || isCanceled;
    btnCancel.textContent = isCanceled ? "Cancelada" : "Cancelar campanha";
  }

  if (isCanceled) {
    clearLastCampaignId();
    stopCampaignPolling();
    setStatus(
      "warn",
      `Campanha cancelada. Enviados: ${sent} | Falhas: ${failed} | Cancelados: ${canceledCount} | Total: ${total}`
    );
  } else if (finished) {
    clearLastCampaignId();
    stopCampaignPolling();
    setStatus("ok", `Finalizada. Enviados: ${sent} | Falhas: ${failed} | Total: ${total}`);
  }
}

function startCampaignPolling(campaignId, opts = {}) {
  stopCampaignPolling();
  state.campaignId = campaignId;
  setCampaignVisible(true);

  if (!opts?.skipSave) saveLastCampaignId(campaignId);

  const tick = async () => {
    try {
      const c = await fetchCampaignOnce(campaignId);
      if (c) renderCampaign(c);
    } catch (e) {
      if (String(e?.message || "").toLowerCase().includes("campaign não encontrada")) {
        clearLastCampaignId();
      }
      setStatus("warn", "Não consegui ler o status da campanha (rede/API).");
      appendDebug(`\n\n[warn] Falha ao ler campaign: ${e?.message || String(e)}`);
    }
  };

  tick();
  state.campaignPollTimer = setInterval(tick, 1200);
}

async function pauseOrResumeCurrentCampaign() {
  const c = state.lastCampaign;
  if (!c?.id) return;

  const isCanceled =
    !!c.canceled ||
    !!c.canceledFlag ||
    String(c.raw?.canceled || "0") === "1";
  if (isCanceled) return;

  const sent = Number(c.counts?.sent ?? 0);
  const failed = Number(c.counts?.failed ?? 0);
  const total = Number(c.counts?.total ?? 0);
  const canceledCount = Number(c.counts?.canceled ?? 0);

  const done = Number(c.counts?.done ?? (sent + failed + canceledCount));
  const finished = total > 0 && done >= total;
  if (finished) return;

  const paused = !!c.paused;
  const url = paused ? campaignResumeUrl(c.id) : campaignPauseUrl(c.id);

  const btn = document.getElementById("btnPauseResume");
  if (btn) {
    btn.disabled = true;
    btn.textContent = paused ? "Retomando..." : "Pausando...";
  }

  try {
    const res = await fetch(url, { method: "POST" });
    const data = await res.json().catch(() => null);
    if (!res.ok) throw new Error(data?.error || `HTTP ${res.status}`);

    const campaign = data?.campaign || (await fetchCampaignOnce(c.id));
    if (campaign) renderCampaign(campaign);
  } catch (e) {
    setStatus("err", "Erro ao pausar/retomar: " + (e?.message || String(e)));
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function cancelCurrentCampaign() {
  const c = state.lastCampaign;
  if (!c?.id) return;

  const isCanceled =
    !!c.canceled ||
    !!c.canceledFlag ||
    String(c.raw?.canceled || "0") === "1";
  if (isCanceled) return;

  const ok = confirm("Cancelar campanha? Isso vai parar e remover o restante da fila (não dá pra retomar).");
  if (!ok) return;

  const btnCancel = document.getElementById("btnCancelCampaign");
  const btnPause = document.getElementById("btnPauseResume");
  if (btnCancel) {
    btnCancel.disabled = true;
    btnCancel.textContent = "Cancelando...";
  }
  if (btnPause) btnPause.disabled = true;

  try {
    const res = await fetch(campaignCancelUrl(c.id), { method: "POST" });
    const data = await res.json().catch(() => null);
    if (!res.ok) throw new Error(data?.error || `HTTP ${res.status}`);

    appendDebug("\n\nCancel:\n" + JSON.stringify(data, null, 2));

    clearLastCampaignId();
    const latest = await fetchCampaignOnce(c.id);
    if (latest) renderCampaign(latest);
  } catch (e) {
    setStatus("err", "Erro ao cancelar: " + (e?.message || String(e)));
    if (btnCancel) {
      btnCancel.disabled = false;
      btnCancel.textContent = "Cancelar campanha";
    }
    if (btnPause) btnPause.disabled = false;
  }
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

const btnPauseResume = document.getElementById("btnPauseResume");
if (btnPauseResume) {
  btnPauseResume.addEventListener("click", async () => {
    await pauseOrResumeCurrentCampaign();
  });
}

const btnCancelCampaign = document.getElementById("btnCancelCampaign");
if (btnCancelCampaign) {
  btnCancelCampaign.addEventListener("click", async () => {
    await cancelCurrentCampaign();
  });
}

document.getElementById("btnEnviar").addEventListener("click", async () => {
  if (state.isSending) return;

  const tipo = document.getElementById("tipo").value;
  const mensagemTemplate = document.getElementById("mensagem").value || "";

  const limMax = Number(document.getElementById("limitMax").value || 1);
  const limMs = Number(document.getElementById("limitMs").value || 1000);

  const file = document.getElementById("arquivo")?.files?.[0] || null;
  const fileUrl = (document.getElementById("fileUrl")?.value || "").trim();

  const csv = getCsvFile();
  const tokenObj = selectedToken();

  clearDebug();

  if (!tokenObj) return setStatus("err", "Adicione um token.");

  const csvErr = validateCsvFile(csv);
  if (csvErr) return setStatus("err", csvErr);

  if (tipo === "text" && !mensagemTemplate.trim()) {
    return setStatus("err", "Mensagem obrigatória para texto.");
  }

  if (!(limMax >= 1) || limMax > MAX_RATE_MAX) {
    return setStatus("err", `Limite inválido. Use max entre 1 e ${MAX_RATE_MAX}.`);
  }

  if (!ALLOWED_INTERVALS.has(limMs)) {
    return setStatus("err", "Intervalo inválido. Use apenas 1s, 2s ou 3s.");
  }

  // mídia: exige URL/file_id ou upload (e não deixa os dois)
  if (tipo !== "text") {
    const hasRef = !!fileUrl; // URL ou file_id
    const hasUpload = !!file;

    if (hasRef && !(isHttpUrl(fileUrl) || isTelegramFileId(fileUrl))) {
      return setStatus(
        "err",
        "Informe uma URL (http/https) OU um file_id do Telegram (sem espaços)."
      );
    }

    if (hasRef && hasUpload) {
      return setStatus("err", "Escolha apenas UM: URL/file_id OU Upload.");
    }

    if (!hasRef && !hasUpload) {
      return setStatus("err", "Para mídia/documento: preencha URL/file_id OU selecione um arquivo (upload).");
    }
  }

  const buttons = readButtons();
  const btnErr = validateButtons(buttons);
  if (btnErr) return setStatus("err", btnErr);

  const form = new FormData();

  // ==== compat (envia os dois nomes) ====
  form.append("botToken", tokenObj.token);
  form.append("type", tipo);
  form.append("caption", mensagemTemplate);
  form.append("csv", csv);

  // antigo/front: limitMax + limitMs
  form.append("limitMax", String(limMax));
  form.append("limitMs", String(limMs));

  // novo/server: limit + interval (+ intervalSec)
  form.append("limit", String(limMax));
  form.append("interval", String(Math.floor(limMs / 1000)));
  form.append("intervalSec", String(Math.floor(limMs / 1000)));

  form.append("buttons", JSON.stringify(buttons));

  if (tipo !== "text") {
    if (fileUrl) form.append("fileUrl", fileUrl);
    else if (file) form.append("file", file);
  }

  setSending(true);
  setStatus("", "Enfileirando...");

  // reset campaign UI
  state.campaignId = null;
  state.lastCampaign = null;
  stopCampaignPolling();
  setCampaignVisible(false);

  const refLabel =
    tipo !== "text"
      ? (fileUrl
          ? (isHttpUrl(fileUrl) ? "URL: " + fileUrl + "\n" : "FileId: " + fileUrl + "\n")
          : (file ? "Upload: " + file.name + "\n" : ""))
      : "";

  appendDebug(
    "POST " + API_URL + "\n" +
      "Bot: " + (tokenObj.label || "Bot") + "\n" +
      "CSV: " + csv.name + "\n" +
      "Tipo: " + tipo + "\n" +
      "Botões: " + (buttons.length || 0) + "\n" +
      "Rate: " + limMax + " a cada " + (limMs / 1000) + "s\n" +
      "Dica: Para campanhas grandes, use 1 msg a cada 2–3s.\n" +
      refLabel
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
      appendDebug("\n\ncampaignId: " + data.campaignId);
      startCampaignPolling(data.campaignId);
    } else {
      appendDebug("\n\n[warn] API não retornou campaignId (sem progresso).");
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

// ✅ restore automático da última campanha (refresh/fechar e abrir)
(function restoreLastCampaign() {
  const last = loadLastCampaignId();
  if (!last) return;

  appendDebug(`\n\n[info] Restaurando última campanha: ${last}`);
  setStatus("warn", "Restaurando monitoramento da última campanha: " + last);

  startCampaignPolling(last, { skipSave: true });
})();
