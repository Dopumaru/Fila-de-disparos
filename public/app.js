// public/app.js
const $ = (id) => document.getElementById(id);

const tokenInput = $("tokenInput");
const btnAddToken = $("btnAddToken");
const btnRemoveToken = $("btnRemoveToken");
const tokensArea = $("tokensArea");

const tipoEl = $("tipo");
const arquivoEl = $("arquivo");
const fileUrlEl = $("fileUrl");
const mensagemEl = $("mensagem");

const csvFileEl = $("csvFile");
const idColumnEl = $("idColumn");

const limitMaxEl = $("limitMax");
const limitMsEl = $("limitMs");

const btnEnviar = $("btnEnviar");
const statusEl = $("status");
const debugEl = $("debug");

let tokens = [];
let selectedToken = null;

function setStatus(msg, cls) {
  statusEl.className = cls || "";
  statusEl.textContent = msg || "";
}
function log(obj) {
  debugEl.textContent = typeof obj === "string" ? obj : JSON.stringify(obj, null, 2);
}

function renderTokens() {
  tokensArea.innerHTML = "";
  tokens.forEach((t) => {
    const b = document.createElement("button");
    b.type = "button";
    b.className = "tokenBtn" + (t === selectedToken ? " active" : "");
    b.textContent = t.slice(0, 4) + "..." + t.slice(-4);
    b.onclick = () => {
      selectedToken = t;
      renderTokens();
    };
    tokensArea.appendChild(b);
  });
}

btnAddToken.onclick = () => {
  const t = tokenInput.value.trim();
  if (!t) return;
  if (!tokens.includes(t)) tokens.push(t);
  selectedToken = t;
  tokenInput.value = "";
  renderTokens();
};

btnRemoveToken.onclick = () => {
  if (!selectedToken) return;
  tokens = tokens.filter((t) => t !== selectedToken);
  selectedToken = tokens[0] || null;
  renderTokens();
};

function parseCSV(text) {
  // CSV simples: separa por linha, detecta ; ou ,
  const lines = text.split(/\r?\n/).filter(Boolean);
  if (!lines.length) return [];

  const header = lines[0];
  const sep = header.includes(";") && !header.includes(",") ? ";" : ",";
  const cols = header.split(sep).map((s) => s.trim());

  const idx = cols.indexOf(idColumnEl.value.trim() || "chatId");
  if (idx === -1) throw new Error(`Coluna "${idColumnEl.value}" não encontrada no CSV`);

  const ids = [];
  for (let i = 1; i < lines.length; i++) {
    const row = lines[i].split(sep);
    const v = (row[idx] || "").trim();
    if (v) ids.push(v);
  }
  return ids;
}

async function uploadIfNeeded() {
  const f = arquivoEl.files?.[0];
  if (!f) return null;

  const fd = new FormData();
  fd.append("file", f);

  const r = await fetch("/upload", { method: "POST", body: fd });
  const j = await r.json();
  if (!j.ok) throw new Error(j.error || "Falha no upload");
  return j.tmpPath; // path local no servidor
}

btnEnviar.onclick = async () => {
  try {
    setStatus("", "");
    log("");

    btnEnviar.disabled = true;

    if (!selectedToken) throw new Error("Selecione um token");
    if (!csvFileEl.files?.[0]) throw new Error("Envie o CSV de leads");

    const csvText = await csvFileEl.files[0].text();
    const leads = parseCSV(csvText);
    if (!leads.length) throw new Error("Nenhum chatId encontrado no CSV");

    const type = tipoEl.value;
    const message = mensagemEl.value || "";

    const hasUpload = arquivoEl.files?.length > 0;
    const fileUrl = fileUrlEl.value.trim();
    if (hasUpload && fileUrl) throw new Error("Preencha URL OU Upload (não os dois).");

    let fileValue = null;
    if (type !== "text") {
      if (hasUpload) fileValue = await uploadIfNeeded();
      else if (fileUrl) fileValue = fileUrl;
      else throw new Error("Para mídia, você precisa de Upload ou URL.");
    }

    const limit = {
      max: Number(limitMaxEl.value) || 1,
      ms: Number(limitMsEl.value) || 1000,
    };

    const payload =
      type === "text"
        ? { text: message }
        : { file: fileValue, caption: message };

    const body = {
      botToken: selectedToken,
      type,
      payload,
      leads,
      limit,
      campaignId: Date.now().toString(36), // simples
    };

    const r = await fetch("/enqueue", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    const j = await r.json();
    if (!j.ok) throw new Error(j.error || "Falha ao enfileirar");

    setStatus(`✅ Campanha enfileirada: ${j.queued} jobs`, "ok");
    log(j);
  } catch (e) {
    setStatus("❌ " + e.message, "err");
    log({ error: e.message });
  } finally {
    btnEnviar.disabled = false;
  }
};
