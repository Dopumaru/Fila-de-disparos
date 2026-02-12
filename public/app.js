
const API_URL = "/disparar";

const state = {
  tokens: [],
  selectedTokenId: null,
  isSending: false,
  isValidating: false,
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
  // limite simples (ex: 10MB)
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
    // não bloqueia o uso — em rede restrita isso acontece
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

  const file = document.getElementById("arquivo").files[0] || null;
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

  if (tipo !== "text" && !file) {
    return setStatus("err", "Selecione um arquivo (mídia/documento).");
  }

  if (!(limMax >= 1) || !(limMs >= 200)) {
    return setStatus("err", "Limite inválido. Use max >= 1 e intervalo >= 200ms.");
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
  if (file) form.append("file", file);

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
    (file ? "Arquivo: " + file.name + "\n" : "")
  );

  try {
    const res = await fetch(API_URL, { method: "POST", body: form });
    const data = await res.json().catch(() => null);

    if (!res.ok) {
      // tenta mostrar erro “limpo”
      const apiMsg = data?.error || data?.message || (data ? JSON.stringify(data) : "");
      throw new Error(`HTTP ${res.status}${apiMsg ? " - " + apiMsg : ""}`);
    }

    // IMPORTANTE: aqui a API só confirmou “enfileirado”
    setStatus(
      "ok",
      "Campanha enfileirada. Total: " + (data?.total ?? "?") + (data?.unique ? ` (únicos: ${data.unique})` : "")
    );

    appendDebug("\nResposta:\n" + JSON.stringify(data, null, 2));

    // Se quiser “progresso real” (enviando X/Y), isso é outra feature:
    // precisa endpoint de status + worker atualizando contadores.
  } catch (err) {
    setStatus("err", "Erro ao enviar para API: " + (err?.message || String(err)));
    appendDebug("\nErro:\n" + (err?.message || String(err)));
  } finally {
    setSending(false);
  }
});

renderTokens();
