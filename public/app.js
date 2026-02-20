// public/app.js (browser-safe)

// ===== helpers =====
function $(sel) { return document.querySelector(sel); }
function toast(msg) { alert(msg); } // simples (pode trocar depois)

async function apiFetch(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  if (!res.ok) {
    throw new Error(data?.error || data?.message || `HTTP ${res.status}: ${text}`);
  }
  return data;
}

// ===== token store (local) =====
const TOKENS_KEY = "painel_tokens";
const SELECTED_KEY = "painel_token_selected";

function loadTokens() {
  try { return JSON.parse(localStorage.getItem(TOKENS_KEY) || "[]"); }
  catch { return []; }
}
function saveTokens(tokens) {
  localStorage.setItem(TOKENS_KEY, JSON.stringify(tokens));
}
function getSelectedToken() {
  return localStorage.getItem(SELECTED_KEY) || "";
}
function setSelectedToken(t) {
  localStorage.setItem(SELECTED_KEY, t || "");
}

// ===== UI wiring =====
function renderTokenInfo() {
  // Se você tiver um <select> de tokens, renderiza aqui.
  // Como não vi seu HTML completo, só garante que o token digitado vira selecionado.
}

async function onAddToken() {
  const input = $("#botToken");
  const token = (input?.value || "").trim();
  if (!token) return toast("Informe o token.");

  // salva localmente
  const tokens = loadTokens();
  if (!tokens.includes(token)) tokens.push(token);
  saveTokens(tokens);
  setSelectedToken(token);

  toast("Token adicionado e selecionado.");

  // opcional: validar token no backend (se existir endpoint)
  // se seu backend tiver /api/validate-bot, descomenta:
  // await apiFetch("/api/validate-bot", { method: "POST", body: JSON.stringify({ botToken: token }) });

  renderTokenInfo();
}

function onRemoveToken() {
  const selected = getSelectedToken();
  if (!selected) return toast("Nenhum token selecionado.");
  const tokens = loadTokens().filter(t => t !== selected);
  saveTokens(tokens);
  setSelectedToken(tokens[0] || "");
  toast("Token removido.");
  renderTokenInfo();
}

// ===== init =====
window.addEventListener("DOMContentLoaded", () => {
  // Ajuste esses IDs para baterem com seu HTML:
  // input token
  // <input id="botToken" />
  // botões:
  // <button id="btnAddToken">Adicionar token</button>
  // <button id="btnRemoveToken">Remover token selecionado</button>

  const addBtn = $("#btnAddToken");
  const rmBtn = $("#btnRemoveToken");

  if (addBtn) addBtn.addEventListener("click", onAddToken);
  if (rmBtn) rmBtn.addEventListener("click", onRemoveToken);

  renderTokenInfo();
});
