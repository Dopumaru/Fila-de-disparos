// api/server.js
require("dotenv").config();
const path = require("path");
const fs = require("fs");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const { Queue } = require("bullmq");
const connection = require("../redis");

const app = express();
app.use(cors());
app.set("trust proxy", 1);

// ===== FRONT (public) =====
app.use(express.static(path.join(__dirname, "..", "public")));

// Health
app.get("/health", (req, res) => res.json({ ok: true }));

// Rota raiz: abre o painel
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "public", "index.html"));
});

// ===== Uploads (DIR configurável e com fallback) =====
function ensureDirWritable(dir) {
  try {
    if (!dir) return null;
    fs.mkdirSync(dir, { recursive: true });
    fs.accessSync(dir, fs.constants.W_OK);
    return dir;
  } catch {
    return null;
  }
}

const DEFAULT_UPLOAD_DIR = path.join(__dirname, "..", "uploads");
const ENV_UPLOAD_DIR = (process.env.UPLOAD_DIR || "").trim();

const UPLOAD_DIR =
  ensureDirWritable(ENV_UPLOAD_DIR) ||
  ensureDirWritable(DEFAULT_UPLOAD_DIR) ||
  ensureDirWritable("/tmp/uploads");

if (!UPLOAD_DIR) {
  throw new Error(
    "Nenhum diretório de upload gravável. Configure UPLOAD_DIR para um caminho com permissão."
  );
}

console.log("📁 Upload dir:", UPLOAD_DIR);

// ✅ Servir uploads por HTTP (worker pega por URL)
app.use(
  "/uploads",
  express.static(UPLOAD_DIR, {
    fallthrough: false,
    maxAge: "1h",
  })
);

// ✅ multer em disco + mantém extensão + limites
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname || "");
    const name =
      Date.now().toString(36) + "-" + Math.random().toString(36).slice(2, 8) + ext;
    cb(null, name);
  },
});

const upload = multer({
  storage,
  limits: {
    fileSize: Number(process.env.UPLOAD_MAX_BYTES || 50 * 1024 * 1024), // 50MB default
  },
});

const queue = new Queue("disparos", { connection });

// ===== Utils =====
function isHttpUrl(u) {
  return /^https?:\/\//i.test(String(u || "").trim());
}

function normalizeKey(k) {
  return String(k || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[^a-z0-9_]/g, "");
}

function applyTemplate(template, rowObj) {
  const t = String(template || "");
  return t.replace(/\{(\w+)\}/g, (_, key) => {
    const k = normalizeKey(key);
    const v = rowObj?.[k];
    return v == null ? "" : String(v);
  });
}

// ===== CSV (sem libs) =====
function detectDelimiter(text) {
  const firstLine =
    String(text || "")
      .replace(/\r\n/g, "\n")
      .replace(/\r/g, "\n")
      .split("\n")[0] || "";

  if (firstLine.includes(";") && !firstLine.includes(",")) return ";";
  return ",";
}

function parseCsvRows(text) {
  // remove BOM (UTF-8 BOM) e normaliza quebras
  const src = String(text || "")
    .replace(/^\uFEFF/, "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n");

  const delim = detectDelimiter(src);

  const lines = src
    .split("\n")
    .map((l) => l.replace(/\uFEFF/g, "")) // reforço anti-bom
    .map((l) => l.trimEnd())
    .filter((l) => l.trim().length > 0);

  const rows = [];
  for (const line of lines) {
    const cols = [];
    let cur = "";
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const ch = line[i];

      if (ch === '"') {
        const next = line[i + 1];
        if (inQuotes && next === '"') {
          cur += '"';
          i++;
          continue;
        }
        inQuotes = !inQuotes;
        continue;
      }

      if (!inQuotes && ch === delim) {
        cols.push(cur.trim());
        cur = "";
      } else {
        cur += ch;
      }
    }

    cols.push(cur.trim());
    rows.push(cols);
  }

  return rows;
}

/**
 * Constrói items:
 * - Se tiver header + dados: items = [{col0, col1, ... , <headers normalizados>...}]
 * - Se for lista simples (só IDs): items = [{col0: "id"}...]
 */
function buildRowObjectsFromCsv(text) {
  const rows = parseCsvRows(text);
  if (!rows.length) return { headers: [], items: [] };

  // caso lista simples: 1 coluna e (a) 1 linha OU (b) várias linhas sem header
  // Vamos detectar se a primeira linha parece header:
  // - se tem letras (a-z) -> provavelmente header
  // - se é só número/-,_ etc -> provavelmente dado
  const firstCell = String(rows[0]?.[0] ?? "").trim();
  const looksLikeHeader = /[a-zA-Z]/.test(firstCell);

  // Se não parecer header, tratamos TUDO como dados (lista simples)
  if (!looksLikeHeader) {
    const items = rows
      .map((r) => String(r?.[0] ?? "").trim())
      .filter((v) => v.length > 0)
      .map((v) => ({ col0: v }));
    return { headers: ["col0"], items };
  }

  // modo normal (header + linhas)
  const headersRaw = rows[0] || [];
  const headers = headersRaw.map((h, idx) => normalizeKey(h) || `col${idx}`);

  const items = [];
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i];
    const obj = {};
    for (let c = 0; c < headers.length; c++) {
      const key = headers[c] || `col${c}`;
      obj[key] = (r[c] ?? "").toString().trim();
      // também mantém colunas por índice (col0, col1...) pra facilitar
      obj[`col${c}`] = (r[c] ?? "").toString().trim();
    }
    items.push(obj);
  }

  return { headers, items };
}

// ===== Botões =====
async function getBotUsername(botToken) {
  const r = await fetch(`https://api.telegram.org/bot${botToken}/getMe`);
  const data = await r.json().catch(() => null);
  if (!data || !data.ok || !data.result) throw new Error("Token inválido (getMe)");
  return data.result.username;
}

function buildOptionsFromButtons(buttons, botUsername) {
  if (!Array.isArray(buttons) || buttons.length === 0) return undefined;

  const inline_keyboard = [];
  for (let i = 0; i < buttons.length; i += 2) {
    const row = [];

    for (let j = i; j < i + 2 && j < buttons.length; j++) {
      const b = buttons[j];
      const text = String(b.text || "").trim();
      const type = String(b.type || "").trim(); // url | start
      const value = String(b.value || "").trim();
      if (!text || !type || !value) continue;

      if (type === "url") {
        if (!isHttpUrl(value)) continue;
        row.push({ text, url: value });
      } else if (type === "start") {
        if (!botUsername) continue;
        const param = encodeURIComponent(value);
        row.push({ text, url: `https://t.me/${botUsername}?start=${param}` });
      }
    }

    if (row.length) inline_keyboard.push(row);
  }

  if (!inline_keyboard.length) return undefined;
  return { reply_markup: { inline_keyboard } };
}

// ✅ opcional: limpeza automática depois de X minutos (evita encher disco)
function scheduleDelete(filePath) {
  const minutes = Number(process.env.UPLOAD_TTL_MINUTES || 180);
  if (!minutes || minutes <= 0) return; // desliga se 0
  setTimeout(() => {
    try {
      if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);
    } catch {}
  }, minutes * 60 * 1000);
}

// ===== ROUTE =====
app.post(
  "/disparar",
  upload.fields([
    { name: "file", maxCount: 1 },
    { name: "csv", maxCount: 1 },
  ]),
  async (req, res) => {
    let csvPathToDelete = null;

    try {
      const botToken = (req.body.botToken || "").trim();
      const type = (req.body.type || "").trim();
      const captionTemplate = req.body.caption ?? "";
      const limitMax = Number(req.body.limitMax || 1);
      const limitMs = Number(req.body.limitMs || 1100);
      const fileUrl = (req.body.fileUrl || "").trim();

      let buttons = [];
      try {
        buttons = JSON.parse(req.body.buttons || "[]");
      } catch {
        return res.status(400).json({ ok: false, error: "buttons precisa ser JSON." });
      }

      if (!botToken) return res.status(400).json({ ok: false, error: "botToken é obrigatório." });
      if (!type) return res.status(400).json({ ok: false, error: "type é obrigatório." });

      if (type === "text" && !String(captionTemplate).trim()) {
        return res.status(400).json({ ok: false, error: "Para text, caption (mensagem) é obrigatório." });
      }

      const mediaFile = req.files?.file?.[0] || null;
      const csvFile = req.files?.csv?.[0] || null;

      if (!csvFile) return res.status(400).json({ ok: false, error: "Envie o CSV no campo csv." });
      csvPathToDelete = csvFile.path;

      if (type !== "text") {
        if (!mediaFile && !fileUrl) {
          return res.status(400).json({
            ok: false,
            error: "Para mídia/documento, envie file (upload) OU preencha fileUrl (http/https).",
          });
        }
        if (fileUrl && !isHttpUrl(fileUrl)) {
          return res.status(400).json({ ok: false, error: "fileUrl inválida (precisa http/https)." });
        }
      }

      if (!Array.isArray(buttons)) buttons = [];
      if (buttons.length > 4) buttons = buttons.slice(0, 4);

      const hasStart = buttons.some((b) => String(b?.type || "").trim() === "start");
      let botUsername = null;
      if (hasStart) botUsername = await getBotUsername(botToken);

      const options = buildOptionsFromButtons(buttons, botUsername);

      // Fonte da mídia
      const isUpload = !!mediaFile && !fileUrl;
      let mediaSource = fileUrl || null;

      if (type !== "text" && isUpload) {
        const PORT = process.env.PORT || 3000;
        const internalHost = (process.env.API_INTERNAL_HOST || "api-disparos").trim();
        const filename = path.basename(mediaFile.path);
        mediaSource = `http://${internalHost}:${PORT}/uploads/${encodeURIComponent(filename)}`;
        scheduleDelete(mediaFile.path);
      }

      // ===== LEADS via CSV =====
      const csvText = fs.readFileSync(csvFile.path, "utf8");
      const { items } = buildRowObjectsFromCsv(csvText);
      if (!items.length) return res.status(400).json({ ok: false, error: "CSV vazio ou inválido." });

      // agora sempre usa a primeira coluna do CSV:
      // - no modo header: col0 vem do item.col0 (que a gente preencheu)
      // - no modo lista simples: item.col0 já é o id
      let leads = [];
      for (const row of items) {
        const chatId = String(row?.col0 || "").trim();
        if (!chatId) continue;
        leads.push({ chatId, vars: row });
      }

      if (!leads.length) {
        return res.status(400).json({
          ok: false,
          error: `Não encontrei IDs na primeira coluna do CSV (coluna A / col0).`,
        });
      }

      // remove duplicados
      const seen = new Set();
      leads = leads.filter((l) => (seen.has(l.chatId) ? false : (seen.add(l.chatId), true)));

      let total = 0;
      for (const lead of leads) {
        const finalCaption = applyTemplate(captionTemplate, lead.vars);

        const jobData =
          type === "text"
            ? {
                chatId: lead.chatId,
                botToken,
                limit: { max: limitMax, ms: limitMs },
                type: "text",
                payload: { text: finalCaption, options },
              }
            : {
                chatId: lead.chatId,
                botToken,
                limit: { max: limitMax, ms: limitMs },
                type,
                payload: {
                  file: mediaSource,
                  caption: finalCaption || "",
                  options,
                },
              };

        await queue.add("envio", jobData);
        total++;
      }

      try { if (csvPathToDelete) fs.unlinkSync(csvPathToDelete); } catch {}

      return res.json({
        ok: true,
        total,
        buttons: buttons.length,
        source: "csv",
        unique: leads.length,
        media: type === "text" ? null : fileUrl ? "url" : "upload-url",
        mediaSource: type === "text" ? null : mediaSource,
      });
    } catch (err) {
      console.error("❌ /disparar erro:", err.message);
      try { if (csvPathToDelete) fs.unlinkSync(csvPathToDelete); } catch {}
      return res.status(500).json({ ok: false, error: "Erro interno" });
    }
  }
);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("✅ API rodando na porta", PORT));
