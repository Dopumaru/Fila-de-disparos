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
  throw new Error("Nenhum diretório de upload gravável. Configure UPLOAD_DIR para um caminho com permissão.");
}

console.log("📁 Upload dir:", UPLOAD_DIR);

// ✅ Servir uploads por HTTP (para o worker pegar por URL e não por path)
app.use("/uploads", express.static(UPLOAD_DIR));

// ✅ multer em disco + mantém extensão
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname || "");
    const name = Date.now().toString(36) + "-" + Math.random().toString(36).slice(2, 8) + ext;
    cb(null, name);
  },
});
const upload = multer({ storage });

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
  const src = String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const delim = detectDelimiter(src);

  const lines = src
    .split("\n")
    .map((l) => l.replace(/\uFEFF/g, ""))
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

function buildRowObjectsFromCsv(text) {
  const rows = parseCsvRows(text);
  if (!rows.length) return { headers: [], items: [] };

  const headersRaw = rows[0] || [];
  const headers = headersRaw.map((h, idx) => normalizeKey(h) || `col${idx}`);

  const items = [];
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i];
    const obj = {};
    for (let c = 0; c < headers.length; c++) {
      const key = headers[c] || `col${c}`;
      obj[key] = (r[c] ?? "").toString().trim();
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
  return data.result.username; // sem "@"
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

      const idColumnRaw = (req.body.idColumn || "chatId").trim();
      const idColumn = normalizeKey(idColumnRaw) || "chatid";

      let buttons = [];
      try {
        buttons = JSON.parse(req.body.buttons || "[]");
      } catch {
        return res.status(400).json({ ok: false, error: "buttons precisa ser JSON." });
      }

      if (!botToken) return res.status(400).json({ ok: false, error: "botToken é obrigatório." });
      if (!type) return res.status(400).json({ ok: false, error: "type é obrigatório." });
      if (!(limitMax >= 1) || !(limitMs >= 200)) {
        return res.status(400).json({ ok: false, error: "limitMax>=1 e limitMs>=200." });
      }

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

      // ✅ Fonte da mídia:
      // - se veio fileUrl, usa ela
      // - se veio upload, transforma em URL /uploads/arquivo.ext (worker consegue baixar)
      const isUpload = !!mediaFile && !fileUrl;

      let mediaSource = fileUrl || null;

      if (type !== "text" && isUpload) {
        const PORT = process.env.PORT || 3000;
        const internalHost = (process.env.API_INTERNAL_HOST || "api-disparos").trim();
        const filename = path.basename(mediaFile.path);
        mediaSource = `http://${internalHost}:${PORT}/uploads/${encodeURIComponent(filename)}`;
      }

      // ===== LEADS via CSV =====
      const csvText = fs.readFileSync(csvFile.path, "utf8");
      const { items } = buildRowObjectsFromCsv(csvText);
      if (!items.length) return res.status(400).json({ ok: false, error: "CSV vazio ou inválido." });

      let leads = [];
      for (const row of items) {
        let chatId = String(row[idColumn] || "").trim();
        if (!chatId) chatId = String(row["chatid"] || "").trim();
        if (!chatId) chatId = String(row["chat_id"] || "").trim();
        if (!chatId) chatId = String(row["id"] || "").trim();
        if (!chatId) chatId = String(row["col0"] || "").trim();
        if (!chatId) continue;
        leads.push({ chatId, vars: row });
      }

      if (!leads.length) {
        return res.status(400).json({
          ok: false,
          error: `Não encontrei IDs no CSV. Verifique a coluna "${idColumnRaw}" (ex: chatId).`,
        });
      }

      // remove duplicados
      const seen = new Set();
      leads = leads.filter((l) => (seen.has(l.chatId) ? false : (seen.add(l.chatId), true)));

      // ✅ NÃO USAMOS MAIS campaignId (worker não apaga arquivo)
      // Se você quiser cleanup automático depois, a gente adiciona na API depois.

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
                  file: mediaSource, // ✅ sempre URL aqui (ou fileUrl)
                  caption: finalCaption || "",
                  options,
                  campaignId: null,
                  tempFile: false,
                },
              };

        await queue.add("envio", jobData);
        total++;
      }

      // apaga CSV upload
      try {
        if (csvPathToDelete) fs.unlinkSync(csvPathToDelete);
      } catch {}

      return res.json({
        ok: true,
        total,
        buttons: buttons.length,
        source: "csv",
        idColumn: idColumnRaw,
        unique: leads.length,
        media: type === "text" ? null : fileUrl ? "url" : "upload-url",
        mediaSource: type === "text" ? null : mediaSource,
      });
    } catch (err) {
      console.error("❌ /disparar erro:", err.message);
      try {
        if (csvPathToDelete) fs.unlinkSync(csvPathToDelete);
      } catch {}
      return res.status(500).json({ ok: false, error: "Erro interno" });
    }
  }
);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("✅ API rodando na porta", PORT));
