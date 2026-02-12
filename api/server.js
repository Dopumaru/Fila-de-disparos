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

// ===== FRONT (public) =====
// Serve /public (index.html, app.js, etc.)
app.use(express.static(path.join(__dirname, "..", "public")));

// CORS (ok manter; se quiser travar depois, dá pra limitar origem)
app.use(cors());

// Health
app.get("/health", (req, res) => res.json({ ok: true }));

// Rota raiz: abre o painel
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "public", "index.html"));
});

// ===== Uploads =====
const UPLOAD_DIR = path.join(__dirname, "..", "uploads");
fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const upload = multer({ dest: UPLOAD_DIR });
const queue = new Queue("disparos", { connection });

// ===== Utils =====
function maskToken(t) {
  if (!t) return "";
  const s = String(t);
  if (s.length <= 10) return "***";
  return s.slice(0, 4) + "..." + s.slice(-4);
}

function isHttpUrl(u) {
  return /^https?:\/\//i.test(u || "");
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
    .map((l) => l.replace(/\uFEFF/g, "")) // remove BOM
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

function normalizeKey(k) {
  return String(k || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[^a-z0-9_]/g, "");
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

function applyTemplate(template, rowObj) {
  const t = String(template || "");
  return t.replace(/\{(\w+)\}/g, (_, key) => {
    const k = normalizeKey(key);
    const v = rowObj?.[k];
    return v == null ? "" : String(v);
  });
}

// ===== Botões =====
async function getBotUsername(botToken) {
  // Node 18+ tem fetch global
  const r = await fetch(`https://api.telegram.org/bot${botToken}/getMe`);
  const data = await r.json().catch(() => null);

  if (!data || !data.ok || !data.result) {
    throw new Error("Token inválido (getMe)");
  }

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
// aceita: file (mídia) e csv (leads) — CSV OBRIGATÓRIO
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

      if (!csvFile) {
        return res.status(400).json({
          ok: false,
          error: "Envie o CSV no campo csv. IDs manuais foram desativados.",
        });
      }
      csvPathToDelete = csvFile.path;

      if (type !== "text" && !mediaFile) {
        return res.status(400).json({ ok: false, error: "Para mídia/documento, envie o arquivo no campo file." });
      }

      if (!Array.isArray(buttons)) buttons = [];
      if (buttons.length > 4) buttons = buttons.slice(0, 4);

      const hasStart = buttons.some((b) => String(b?.type || "").trim() === "start");
      let botUsername = null;
      if (hasStart) {
        botUsername = await getBotUsername(botToken);
        if (!botUsername) {
          return res.status(400).json({ ok: false, error: "Não consegui obter username do bot para botão START." });
        }
      }

      const options = buildOptionsFromButtons(buttons, botUsername);
      const mediaPath = mediaFile?.path || null;

      // ===== LEADS via CSV =====
      const csvText = fs.readFileSync(csvFile.path, "utf8");
      const { items } = buildRowObjectsFromCsv(csvText);

      if (!items.length) {
        return res.status(400).json({ ok: false, error: "CSV vazio ou inválido." });
      }

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
      leads = leads.filter((l) => {
        if (seen.has(l.chatId)) return false;
        seen.add(l.chatId);
        return true;
      });

      // ===== ENFILEIRAR =====
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
                  file: mediaPath,
                  caption: finalCaption || "",
                  options,
                  tempFile: false,
                },
              };

        await queue.add("envio", jobData);
        total++;
      }

      console.log(
        `✅ Enfileirado: total=${total} type=${type} source=csv buttons=${buttons.length} token=${maskToken(botToken)}`
      );

      // apaga CSV upload (não precisa guardar)
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

// ===== Start =====
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("✅ API rodando na porta", PORT);
});

