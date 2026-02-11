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

// garante pasta uploads
const UPLOAD_DIR = path.join(__dirname, "..", "uploads");
fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const upload = multer({ dest: UPLOAD_DIR });

const queue = new Queue("disparos", { connection });

app.get("/health", (req, res) => res.json({ ok: true }));

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
  const firstLine = String(text || "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .split("\n")[0] || "";
  // se tiver ; e não tiver , -> ;
  if (firstLine.includes(";") && !firstLine.includes(",")) return ";";
  return ",";
}

// parser simples: vírgula ou ; + aspas (básico)
function parseCsvRows(text) {
  const src = String(text || "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n");

  const delim = detectDelimiter(src);

  const lines = src
    .split("\n")
    .map(l => l.trim())
    .filter(Boolean);

  const rows = [];
  for (const line of lines) {
    const cols = [];
    let cur = "";
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const ch = line[i];

      if (ch === '"') {
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
  const headers = headersRaw.map(h => normalizeKey(h));

  // se não tiver header útil, cria header genérico col0, col1...
  const hasAnyHeader = headers.some(h => h && h !== "col0");
  const items = [];

  if (hasAnyHeader) {
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
  } else {
    // sem header: trata primeira coluna como chatId por padrão
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const obj = {};
      for (let c = 0; c < r.length; c++) {
        obj[`col${c}`] = (r[c] ?? "").toString().trim();
      }
      items.push(obj);
    }
    return { headers: [], items };
  }
}

function applyTemplate(template, rowObj) {
  const t = String(template || "");
  // substitui {nome} {cidade}...
  return t.replace(/\{(\w+)\}/g, (_, key) => {
    const k = normalizeKey(key);
    const v = rowObj?.[k];
    return v == null ? "" : String(v);
  });
}

// ===== Botões =====

// Busca username do bot (sem salvar)
async function getBotUsername(botToken) {
  // Node 18+ tem fetch global
  const r = await fetch(`https://api.telegram.org/bot${botToken}/getMe`);
  const data = await r.json().catch(() => null);

  if (!data || !data.ok || !data.result) {
    throw new Error("Token inválido (getMe)");
  }

  return data.result.username; // sem "@"
}

// Converte array buttons -> options.reply_markup.inline_keyboard
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
// agora aceita: file (mídia) e csv (leads)
app.post(
  "/disparar",
  upload.fields([
    { name: "file", maxCount: 1 },
    { name: "csv", maxCount: 1 },
  ]),
  async (req, res) => {
    try {
      const botToken = (req.body.botToken || "").trim();
      const type = (req.body.type || "").trim(); // text/photo/video/audio/document/voice/video_note
      const captionTemplate = req.body.caption ?? "";
      const limitMax = Number(req.body.limitMax || 1);
      const limitMs = Number(req.body.limitMs || 1100);

      // idColumn: qual coluna do CSV contém o chatId
      const idColumnRaw = (req.body.idColumn || "chatid").trim();
      const idColumn = normalizeKey(idColumnRaw) || "chatid";

      // buttons (opcional)
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

      // valida template x tipo
      if (type === "text") {
        if (!String(captionTemplate).trim()) {
          return res.status(400).json({ ok: false, error: "Para text, caption (mensagem) é obrigatório." });
        }
      }

      // pega arquivos (mídia e csv)
      const mediaFile = req.files?.file?.[0] || null;
      const csvFile = req.files?.csv?.[0] || null;

      // valida arquivo x tipo (mídia)
      if (type !== "text") {
        if (!mediaFile) {
          return res.status(400).json({ ok: false, error: "Para mídia/documento, envie o arquivo no campo file." });
        }
      }

      // limita botões
      if (!Array.isArray(buttons)) buttons = [];
      if (buttons.length > 4) buttons = buttons.slice(0, 4);

      // se tiver botão START, precisamos do username
      const hasStart = buttons.some((b) => String(b?.type || "").trim() === "start");
      let botUsername = null;
      if (hasStart) {
        botUsername = await getBotUsername(botToken);
        if (!botUsername) {
          return res.status(400).json({ ok: false, error: "Não consegui obter username do bot para botão START." });
        }
      }
      const options = buildOptionsFromButtons(buttons, botUsername);

      // caminho do arquivo de mídia no servidor
      const mediaPath = mediaFile?.path || null;

      // ===== LEADS: CSV ou IDS =====
      let leads = []; // array de { chatId, vars }

      if (csvFile) {
        // lê csv e monta leads
        const csvText = fs.readFileSync(csvFile.path, "utf8");
        const { items } = buildRowObjectsFromCsv(csvText);

        if (!items.length) {
          return res.status(400).json({ ok: false, error: "CSV vazio ou inválido." });
        }

        for (const row of items) {
          // tenta pegar chatId por coluna escolhida
          let chatId = String(row[idColumn] || "").trim();

          // fallback comum: chatid/chat_id/id/col0
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
      } else {
        // ids manual
        let ids = [];
        try {
          ids = JSON.parse(req.body.ids || "[]");
        } catch {
          return res.status(400).json({ ok: false, error: 'ids precisa ser JSON (ex: ["123","456"]).' });
        }

        if (!Array.isArray(ids) || ids.length === 0) {
          return res.status(400).json({ ok: false, error: "Envie ids (array) ou um csv." });
        }

        leads = ids
          .map((rawId) => String(rawId).trim())
          .filter(Boolean)
          .map((chatId) => ({ chatId, vars: {} }));
      }

      // remove duplicados por chatId (mantém primeira ocorrência)
      const seen = new Set();
      leads = leads.filter((l) => {
        if (seen.has(l.chatId)) return false;
        seen.add(l.chatId);
        return true;
      });

      // ===== ENFILEIRAR =====
      let total = 0;

      for (const lead of leads) {
        const chatId = lead.chatId;

        const finalCaption =
          lead.vars && Object.keys(lead.vars).length
            ? applyTemplate(captionTemplate, lead.vars)
            : String(captionTemplate);

        const jobData =
          type === "text"
            ? {
                chatId,
                botToken,
                limit: { max: limitMax, ms: limitMs },
                type: "text",
                payload: {
                  text: finalCaption,
                  options,
                },
              }
            : {
                chatId,
                botToken,
                limit: { max: limitMax, ms: limitMs },
                type,
                payload: {
                  file: mediaPath,
                  caption: finalCaption || "",
                  options,
                  tempFile: true,
                },
              };

        await queue.add("envio", jobData);
        total++;
      }

      console.log(
        `✅ Enfileirado: total=${total} type=${type} leads=${csvFile ? "csv" : "ids"} buttons=${buttons.length} token=${maskToken(
          botToken
        )}`
      );

      // opcional: apaga o csv upload pra não acumular
      try {
        if (csvFile?.path) fs.unlinkSync(csvFile.path);
      } catch {}

      return res.json({
        ok: true,
        total,
        buttons: buttons.length,
        source: csvFile ? "csv" : "ids",
        idColumn: idColumnRaw,
      });
    } catch (err) {
      console.error("❌ /disparar erro:", err.message);
      return res.status(500).json({ ok: false, error: "Erro interno" });
    }
  }
);

const PORT = process.env.API_PORT || 3000;
app.listen(PORT, () => console.log("✅ API rodando na porta", PORT));

