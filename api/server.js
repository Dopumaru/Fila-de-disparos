// api/server.js
require("dotenv").config();
const path = require("path");
const fs = require("fs");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const crypto = require("crypto");
const IORedis = require("ioredis");
const { Queue } = require("bullmq");
const connection = require("../redis");

const app = express();
app.use(cors());
app.set("trust proxy", 1);

// ===== FRONT (public) =====
app.use(express.static(path.join(__dirname, "..", "public")));

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

// ===== ✅ Redis client (para status de campanha) =====
function buildRedisUrlFromConnection(conn) {
  if (!conn) return null;
  if (typeof conn === "string") return conn; // se seu redis.js exporta uma URL
  const host = conn.host || conn.hostname;
  const port = conn.port || 6379;
  if (!host) return null;

  // suporte opcional a senha/db se existirem no objeto
  const password = conn.password ? encodeURIComponent(conn.password) : null;
  const db = Number.isFinite(conn.db) ? conn.db : null;

  let auth = "";
  if (password) auth = `:${password}@`;

  let url = `redis://${auth}${host}:${port}`;
  if (db != null) url += `/${db}`;
  return url;
}

const REDIS_URL =
  (process.env.REDIS_URL && process.env.REDIS_URL.trim()) ||
  buildRedisUrlFromConnection(connection);

if (!REDIS_URL) {
  throw new Error(
    "REDIS_URL não definido e não foi possível inferir pelo connection. Defina REDIS_URL (ex: redis://redis-fila:6379)."
  );
}

const redis = new IORedis(REDIS_URL);
redis.on("error", (e) => console.error("❌ Redis error:", e.message));
redis.on("connect", () => console.log("✅ Redis (status) conectado via", REDIS_URL));

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

// ===== 10) Health real =====
app.get("/health", async (_req, res) => {
  try {
    const ping = await redis.ping();

    const counts = await queue.getJobCounts(
      "waiting",
      "active",
      "completed",
      "failed",
      "delayed",
      "paused"
    );

    let uploadWritable = false;
    try {
      fs.accessSync(UPLOAD_DIR, fs.constants.W_OK);
      uploadWritable = true;
    } catch {}

    return res.json({
      ok: true,
      redis: ping === "PONG" ? "ok" : ping,
      queue: { name: "disparos", counts },
      uploads: { dir: UPLOAD_DIR, writable: uploadWritable },
      ts: new Date().toISOString(),
    });
  } catch (e) {
    console.error("❌ /health error:", e.message);
    return res.status(500).json({
      ok: false,
      error: "healthcheck_failed",
      detail: e?.message,
      ts: new Date().toISOString(),
    });
  }
});

// Rota raiz: abre o painel
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "public", "index.html"));
});

// ===== STATUS CAMPANHA =====
app.get("/campaign/:id/status", async (req, res) => {
  try {
    const id = String(req.params.id || "").trim();
    const key = `campaign:${id}`;
    const data = await redis.hgetall(key);

    if (!data || !data.id) {
      return res.status(404).json({ ok: false, error: "campaign_not_found" });
    }

    const total = Number(data.total || 0);
    const sent = Number(data.sent || 0);
    const failed = Number(data.failed || 0);
    const done = sent + failed;
    const pending = Math.max(0, total - done);

    return res.json({
      ok: true,
      campaignId: id,
      total,
      sent,
      failed,
      pending,
      done,
      progress: total > 0 ? done / total : 0,
      createdAt: data.createdAt ? Number(data.createdAt) : null,
      meta: {
        type: data.type || null,
        idColumn: data.idColumn || null,
      },
    });
  } catch (e) {
    console.error("❌ campaign status error:", e.message);
    res.status(500).json({ ok: false, error: "internal_error" });
  }
});

// ===== PAUSE/RESUME GLOBAL DA FILA =====
app.post("/queue/pause", async (_req, res) => {
  try {
    await queue.pause();
    res.json({ ok: true, paused: true });
  } catch (e) {
    console.error("❌ pause error:", e.message);
    res.status(500).json({ ok: false, error: "internal_error" });
  }
});

app.post("/queue/resume", async (_req, res) => {
  try {
    await queue.resume();
    res.json({ ok: true, paused: false });
  } catch (e) {
    console.error("❌ resume error:", e.message);
    res.status(500).json({ ok: false, error: "internal_error" });
  }
});

app.get("/queue/status", async (_req, res) => {
  try {
    const paused = await queue.isPaused();
    res.json({ ok: true, paused: !!paused });
  } catch (e) {
    console.error("❌ queue status error:", e.message);
    res.status(500).json({ ok: false, error: "internal_error" });
  }
});

// ===== 7) Validações extras =====
const ALLOWED_TYPES = new Set([
  "text",
  "photo",
  "video",
  "audio",
  "voice",
  "document",
  "video_note",
]);

const allowedByType = {
  photo: [".jpg", ".jpeg", ".png", ".webp"],
  video: [".mp4", ".mov", ".mkv", ".webm"],
  audio: [".mp3", ".m4a", ".wav", ".ogg"],
  voice: [".ogg"],
  video_note: [".mp4"],
  document: [], // deixa livre (PDF/ZIP/etc). Se quiser travar, coloque lista.
};

// ===== 9) Retenção de jobs (BullMQ) =====
// Ajuste fino via env (opcional). Defaults seguros.
const RETAIN_COMPLETED = Number(process.env.RETAIN_COMPLETED || 2000); // últimos 2000 concluídos
const RETAIN_FAILED = Number(process.env.RETAIN_FAILED || 5000); // últimos 5000 falhados

function jobRetentionOptions() {
  // BullMQ aceita count/age; aqui usamos count (mais garantido)
  const completed = Number.isFinite(RETAIN_COMPLETED) && RETAIN_COMPLETED > 0 ? RETAIN_COMPLETED : 2000;
  const failed = Number.isFinite(RETAIN_FAILED) && RETAIN_FAILED > 0 ? RETAIN_FAILED : 5000;

  return {
    removeOnComplete: { count: completed },
    removeOnFail: { count: failed },
  };
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

      // ===== validações base =====
      if (!botToken) return res.status(400).json({ ok: false, error: "botToken é obrigatório." });
      if (!type) return res.status(400).json({ ok: false, error: "type é obrigatório." });

      // ✅ valida type permitido
      if (!ALLOWED_TYPES.has(type)) {
        return res.status(400).json({
          ok: false,
          error: `type inválido: ${type}`,
          allowed: Array.from(ALLOWED_TYPES),
        });
      }

      if (!(limitMax >= 1) || !(limitMs >= 200)) {
        return res.status(400).json({ ok: false, error: "limitMax>=1 e limitMs>=200." });
      }

      if (type === "text" && !String(captionTemplate).trim()) {
        return res
          .status(400)
          .json({ ok: false, error: "Para text, caption (mensagem) é obrigatório." });
      }

      // ✅ fileUrl sanity
      if (fileUrl && fileUrl.length > 2000) {
        return res.status(400).json({ ok: false, error: "fileUrl muito longa." });
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

      // ✅ valida extensão do upload por type (quando aplicável)
      if (type !== "text" && mediaFile) {
        const ext = path.extname(mediaFile.originalname || "").toLowerCase();
        const list = allowedByType[type] || [];
        if (list.length && !list.includes(ext)) {
          return res.status(400).json({
            ok: false,
            error: `Arquivo inválido para type='${type}'.`,
            receivedExt: ext,
            allowedExt: list,
          });
        }
      }

      if (!Array.isArray(buttons)) buttons = [];
      if (buttons.length > 4) buttons = buttons.slice(0, 4);

      const hasStart = buttons.some((b) => String(b?.type || "").trim() === "start");
      let botUsername = null;
      if (hasStart) botUsername = await getBotUsername(botToken);

      const options = buildOptionsFromButtons(buttons, botUsername);

      // ✅ Fonte da mídia:
      const isUpload = !!mediaFile && !fileUrl;
      let mediaSource = fileUrl || null;

      if (type !== "text" && isUpload) {
        const PORT = process.env.PORT || 3000;
        const internalHost = (process.env.API_INTERNAL_HOST || "api-disparos").trim();

        // aviso útil (não quebra)
        if (!process.env.API_INTERNAL_HOST) {
          console.warn("⚠️ API_INTERNAL_HOST não definido. Worker pode não conseguir acessar /uploads.");
        }

        const filename = path.basename(mediaFile.path);
        mediaSource = `http://${internalHost}:${PORT}/uploads/${encodeURIComponent(filename)}`;
        scheduleDelete(mediaFile.path);
      }

      // ===== LEADS via CSV =====
      const csvText = fs.readFileSync(csvFile.path, "utf8");
      const { headers, items } = buildRowObjectsFromCsv(csvText);
      if (!items.length) return res.status(400).json({ ok: false, error: "CSV vazio ou inválido." });

      // ✅ valida se existe alguma coluna “ID” conhecida no header
      const possibleIdCols = new Set([idColumn, "chatid", "chat_id", "id", "col0"]);
      const hasAnyIdCol = headers.some((h) => possibleIdCols.has(h));
      if (!hasAnyIdCol) {
        return res.status(400).json({
          ok: false,
          error: `CSV não contém coluna de ID válida. Tentativas: ${Array.from(possibleIdCols).join(", ")}`,
          headersEncontrados: headers,
        });
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
      leads = leads.filter((l) => (seen.has(l.chatId) ? false : (seen.add(l.chatId), true)));

      // ===== CAMPANHA =====
      const campaignId = crypto.randomUUID();
      const campaignKey = `campaign:${campaignId}`;
      const createdAt = Date.now();
      const totalLeads = leads.length;

      await redis.hset(campaignKey, {
        id: campaignId,
        createdAt: String(createdAt),
        total: String(totalLeads),
        sent: "0",
        failed: "0",
        type: String(type),
        idColumn: String(idColumnRaw),
      });
      await redis.pexpire(campaignKey, 7 * 24 * 60 * 60 * 1000);

      let total = 0;
      const retention = jobRetentionOptions();

      for (const lead of leads) {
        const finalCaption = applyTemplate(captionTemplate, lead.vars);

        const jobData =
          type === "text"
            ? {
                chatId: lead.chatId,
                botToken,
                limit: { max: limitMax, ms: limitMs },
                type: "text",
                campaignId,
                payload: { text: finalCaption, options },
              }
            : {
                chatId: lead.chatId,
                botToken,
                limit: { max: limitMax, ms: limitMs },
                type,
                campaignId,
                payload: {
                  file: mediaSource,
                  caption: finalCaption || "",
                  options,
                },
              };

        // ✅ Passo 9: retenção de jobs
        await queue.add("envio", jobData, retention);
        total++;
      }

      // apaga CSV upload
      try {
        if (csvPathToDelete) fs.unlinkSync(csvPathToDelete);
      } catch {}

      return res.json({
        ok: true,
        campaignId,
        total,
        buttons: buttons.length,
        source: "csv",
        idColumn: idColumnRaw,
        unique: leads.length,
        media: type === "text" ? null : fileUrl ? "url" : "upload-url",
        mediaSource: type === "text" ? null : mediaSource,
        retention: {
          completed: retention.removeOnComplete?.count,
          failed: retention.removeOnFail?.count,
        },
      });
    } catch (err) {
      console.error("❌ /disparar erro:", err);

      try {
        if (csvPathToDelete) fs.unlinkSync(csvPathToDelete);
      } catch {}

      return res.status(500).json({ ok: false, error: "Erro interno", detail: err?.message });
    }
  }
);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("✅ API rodando na porta", PORT));
