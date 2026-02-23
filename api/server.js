// api/server.js
require("dotenv").config();
const path = require("path");
const fs = require("fs");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const crypto = require("crypto");
const { Queue } = require("bullmq");

const { redis, connection } = require("../redis");

const app = express();
app.use(cors());
app.use(express.json({ limit: "8mb" }));
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
const UPLOAD_DIR =
  ensureDirWritable(process.env.UPLOAD_DIR) ||
  ensureDirWritable(DEFAULT_UPLOAD_DIR) ||
  null;

if (!UPLOAD_DIR) {
  console.warn("⚠️ UPLOAD_DIR indisponível. Upload local pode falhar.");
}

// Servir uploads
const uploadsDirToServe = UPLOAD_DIR || DEFAULT_UPLOAD_DIR;
app.use("/uploads", express.static(uploadsDirToServe));

// ===== Multer =====
// IMPORTANT: seu painel manda `csv` e também `file` (mídia).
// Então usamos upload.fields([csv, file]).
const upload = multer({
  dest: uploadsDirToServe,
  limits: { fileSize: 60 * 1024 * 1024 }, // 60MB
});

// ===== Queue =====
const QUEUE_NAME = process.env.QUEUE_NAME || "disparos";

// Limites pra não lotar Redis em disparos grandes (26k+)
const REMOVE_COMPLETE_COUNT = Number(process.env.REMOVE_COMPLETE_COUNT) || 2000;
const REMOVE_FAIL_COUNT = Number(process.env.REMOVE_FAIL_COUNT) || 5000;

const queue = new Queue(QUEUE_NAME, {
  connection,
  defaultJobOptions: {
    removeOnComplete: { count: REMOVE_COMPLETE_COUNT },
    removeOnFail: { count: REMOVE_FAIL_COUNT },
  },
});

// ===== Helpers =====
function normalizeKey(k) {
  return String(k || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_");
}

function applyTemplate(template, row) {
  const t = String(template ?? "");
  return t.replace(/\{(\w+)\}/g, (_, key) => {
    const nk = normalizeKey(key);
    const val = row?.[nk];
    return val == null ? "" : String(val);
  });
}

function campaignKey(id) {
  return `campaign:${id}`;
}

function genId() {
  return crypto.randomBytes(10).toString("hex");
}

async function setCampaignMeta(id, meta) {
  const key = campaignKey(id);
  await redis.hset(key, meta);
  await redis.expire(key, 60 * 60 * 24 * 7); // 7 dias
}

async function getCampaignMeta(id) {
  const key = campaignKey(id);
  const data = await redis.hgetall(key);
  return data || {};
}

// Parse simples (CSV): primeira coluna = id
function parseCsvFirstColumnAsIds(csvText) {
  const lines = String(csvText || "")
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  if (lines.length === 0) return [];

  // se primeira linha parece header (tem letras), pula
  const firstCell = lines[0].split(",")[0].trim();
  const startIdx = /[a-zA-Z]/.test(firstCell) ? 1 : 0;

  const out = [];
  for (let i = startIdx; i < lines.length; i++) {
    const id = lines[i].split(",")[0].trim();
    if (id) out.push({ id });
  }
  return out;
}

function toInt(n, def) {
  const v = Number(n);
  return Number.isFinite(v) ? v : def;
}

// Base URL pública (para o navegador/Telegram). Worker também consegue baixar via pública.
function getPublicBaseUrl(req) {
  const envBase = String(process.env.PUBLIC_BASE_URL || "").trim();
  if (envBase) return envBase.replace(/\/+$/, "");

  const proto = req.headers["x-forwarded-proto"] || req.protocol || "http";
  const host = req.headers["x-forwarded-host"] || req.get("host");
  return `${proto}://${host}`.replace(/\/+$/, "");
}

// =====================================================================================
// ✅ ROTA NOVA (JSON): /campaign
// =====================================================================================
app.post("/campaign", async (req, res) => {
  try {
    const {
      leads = [],
      botToken,
      chatIdColumn = "id",
      ratePerSecond = 10,
      message,
      fileUrl,
      fileType,
      caption,
      campaignId,
    } = req.body || {};

    if (!Array.isArray(leads) || leads.length === 0) {
      return res.status(400).json({ ok: false, error: "Leads vazio." });
    }
    if (!botToken) {
      return res.status(400).json({ ok: false, error: "botToken ausente." });
    }

    const id = campaignId || genId();

    const rps = Math.max(1, Math.min(30, toInt(ratePerSecond, 10)));
    const baseDelayMs = Math.floor(1000 / rps);

    await setCampaignMeta(id, {
      paused: "0",
      canceled: "0",
      total: String(leads.length),
      sent: "0",
      failed: "0",
      createdAt: new Date().toISOString(),
      ratePerSecond: String(rps),
    });

    const idCol = normalizeKey(chatIdColumn);

    const jobs = leads.map((row, idx) => {
      const chatId =
        row?.[idCol] ?? row?.[chatIdColumn] ?? row?.id ?? row?.chat_id;

      const textFinal = applyTemplate(message, row);
      const captionFinal = applyTemplate(caption, row);

      return {
        name: "send",
        data: {
          campaignId: id,
          botToken,
          chatId,
          text: textFinal,
          fileUrl,
          fileType,
          caption: captionFinal,
        },
        opts: {
          delay: idx * baseDelayMs,
          attempts: 6,
          backoff: { type: "exponential", delay: 2000 },
          removeOnComplete: { count: REMOVE_COMPLETE_COUNT },
          removeOnFail: { count: REMOVE_FAIL_COUNT },
        },
      };
    });

    await queue.addBulk(jobs);

    return res.json({
      ok: true,
      campaignId: id,
      total: leads.length,
      ratePerSecond: rps,
    });
  } catch (e) {
    console.error("❌ /campaign erro:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// =====================================================================================
// ✅ COMPAT COM TEU PAINEL: /disparar (multipart/form-data + CSV upload + arquivo opcional)
// =====================================================================================
app.post(
  "/disparar",
  upload.fields([
    { name: "csv", maxCount: 1 },
    { name: "file", maxCount: 1 },
  ]),
  async (req, res) => {
    try {
      // Token vindo do painel
      const botToken =
        req.body.botToken ||
        req.body.token ||
        req.body.bot ||
        req.body.bot_token ||
        process.env.TELEGRAM_BOT_TOKEN;

      // Mensagem (texto)
      const message =
        req.body.message ||
        req.body.mensagem ||
        req.body.msg ||
        req.body.text ||
        req.body.texto ||
        req.body.messageText ||
        req.body.message_text ||
        req.body.legenda ||
        req.body.caption ||
        "";

      // Tipo do envio
      const tipo = String(req.body.tipo || req.body.type || "text").toLowerCase();

      // Rate do painel
      const limit = toInt(req.body.limit || req.body.rate || 1, 1);
      const intervalSec = toInt(req.body.intervalSec || req.body.interval || 1, 1);

      const ratePerSecond = Math.max(
        1,
        Math.min(30, Math.floor(limit / Math.max(1, intervalSec)) || 1)
      );

      if (!botToken) {
        return res.status(400).json({
          ok: false,
          error: "botToken ausente (env TELEGRAM_BOT_TOKEN ou body botToken/token/bot).",
        });
      }

      const csvFile = req.files?.csv?.[0];
      if (!csvFile?.path) {
        return res.status(400).json({ ok: false, error: "CSV não enviado (campo 'csv')." });
      }

      // lê CSV e remove do disco
      const csvText = fs.readFileSync(csvFile.path, "utf8");
      try {
        fs.unlinkSync(csvFile.path);
      } catch {}

      const leads = parseCsvFirstColumnAsIds(csvText);
      if (leads.length === 0) {
        return res.status(400).json({ ok: false, error: "Nenhum ID válido no CSV." });
      }

      // Arquivo opcional vindo do painel
      const mediaFile = req.files?.file?.[0];

      // Caption opcional
      const caption =
        req.body.caption ||
        req.body.legenda ||
        req.body.captionText ||
        req.body.caption_text ||
        "";

      // Se for texto puro, precisa de message
      const hasMedia = !!mediaFile;
      if (!hasMedia && !String(message || "").trim()) {
        return res.status(400).json({
          ok: false,
          error: "Mensagem vazia. Preencha 'Mensagem / Legenda' para tipo Texto.",
        });
      }

      const campaignId = genId();

      await setCampaignMeta(campaignId, {
        paused: "0",
        canceled: "0",
        total: String(leads.length),
        sent: "0",
        failed: "0",
        createdAt: new Date().toISOString(),
        ratePerSecond: String(ratePerSecond),
      });

      const baseDelayMs = Math.floor(1000 / ratePerSecond);

      // Monta fileUrl se tiver upload
      let fileUrl;
      if (mediaFile?.filename) {
        const base = getPublicBaseUrl(req);
        fileUrl = `${base}/uploads/${mediaFile.filename}`;
      }

      const jobs = leads.map((row, idx) => ({
        name: "send",
        data: {
          campaignId,
          botToken,
          chatId: row.id,
          text: message,
          fileUrl: fileUrl || undefined,
          fileType: tipo === "text" ? undefined : tipo,
          caption: caption || undefined,
        },
        opts: {
          delay: idx * baseDelayMs,
          attempts: 6,
          backoff: { type: "exponential", delay: 2000 },
          removeOnComplete: { count: REMOVE_COMPLETE_COUNT },
          removeOnFail: { count: REMOVE_FAIL_COUNT },
        },
      }));

      await queue.addBulk(jobs);

      return res.json({
        ok: true,
        campaignId,
        total: leads.length,
        ratePerSecond,
        usedFileUrl: !!fileUrl,
        receivedFileFields: Object.keys(req.files || {}),
      });
    } catch (e) {
      console.error("❌ /disparar erro:", e);
      return res.status(500).json({ ok: false, error: e?.message || String(e) });
    }
  }
);

// =====================================================================================
// Campaign status/pause/resume/cancel
// =====================================================================================
app.get("/campaign/:id", async (req, res) => {
  try {
    const id = req.params.id;
    const meta = await getCampaignMeta(id);
    if (!meta || Object.keys(meta).length === 0) {
      return res.status(404).json({ ok: false, error: "Campaign não encontrada." });
    }
    return res.json({ ok: true, campaignId: id, ...meta });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/campaign/:id/pause", async (req, res) => {
  try {
    const id = req.params.id;
    await redis.hset(campaignKey(id), {
      paused: "1",
      pausedAt: new Date().toISOString(),
    });
    return res.json({ ok: true, campaignId: id, paused: true });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/campaign/:id/resume", async (req, res) => {
  try {
    const id = req.params.id;
    await redis.hset(campaignKey(id), {
      paused: "0",
      resumedAt: new Date().toISOString(),
    });
    return res.json({ ok: true, campaignId: id, paused: false });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// ✅ CANCEL REAL (B): mata tudo e impede envios futuros
app.post("/campaign/:id/cancel", async (req, res) => {
  try {
    const id = req.params.id;

    // marca cancelado e despausa (pra não ficar preso no loop)
    await redis.hset(campaignKey(id), {
      canceled: "1",
      canceledAt: new Date().toISOString(),
      paused: "0",
    });

    // remove jobs pendentes (waiting/delayed/paused)
    const states = ["waiting", "delayed", "paused"];
    let removed = 0;

    for (const st of states) {
      // 100k cobre 30k tranquilo
      const jobs = await queue.getJobs([st], 0, 100000);
      for (const job of jobs) {
        if (job?.data?.campaignId === id) {
          try {
            await job.remove();
            removed++;
          } catch {}
        }
      }
    }

    return res.json({
      ok: true,
      campaignId: id,
      canceled: true,
      removedPendingJobs: removed,
    });
  } catch (e) {
    console.error("❌ /campaign/:id/cancel erro:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// =====================================================================================
// Erros do Multer (ex: Unexpected field)
// =====================================================================================
app.use((err, req, res, next) => {
  if (err && err.name === "MulterError") {
    console.error("❌ MulterError:", err);
    return res.status(400).json({ ok: false, error: `Upload inválido: ${err.message}` });
  }
  return next(err);
});

// ===== Start =====
const PORT = Number(process.env.PORT) || 3000;
app.listen(PORT, () => console.log(`✅ API on :${PORT} | queue=${QUEUE_NAME}`));
