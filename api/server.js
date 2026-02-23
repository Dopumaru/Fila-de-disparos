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
app.use(express.json({ limit: "8mb" })); // configs em JSON
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

const upload = multer({
  dest: UPLOAD_DIR || DEFAULT_UPLOAD_DIR,
  limits: { fileSize: 60 * 1024 * 1024 }, // 60MB
});

// ===== Queue =====
const QUEUE_NAME = process.env.QUEUE_NAME || "disparos";

const queue = new Queue(QUEUE_NAME, {
  connection,
  defaultJobOptions: {
    removeOnComplete: 2000,
    removeOnFail: 5000,
  },
});

// ===== Helpers =====
function isHttpUrl(u) {
  return /^https?:\/\//i.test(String(u || "").trim());
}

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

  const firstCell = lines[0].split(",")[0].trim();
  const startIdx = /[a-zA-Z]/.test(firstCell) ? 1 : 0;

  const out = [];
  for (let i = startIdx; i < lines.length; i++) {
    const id = lines[i].split(",")[0].trim();
    if (id) out.push({ id });
  }
  return out;
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

    const rps = Math.max(1, Math.min(30, Number(ratePerSecond) || 10));
    const baseDelayMs = Math.floor(1000 / rps);

    await setCampaignMeta(id, {
      paused: "0",
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
          removeOnComplete: true,
          removeOnFail: false,
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
// ✅ COMPAT COM TEU PAINEL: /disparar (multipart/form-data + CSV upload)
// =====================================================================================
app.post(
  "/disparar",
  upload.fields([
    { name: "csv", maxCount: 1 },
    { name: "upload", maxCount: 1 }, // arquivo de mídia do painel
  ]),
  async (req, res) => {
    try {
      const botToken =
        req.body.botToken ||
        req.body.token ||
        req.body.bot ||
        req.body.bot_token ||
        process.env.TELEGRAM_BOT_TOKEN;

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

      const tipo = String(req.body.tipo || req.body.type || "text").toLowerCase();

      const caption =
        req.body.caption ||
        req.body.legenda ||
        req.body.captionText ||
        req.body.caption_text ||
        "";

      const limit = Number(req.body.limit || req.body.rate || 1);
      const intervalSec = Number(req.body.intervalSec || req.body.interval || 1);

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

      // pega CSV
      const csvFile = req.files?.csv?.[0];
      if (!csvFile?.path) {
        return res.status(400).json({ ok: false, error: "CSV não enviado (campo 'csv')." });
      }

      const csvText = fs.readFileSync(csvFile.path, "utf8");
      try { fs.unlinkSync(csvFile.path); } catch {}

      const leads = parseCsvFirstColumnAsIds(csvText);
      if (leads.length === 0) {
        return res.status(400).json({ ok: false, error: "Nenhum ID válido no CSV." });
      }

      // pega mídia (se tiver)
      const mediaFile = req.files?.upload?.[0];

      // Se painel mandou arquivo, geramos uma URL interna acessível pro worker
      // (Importante: o worker precisa conseguir acessar a API por host interno)
      let fileUrl =
        req.body.fileUrl || req.body.file_url || req.body.arquivoUrl || "";

      if (!fileUrl && mediaFile?.filename) {
        const apiHost =
          process.env.API_INTERNAL_HOST || process.env.API_HOST || "api-disparos";
        const port = process.env.PORT ? Number(process.env.PORT) : 80;

        // EasyPanel normalmente expõe a app na 80 internamente.
        // URL interna para o worker baixar o arquivo:
        fileUrl = `http://${apiHost}:${port}/uploads/${mediaFile.filename}`;
      }

      // validações
      if (!fileUrl && tipo !== "text" && tipo !== "none") {
        return res.status(400).json({
          ok: false,
          error: "Tipo de mídia selecionado, mas nenhum arquivo/URL foi enviado.",
        });
      }

      if (!fileUrl && !String(message || "").trim()) {
        return res.status(400).json({
          ok: false,
          error: "Mensagem vazia. Preencha 'Mensagem / Legenda' para tipo Texto.",
        });
      }

      const campaignId = genId();

      await setCampaignMeta(campaignId, {
        paused: "0",
        total: String(leads.length),
        sent: "0",
        failed: "0",
        createdAt: new Date().toISOString(),
        ratePerSecond: String(ratePerSecond),
      });

      const baseDelayMs = Math.floor(1000 / ratePerSecond);

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
          removeOnComplete: true,
          removeOnFail: false,
        },
      }));

      await queue.addBulk(jobs);

      return res.json({
        ok: true,
        campaignId,
        total: leads.length,
        ratePerSecond,
        usedFileUrl: !!fileUrl,
      });
    } catch (e) {
      console.error("❌ /disparar erro:", e);
      return res.status(500).json({ ok: false, error: e?.message || String(e) });
    }
  }
);

    // ✅ Aqui está o fix do "text ausente": aceita vários nomes que o front pode enviar
    const message =
      req.body.message ||
      req.body.mensagem ||
      req.body.msg ||
      req.body.text ||
      req.body.texto ||
      req.body.messageText ||
      req.body.message_text ||
      req.body.legenda ||
      req.body.caption || // caso painel use o mesmo campo
      "";

    // Tipo do envio
    const tipo = String(req.body.tipo || req.body.type || "text").toLowerCase();

    // Arquivo (se existir)
    const fileUrl =
      req.body.fileUrl || req.body.file_url || req.body.arquivoUrl || "";

    const caption =
      req.body.caption ||
      req.body.legenda ||
      req.body.captionText ||
      req.body.caption_text ||
      "";

    // Rate do painel: "limit" por intervalo e intervalo em segundos
    const limit = Number(req.body.limit || req.body.rate || 1);
    const intervalSec = Number(req.body.intervalSec || req.body.interval || 1);

    // Se for 1 a cada 1s => 1 rps
    // Se for 2 a cada 1s => 2 rps
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

    if (!req.file?.path) {
      return res.status(400).json({ ok: false, error: "CSV não enviado (campo 'csv')." });
    }

    const csvText = fs.readFileSync(req.file.path, "utf8");
    try {
      fs.unlinkSync(req.file.path);
    } catch {}

    const leads = parseCsvFirstColumnAsIds(csvText);

    if (leads.length === 0) {
      return res.status(400).json({ ok: false, error: "Nenhum ID válido no CSV." });
    }

    // Se for envio de texto puro, precisa ter message
    if (!fileUrl && !String(message || "").trim()) {
      return res.status(400).json({
        ok: false,
        error: "Mensagem vazia. Preencha 'Mensagem / Legenda' para tipo Texto.",
      });
    }

    const campaignId = genId();

    await setCampaignMeta(campaignId, {
      paused: "0",
      total: String(leads.length),
      sent: "0",
      failed: "0",
      createdAt: new Date().toISOString(),
      ratePerSecond: String(ratePerSecond),
    });

    const baseDelayMs = Math.floor(1000 / ratePerSecond);

    const jobs = leads.map((row, idx) => ({
      name: "send",
      data: {
        campaignId,
        botToken,
        chatId: row.id,
        text: message, // ✅ sempre cai aqui (fix do worker)
        fileUrl: fileUrl || undefined,
        fileType: tipo === "text" ? undefined : tipo,
        caption: caption || undefined,
      },
      opts: {
        delay: idx * baseDelayMs,
        attempts: 6,
        backoff: { type: "exponential", delay: 2000 },
        removeOnComplete: true,
        removeOnFail: false,
      },
    }));

    await queue.addBulk(jobs);

    return res.json({
      ok: true,
      campaignId,
      total: leads.length,
      ratePerSecond,
    });
  } catch (e) {
    console.error("❌ /disparar erro:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// =====================================================================================
// Campaign status/pause/resume
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
    await redis.hset(campaignKey(id), { paused: "1", pausedAt: new Date().toISOString() });
    return res.json({ ok: true, campaignId: id, paused: true });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/campaign/:id/resume", async (req, res) => {
  try {
    const id = req.params.id;
    await redis.hset(campaignKey(id), { paused: "0", resumedAt: new Date().toISOString() });
    return res.json({ ok: true, campaignId: id, paused: false });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// ===== Start =====
const PORT = Number(process.env.PORT) || 3000;
app.listen(PORT, () => console.log(`✅ API on :${PORT} | queue=${QUEUE_NAME}`));
