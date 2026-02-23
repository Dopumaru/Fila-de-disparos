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
app.use(express.json({ limit: "4mb" })); // json de config (não manda arquivo aqui)
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
  limits: { fileSize: 50 * 1024 * 1024 }, // 50MB (ajuste se precisar)
});

// ===== Compat: rota /disparar (front antigo) =====
app.post("/disparar", upload.single("csv"), async (req, res) => {
  try {
    // Campos que costumam vir do form do front
    const botToken = req.body.botToken || req.body.token || process.env.TELEGRAM_BOT_TOKEN;
    const message = req.body.message || req.body.text || "";
    const tipo = (req.body.tipo || req.body.type || "text").toLowerCase(); // text/photo/video/document/audio/voice/video_note
    const fileUrl = req.body.fileUrl || req.body.file_url || "";
    const caption = req.body.caption || "";

    // Rate do teu front: "limite por intervalo" e "intervalo"
    const limit = Number(req.body.limit || req.body.rate || 1);         // ex: 1
    const intervalSec = Number(req.body.intervalSec || req.body.interval || 1); // ex: 1 segundo
    const ratePerSecond = Math.max(1, Math.min(30, Math.floor(limit / Math.max(1, intervalSec)) || 1));

    if (!botToken) return res.status(400).json({ ok: false, error: "botToken ausente (env TELEGRAM_BOT_TOKEN ou body.botToken)" });
    if (!req.file?.path) return res.status(400).json({ ok: false, error: "CSV não enviado (campo 'csv')" });

    const csvText = fs.readFileSync(req.file.path, "utf8");
    try { fs.unlinkSync(req.file.path); } catch {}

    // Parse simples: primeira coluna é ID
    const lines = csvText
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter(Boolean);

    if (lines.length === 0) return res.status(400).json({ ok: false, error: "CSV vazio" });

    // Se primeira linha tiver letras, trata como header
    const first = lines[0].split(",")[0].trim();
    const startIdx = /[a-zA-Z]/.test(first) ? 1 : 0;

    const leads = [];
    for (let i = startIdx; i < lines.length; i++) {
      const id = lines[i].split(",")[0].trim();
      if (id) leads.push({ id });
    }

    if (leads.length === 0) return res.status(400).json({ ok: false, error: "Nenhum ID válido no CSV" });

    // Reaproveita lógica de /campaign (mesmo formato)
    const id = genId();
    await setCampaignMeta(id, {
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
        campaignId: id,
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

    return res.json({ ok: true, campaignId: id, total: leads.length, ratePerSecond });
  } catch (e) {
    console.error("❌ /disparar erro:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
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
  await redis.expire(key, 60 * 60 * 24 * 7); // 7 dias (ajuste)
}

async function getCampaignMeta(id) {
  const key = campaignKey(id);
  const data = await redis.hgetall(key);
  return data || {};
}

// ===== Rotas Campaign =====

// cria/enfileira (JSON) — mantém compatível com o front
app.post("/campaign", async (req, res) => {
  try {
    const {
      leads = [],
      botToken,
      chatIdColumn = "id",
      ratePerSecond = 10,
      message,
      fileUrl,
      fileType, // "photo" | "video" | "document" | "audio" | "voice" | "video_note"
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
    const key = campaignKey(id);

    // salva meta/status inicial
    await setCampaignMeta(id, {
      paused: "0",
      total: String(leads.length),
      sent: "0",
      failed: "0",
      createdAt: new Date().toISOString(),
      ratePerSecond: String(ratePerSecond || 10),
    });

    // enqueue em bulk (muito mais leve que add 26k vezes)
    // throttle via delay (ratePerSecond) – espalha jobs no tempo
    const rps = Math.max(1, Math.min(30, Number(ratePerSecond) || 10));
    const baseDelayMs = Math.floor(1000 / rps);

    const idCol = normalizeKey(chatIdColumn);

    const jobs = leads.map((row, idx) => {
      const chatId = row?.[idCol] ?? row?.[chatIdColumn] ?? row?.id ?? row?.chat_id;

      // template aplicado no texto/caption
      const textFinal = applyTemplate(message, row);
      const captionFinal = applyTemplate(caption, row);

      const delay = idx * baseDelayMs;

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
          delay,
          // NÃO coloca 999999. Isso vira inferno em erro permanente.
          attempts: 6,
          backoff: { type: "exponential", delay: 2000 },
          removeOnComplete: true,
          removeOnFail: false,
        },
      };
    });

    await queue.addBulk(jobs);

    return res.json({ ok: true, campaignId: id, total: leads.length, ratePerSecond: rps });
  } catch (e) {
    console.error("❌ /campaign erro:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// status
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

// pause
app.post("/campaign/:id/pause", async (req, res) => {
  try {
    const id = req.params.id;
    await redis.hset(campaignKey(id), { paused: "1", pausedAt: new Date().toISOString() });
    return res.json({ ok: true, campaignId: id, paused: true });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// resume
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
