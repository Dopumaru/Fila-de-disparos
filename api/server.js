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
const upload = multer({
  dest: uploadsDirToServe,
  limits: { fileSize: 60 * 1024 * 1024 }, // 60MB
});

// ===== Queue =====
const QUEUE_NAME = process.env.QUEUE_NAME || "disparos";

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

function toInt(n, def) {
  const v = Number(n);
  return Number.isFinite(v) ? v : def;
}

function getPublicBaseUrl(req) {
  const envBase = String(process.env.PUBLIC_BASE_URL || "").trim();
  if (envBase) return envBase.replace(/\/+$/, "");

  const proto = req.headers["x-forwarded-proto"] || req.protocol || "http";
  const host = req.headers["x-forwarded-host"] || req.get("host");
  return `${proto}://${host}`.replace(/\/+$/, "");
}

function clampRps(n) {
  return Math.max(1, Math.min(30, toInt(n, 10)));
}

function parseButtons(raw) {
  if (!raw) return [];
  try {
    const v = typeof raw === "string" ? JSON.parse(raw) : raw;
    if (!Array.isArray(v)) return [];
    return v
      .map((b) => ({
        text: String(b?.text || "").trim(),
        type: String(b?.type || "").trim().toLowerCase(), // url | start
        value: String(b?.value || "").trim(),
      }))
      .filter((b) => b.text && b.value && (b.type === "url" || b.type === "start"))
      .slice(0, 4);
  } catch {
    return [];
  }
}

// ===== ✅ Rate realtime (sem delay no job) =====
// A ideia: enfileira tudo SEM delay e o worker aplica o ritmo lendo ratePerSecond em tempo real.

// Aqui no server, a gente só guarda o rate na meta e NÃO calcula delay por idx.

// ===== upload url validator (opcional) =====
async function validatePublicUrl(url) {
  // leve e sem lib extra: só valida forma básica e deixa o worker lidar com falhas reais.
  const s = String(url || "").trim();
  if (!s) return false;
  return /^https?:\/\//i.test(s);
}

function buildAssetKey(campaignId, fileType) {
  const t = String(fileType || "file").toLowerCase();
  return `campaign:${campaignId}:asset:${t}:v1`;
}

// ===== Enqueue central (usado por /campaign e /disparar) =====
async function enqueueCampaign({
  req,
  campaignId,
  leads,
  botToken,
  chatIdColumn = "id",
  ratePerSecond = 10,
  message,
  fileUrl,
  fileType,
  caption,
  buttons,
}) {
  const id = campaignId || genId();
  const rps = clampRps(ratePerSecond);

  await setCampaignMeta(id, {
    paused: "0",
    canceled: "0",
    total: String(leads.length),
    sent: "0",
    failed: "0",
    canceledCount: "0",
    createdAt: new Date().toISOString(),
    ratePerSecond: String(rps),
  });

  const idCol = normalizeKey(chatIdColumn);
  const btns = parseButtons(buttons);

  // ✅ assetKey para cache de file_id no worker (principalmente vídeo)
  const assetKey = fileUrl ? buildAssetKey(id, fileType) : undefined;

  const jobs = leads.map((row) => {
    const chatId = row?.[idCol] ?? row?.[chatIdColumn] ?? row?.id ?? row?.chat_id;

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
        buttons: btns,
        assetKey,
      },
      opts: {
        // ✅ SEM delay: rate realtime fica no worker
        attempts: 6,
        backoff: { type: "exponential", delay: 2000 },
      },
    };
  });

  await queue.addBulk(jobs);

  return { campaignId: id, total: leads.length, ratePerSecond: rps, buttons: btns.length, assetKey: !!assetKey };
}

// =====================================================================================
// JSON: /campaign
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
      buttons,
    } = req.body || {};

    if (!Array.isArray(leads) || leads.length === 0) {
      return res.status(400).json({ ok: false, error: "Leads vazio." });
    }
    if (!botToken) {
      return res.status(400).json({ ok: false, error: "botToken ausente." });
    }

    if (fileUrl) {
      const ok = await validatePublicUrl(fileUrl);
      if (!ok) return res.status(400).json({ ok: false, error: "fileUrl inválida (precisa ser http/https público)." });
    }

    const r = await enqueueCampaign({
      req,
      campaignId,
      leads,
      botToken,
      chatIdColumn,
      ratePerSecond,
      message,
      fileUrl,
      fileType,
      caption,
      buttons,
    });

    return res.json({ ok: true, ...r });
  } catch (e) {
    console.error("❌ /campaign erro:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// =====================================================================================
// multipart: /disparar
// =====================================================================================
app.post(
  "/disparar",
  upload.fields([
    { name: "csv", maxCount: 1 },
    { name: "file", maxCount: 1 },
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

      const limit = toInt(req.body.limit || req.body.rate || req.body.limitMax || 1, 1);
      const intervalSec = toInt(req.body.intervalSec || req.body.interval || req.body.intervalS || 1, 1);
      const ratePerSecond = clampRps(Math.floor(limit / Math.max(1, intervalSec)) || 1);

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

      const csvText = fs.readFileSync(csvFile.path, "utf8");
      try {
        fs.unlinkSync(csvFile.path);
      } catch {}

      const leads = parseCsvFirstColumnAsIds(csvText);
      if (leads.length === 0) {
        return res.status(400).json({ ok: false, error: "Nenhum ID válido no CSV." });
      }

      const mediaFile = req.files?.file?.[0];

      const caption =
        req.body.caption ||
        req.body.legenda ||
        req.body.captionText ||
        req.body.caption_text ||
        "";

      const hasMedia = !!mediaFile;
      if (!hasMedia && !String(message || "").trim()) {
        return res.status(400).json({
          ok: false,
          error: "Mensagem vazia. Preencha 'Mensagem / Legenda' para tipo Texto.",
        });
      }

      const campaignId = genId();

      let fileUrl;
      if (mediaFile?.filename) {
        const base = getPublicBaseUrl(req);
        fileUrl = `${base}/uploads/${mediaFile.filename}`;
      }

      const btns = parseButtons(req.body.buttons);

      const r = await enqueueCampaign({
        req,
        campaignId,
        leads,
        botToken,
        chatIdColumn: "id",
        ratePerSecond,
        message,
        fileUrl: fileUrl || undefined,
        fileType: tipo === "text" ? undefined : tipo,
        caption: caption || undefined,
        buttons: btns,
      });

      return res.json({
        ok: true,
        ...r,
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
// status/pause/resume/rate/cancel
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

app.post("/campaign/:id/rate", async (req, res) => {
  try {
    const id = req.params.id;
    const meta = await getCampaignMeta(id);
    if (!meta || Object.keys(meta).length === 0) {
      return res.status(404).json({ ok: false, error: "Campaign não encontrada." });
    }
    if (String(meta.canceled || "0") === "1") {
      return res.status(400).json({ ok: false, error: "Campaign já está cancelada." });
    }

    const raw =
      req.body?.ratePerSecond ??
      req.body?.rate ??
      req.body?.rps ??
      req.query?.ratePerSecond ??
      req.query?.rate ??
      req.query?.rps;

    const rps = clampRps(raw);

    await redis.hset(campaignKey(id), {
      ratePerSecond: String(rps),
      rateUpdatedAt: new Date().toISOString(),
    });
    return res.json({ ok: true, campaignId: id, ratePerSecond: rps });
  } catch (e) {
    console.error("❌ /campaign/:id/rate erro:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/campaign/:id/cancel", async (req, res) => {
  try {
    const id = req.params.id;

    await redis.hset(campaignKey(id), {
      canceled: "1",
      canceledAt: new Date().toISOString(),
      paused: "0",
    });

    // Varredura paginada pra não travar em filas grandes
    const CANCEL_SCAN_MAX = Number(process.env.CANCEL_SCAN_MAX) || 20000;
    const PAGE_SIZE = Number(process.env.CANCEL_SCAN_PAGE_SIZE) || 500;

    const states = ["waiting", "delayed", "paused"];
    let removed = 0;

    for (const st of states) {
      let start = 0;

      while (start < CANCEL_SCAN_MAX) {
        const end = Math.min(start + PAGE_SIZE - 1, CANCEL_SCAN_MAX - 1);
        const jobs = await queue.getJobs([st], start, end);
        if (!jobs || jobs.length === 0) break;

        for (const job of jobs) {
          if (job?.data?.campaignId === id) {
            try {
              await job.remove();
              removed++;
            } catch {}
          }
        }

        if (jobs.length < PAGE_SIZE) break;
        start += PAGE_SIZE;
      }
    }

    return res.json({
      ok: true,
      campaignId: id,
      canceled: true,
      removedPendingJobs: removed,
      scanMax: CANCEL_SCAN_MAX,
      pageSize: PAGE_SIZE,
    });
  } catch (e) {
    console.error("❌ /campaign/:id/cancel erro:", e);
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// =====================================================================================
// Multer errors
// =====================================================================================
app.use((err, req, res, next) => {
  if (err && err.name === "MulterError") {
    console.error("❌ MulterError:", err);
    return res.status(400).json({ ok: false, error: `Upload inválido: ${err.message}` });
  }
  return next(err);
});

// =====================================================================================
// Uploads Cleanup (TTL)
// =====================================================================================
const UPLOAD_TTL_HOURS = Number(process.env.UPLOAD_TTL_HOURS) || 24; // padrão: 24h
const UPLOAD_CLEAN_INTERVAL_MIN = Number(process.env.UPLOAD_CLEAN_INTERVAL_MIN) || 60; // roda 1x/h

function cleanupUploads() {
  try {
    const dir = uploadsDirToServe;
    if (!dir || !fs.existsSync(dir)) return;

    const files = fs.readdirSync(dir);
    const now = Date.now();
    const ttlMs = UPLOAD_TTL_HOURS * 60 * 60 * 1000;

    let removed = 0;

    for (const file of files) {
      const fullPath = path.join(dir, file);

      try {
        const stat = fs.statSync(fullPath);
        if (!stat.isFile()) continue;

        const age = now - stat.mtimeMs;
        if (age > ttlMs) {
          fs.unlinkSync(fullPath);
          removed++;
        }
      } catch {}
    }

    if (removed > 0) {
      console.log(`🧹 Upload cleanup: ${removed} arquivo(s) removido(s) | TTL=${UPLOAD_TTL_HOURS}h`);
    }
  } catch (e) {
    console.error("❌ Erro no cleanup de uploads:", e?.message || String(e));
  }
}

setTimeout(cleanupUploads, 10_000);
setInterval(cleanupUploads, UPLOAD_CLEAN_INTERVAL_MIN * 60 * 1000);

// ===== Start =====
const PORT = Number(process.env.PORT) || 3000;
app.listen(PORT, () => console.log(`✅ API on :${PORT} | queue=${QUEUE_NAME}`));
