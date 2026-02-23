// worker.js
require("dotenv").config();
const { Worker } = require("bullmq");
const TelegramBot = require("node-telegram-bot-api");
const { redis, connection } = require("./redis");

const QUEUE_NAME = process.env.QUEUE_NAME || "disparos";
const DEFAULT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;

if (!DEFAULT_TOKEN) {
  console.warn("⚠️ TELEGRAM_BOT_TOKEN não definido (ok se sempre mandar botToken no job).");
}

process.on("unhandledRejection", (err) => console.error("❌ unhandledRejection:", err));
process.on("uncaughtException", (err) => console.error("❌ uncaughtException:", err));

// cache de bots por token (evita criar 26k instâncias)
const botCache = new Map();
function getBot(token) {
  const t = token || DEFAULT_TOKEN;
  if (!t) throw new Error("botToken ausente");
  if (botCache.has(t)) return botCache.get(t);
  const bot = new TelegramBot(t, { polling: false });
  botCache.set(t, bot);
  return bot;
}

function campaignKey(id) {
  return `campaign:${id}`;
}

async function isCampaignPaused(campaignId) {
  if (!campaignId) return false;
  const v = await redis.hget(campaignKey(campaignId), "paused");
  return String(v || "0") === "1";
}

// ✅ cancel real
async function isCampaignCanceled(campaignId) {
  if (!campaignId) return false;
  const v = await redis.hget(campaignKey(campaignId), "canceled");
  return String(v || "0") === "1";
}

async function incCampaign(campaignId, field, by = 1) {
  if (!campaignId) return;
  await redis.hincrby(campaignKey(campaignId), field, by);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// =====================
// Error helpers
// =====================
function getTelegramStatusCode(err) {
  return err?.response?.statusCode;
}

function getTelegramDescription(err) {
  const body = err?.response?.body;
  return String(body?.description || err?.message || "");
}

// Extract retry_after (seconds) from Telegram 429.
// node-telegram-bot-api normalmente traz:
// err.response.body.parameters.retry_after
function getRetryAfterSeconds(err) {
  const body = err?.response?.body;
  const p = body?.parameters;
  const ra1 = Number(p?.retry_after);
  if (Number.isFinite(ra1) && ra1 > 0) return ra1;

  const desc = getTelegramDescription(err);
  // Ex: "Too Many Requests: retry after 10"
  const m = desc.match(/retry after\s+(\d+)/i);
  if (m) {
    const ra2 = Number(m[1]);
    if (Number.isFinite(ra2) && ra2 > 0) return ra2;
  }
  return null;
}

function is429(err) {
  return getTelegramStatusCode(err) === 429;
}

// Classifica erro "permanente"
function isPermanentTelegramError(err) {
  const code = err?.code;
  const statusCode = getTelegramStatusCode(err);
  const desc = getTelegramDescription(err).toLowerCase();

  if (statusCode === 403) return true;

  if (statusCode === 400) {
    if (
      desc.includes("chat not found") ||
      desc.includes("user is deactivated") ||
      desc.includes("bot was blocked") ||
      desc.includes("bot is not a member") ||
      desc.includes("wrong file identifier") ||
      desc.includes("wrong remote file identifier") ||
      desc.includes("file is too big") ||
      desc.includes("bad request")
    ) return true;
  }

  if (statusCode === 401) return true;

  // rede geralmente NÃO é permanente
  if (code === "ETIMEDOUT" || code === "ECONNRESET") return false;

  return false;
}

function isRetryableTransient(err) {
  const code = err?.code;
  const sc = getTelegramStatusCode(err);

  if (sc === 429) return true; // rate limit
  if (sc >= 500 && sc <= 599) return true; // telegram/infra
  if (code === "ETIMEDOUT" || code === "ECONNRESET") return true;

  return false;
}

// =====================
// Throttled logs
// =====================
const stats = {
  startedAt: Date.now(),
  processed: 0,
  sent: 0,
  failed: 0,
  canceled: 0,

  // retries
  retry429: 0,
  retryOther: 0,

  lastLogAt: 0,

  // para log resumido de 429
  last429LogAt: 0,
  last429Count: 0,
  last429Sec: 0,
};

function maybeLogProgress() {
  const now = Date.now();
  const elapsed = Math.max(1, Math.floor((now - stats.startedAt) / 1000));
  const everyN = 250;

  if (stats.processed % everyN === 0 || now - stats.lastLogAt > 15000) {
    stats.lastLogAt = now;
    const rps = (stats.processed / elapsed).toFixed(2);
    console.log(
      `📊 progress: processed=${stats.processed} sent=${stats.sent} failed=${stats.failed} canceled=${stats.canceled} retry429=${stats.retry429} retryOther=${stats.retryOther} avg=${rps}/s`
    );
  }

  // log resumido de 429 (no máximo a cada 10s)
  if (stats.last429Count > 0 && now - stats.last429LogAt > 10000) {
    stats.last429LogAt = now;
    console.warn(
      `⏳ 429 em lote: +${stats.last429Count} (último retry_after=${stats.last429Sec || "?"}s) nos últimos ~10s`
    );
    stats.last429Count = 0;
    stats.last429Sec = 0;
  }
}

async function waitIfPaused(campaignId) {
  if (!campaignId) return;
  while (await isCampaignPaused(campaignId)) {
    // ✅ se cancelou enquanto estava pausado, sai imediatamente
    if (await isCampaignCanceled(campaignId)) return;
    await sleep(1500);
  }
}

// ✅ Se for URL (http/https): baixa e manda como Buffer.
// ✅ Se não for URL: assume que é file_id do Telegram e manda direto.
async function inputFromUrlOrId(fileUrl) {
  const s = String(fileUrl || "").trim();
  if (!s) return null;

  if (!/^https?:\/\//i.test(s)) {
    return s; // file_id
  }

  const r = await fetch(s);
  if (!r.ok) {
    throw new Error(`Falha ao baixar arquivo (${r.status}) ${s}`);
  }
  const buf = Buffer.from(await r.arrayBuffer());
  return buf;
}

async function sendTelegram(bot, payload) {
  const campaignId = payload.campaignId;

  // ✅ trava final: se cancelado, não envia
  if (campaignId && (await isCampaignCanceled(campaignId))) {
    return { ok: true, canceled: true };
  }

  const chatId = payload.chatId;
  const text = payload.text;
  const caption = payload.caption;
  const fileType = String(payload.fileType || "").toLowerCase();
  const fileUrl = String(payload.fileUrl || "").trim();

  if (!chatId) throw new Error("chatId ausente");

  // Texto puro
  if (!fileUrl) {
    if (!text) throw new Error("text ausente");
    return bot.sendMessage(chatId, text);
  }

  const input = await inputFromUrlOrId(fileUrl);
  if (!input) throw new Error("fileUrl inválida/vazia");

  if (fileType === "photo") return bot.sendPhoto(chatId, input, caption ? { caption } : undefined);
  if (fileType === "video") return bot.sendVideo(chatId, input, caption ? { caption } : undefined);
  if (fileType === "document") return bot.sendDocument(chatId, input, caption ? { caption } : undefined);
  if (fileType === "audio") return bot.sendAudio(chatId, input, caption ? { caption } : undefined);
  if (fileType === "voice") return bot.sendVoice(chatId, input, caption ? { caption } : undefined);
  if (fileType === "video_note") return bot.sendVideoNote(chatId, input);

  return bot.sendDocument(chatId, input, caption ? { caption } : undefined);
}

// =====================
// Worker
// =====================
const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    const data = job.data || {};
    const campaignId = data.campaignId;

    // ✅ pausa (mas sai se cancelou)
    await waitIfPaused(campaignId);

    // ✅ cancel real: não processa nem envia, mesmo se o worker já pegou o job
    if (await isCampaignCanceled(campaignId)) {
      stats.processed++;
      stats.canceled++;
      maybeLogProgress();

      await incCampaign(campaignId, "canceled");

      try {
        job.discard();
      } catch {}
      return { ok: true, canceled: true };
    }

    const bot = getBot(data.botToken);

    try {
      const r = await sendTelegram(bot, data);

      // se cancelou "no meio" (entre checks), sendTelegram retorna canceled:true
      if (r && r.canceled) {
        stats.processed++;
        stats.canceled++;
        maybeLogProgress();
        await incCampaign(campaignId, "canceled");
        try {
          job.discard();
        } catch {}
        return { ok: true, canceled: true };
      }

      stats.processed++;
      stats.sent++;
      maybeLogProgress();

      await incCampaign(campaignId, "sent");
      return { ok: true };
    } catch (err) {
      // ✅ tratamento especial 429: respeita retry_after e NÃO “flooda” falhas
      if (is429(err)) {
        const ra = getRetryAfterSeconds(err) || 3;
        // jitterzinho pra não sincronizar tudo
        const jitterMs = Math.floor(Math.random() * 350);
        await sleep(ra * 1000 + jitterMs);

        stats.processed++;
        stats.retry429++;
        stats.last429Count++;
        stats.last429Sec = ra;
        maybeLogProgress();

        // opcional: contabiliza retries no Redis (não quebra nada se não existir)
        await incCampaign(campaignId, "retry429");

        // rethrow pra BullMQ aplicar attempts/backoff
        throw err;
      }

      // outros transitórios (rede/5xx): também não contam como "failed" definitivo
      if (isRetryableTransient(err) && !isPermanentTelegramError(err)) {
        stats.processed++;
        stats.retryOther++;
        maybeLogProgress();

        await incCampaign(campaignId, "retryOther");
        throw err;
      }

      // falha “de verdade” (permanente ou não-classificada)
      stats.processed++;
      stats.failed++;
      maybeLogProgress();

      await incCampaign(campaignId, "failed");

      if (isPermanentTelegramError(err)) {
        try {
          job.discard();
        } catch {}
      }

      throw err;
    }
  },
  {
    connection,
    concurrency: Number(process.env.WORKER_CONCURRENCY) || 10,
    lockDuration: Number(process.env.WORKER_LOCK_MS) || 10 * 60 * 1000,
    autorun: true,
  }
);

worker.on("ready", () => console.log(`✅ Worker ready | queue=${QUEUE_NAME}`));
worker.on("error", (err) => console.error("❌ Worker error:", err?.message || err));

// ✅ evita flood de log: não loga 429 por job (a gente já loga resumo a cada ~10s)
worker.on("failed", (job, err) => {
  const sc = getTelegramStatusCode(err);

  if (sc === 429) {
    // ignora aqui (resumo sai no maybeLogProgress)
    return;
  }

  const id = job?.id;
  const desc = getTelegramDescription(err);
  console.warn(`💥 failed job=${id} status=${sc || "-"} desc=${desc || err?.message || err}`);
});
