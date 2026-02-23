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

async function incCampaign(campaignId, field) {
  if (!campaignId) return;
  await redis.hincrby(campaignKey(campaignId), field, 1);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// Classifica erro "permanente" (não adianta retry infinito)
// Telegram: bot foi bloqueado, chat inválido, etc.
function isPermanentTelegramError(err) {
  const code = err?.code;
  const statusCode = err?.response?.statusCode;
  const body = err?.response?.body;
  const desc = String(body?.description || err?.message || "").toLowerCase();

  // 403: Forbidden (blocked by the user / bot kicked)
  if (statusCode === 403) return true;

  // 400: Bad Request (chat not found, wrong file id, etc.) — muitas vezes permanente
  if (statusCode === 400) {
    if (
      desc.includes("chat not found") ||
      desc.includes("user is deactivated") ||
      desc.includes("bot was blocked") ||
      desc.includes("bot is not a member") ||
      desc.includes("wrong file identifier") ||
      desc.includes("file is too big")
    ) return true;
  }

  // 401: Unauthorized (token inválido) — permanente
  if (statusCode === 401) return true;

  // ioredis / network geralmente NÃO é permanente
  if (code === "ETIMEDOUT" || code === "ECONNRESET") return false;

  return false;
}

// Logs “throttled” (não explode em 26k)
const stats = {
  startedAt: Date.now(),
  processed: 0,
  sent: 0,
  failed: 0,
  lastLogAt: 0,
};

function maybeLogProgress() {
  const now = Date.now();
  const elapsed = Math.max(1, Math.floor((now - stats.startedAt) / 1000));
  const everyN = 250; // log a cada 250 jobs

  if (stats.processed % everyN === 0 || now - stats.lastLogAt > 15000) {
    stats.lastLogAt = now;
    const rps = (stats.processed / elapsed).toFixed(2);
    console.log(
      `📊 progress: processed=${stats.processed} sent=${stats.sent} failed=${stats.failed} avg=${rps}/s`
    );
  }
}

async function waitIfPaused(campaignId) {
  if (!campaignId) return;

  // Se pausar, fica esperando SEM log por job.
  // Para não travar o lock, usamos lockDuration alto no worker config.
  while (await isCampaignPaused(campaignId)) {
    await sleep(1500);
  }
}

async function sendTelegram(bot, payload) {
  const { chatId, text, fileType, fileUrl, caption } = payload;

  if (!chatId) throw new Error("chatId ausente");

  // Se não tem arquivo, manda mensagem
  if (!fileUrl) {
    if (!text) throw new Error("text ausente");
    return bot.sendMessage(chatId, text);
  }

  // Se tiver fileUrl, roteia pelo tipo
  const type = String(fileType || "").toLowerCase();

  if (type === "photo") return bot.sendPhoto(chatId, fileUrl, caption ? { caption } : undefined);
  if (type === "video") return bot.sendVideo(chatId, fileUrl, caption ? { caption } : undefined);
  if (type === "document") return bot.sendDocument(chatId, fileUrl, caption ? { caption } : undefined);
  if (type === "audio") return bot.sendAudio(chatId, fileUrl, caption ? { caption } : undefined);
  if (type === "voice") return bot.sendVoice(chatId, fileUrl, caption ? { caption } : undefined);
  if (type === "video_note") return bot.sendVideoNote(chatId, fileUrl);

  // fallback (document)
  return bot.sendDocument(chatId, fileUrl, caption ? { caption } : undefined);
}

// Worker
const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    const data = job.data || {};
    const campaignId = data.campaignId;

    // ✅ PAUSE REAL (sem flood)
    await waitIfPaused(campaignId);

    const bot = getBot(data.botToken);

    try {
      await sendTelegram(bot, data);

      stats.processed++;
      stats.sent++;
      maybeLogProgress();

      await incCampaign(campaignId, "sent");
      return { ok: true };
    } catch (err) {
      stats.processed++;
      stats.failed++;
      maybeLogProgress();

      await incCampaign(campaignId, "failed");

      // Se é erro permanente, não faz retry (evita inferno em 26k)
      if (isPermanentTelegramError(err)) {
        // discard evita novas tentativas dentro do attempts
        try { job.discard(); } catch {}
      }

      throw err;
    }
  },
  {
    connection,
    concurrency: Number(process.env.WORKER_CONCURRENCY) || 10,

    // Crucial pra pausa por espera: evita lock expirar enquanto aguarda
    lockDuration: Number(process.env.WORKER_LOCK_MS) || 10 * 60 * 1000, // 10 min

    // Evita “metralhar” reconexão
    autorun: true,
  }
);

worker.on("ready", () => console.log(`✅ Worker ready | queue=${QUEUE_NAME}`));
worker.on("error", (err) => console.error("❌ Worker error:", err?.message || err));
worker.on("failed", (job, err) => {
  // ⚠️ aqui loga só falhas (não cada sucesso)
  const id = job?.id;
  const sc = err?.response?.statusCode;
  const desc = err?.response?.body?.description;
  console.warn(`💥 failed job=${id} status=${sc || "-"} desc=${desc || err?.message || err}`);
});
