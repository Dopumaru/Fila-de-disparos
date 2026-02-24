// worker.js
require("dotenv").config();
const { Worker } = require("bullmq");
const TelegramBot = require("node-telegram-bot-api");
const { redis, connection } = require("./redis");
const crypto = require("crypto");

const QUEUE_NAME = process.env.QUEUE_NAME || "disparos";
const DEFAULT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;

if (!DEFAULT_TOKEN) {
  console.warn("⚠️ TELEGRAM_BOT_TOKEN não definido (ok se sempre mandar botToken no job).");
}

process.on("unhandledRejection", (err) => console.error("❌ unhandledRejection:", err));
process.on("uncaughtException", (err) => console.error("❌ uncaughtException:", err));

/* =========================
   BOT CACHE
========================= */

const botCache = new Map();
function getBot(token) {
  const t = token || DEFAULT_TOKEN;
  if (!t) throw new Error("botToken ausente");
  if (botCache.has(t)) return botCache.get(t);
  const bot = new TelegramBot(t, { polling: false });
  botCache.set(t, bot);
  return bot;
}

const botUsernameCache = new Map();
async function getBotUsername(bot, tokenKey) {
  if (botUsernameCache.has(tokenKey)) return botUsernameCache.get(tokenKey);
  try {
    const me = await bot.getMe();
    const u = me?.username ? String(me.username) : "";
    botUsernameCache.set(tokenKey, u);
    return u;
  } catch {
    botUsernameCache.set(tokenKey, "");
    return "";
  }
}

/* =========================
   CAMPAIGN HELPERS
========================= */

function campaignKey(id) {
  return `campaign:${id}`;
}

async function isCampaignPaused(campaignId) {
  if (!campaignId) return false;
  const v = await redis.hget(campaignKey(campaignId), "paused");
  return String(v || "0") === "1";
}

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

/* =========================
   FILE_ID CACHE
========================= */

function mediaCacheKey(botToken, fileType, fileUrl) {
  const h = crypto
    .createHash("sha1")
    .update(`${botToken}|${fileType}|${fileUrl}`)
    .digest("hex");
  return `media:fileid:${h}`;
}

function extractFileId(fileType, msg) {
  if (!msg) return null;

  if (fileType === "video") return msg?.video?.file_id;
  if (fileType === "document") return msg?.document?.file_id;
  if (fileType === "audio") return msg?.audio?.file_id;
  if (fileType === "voice") return msg?.voice?.file_id;
  if (fileType === "video_note") return msg?.video_note?.file_id;

  if (fileType === "photo") {
    const arr = msg?.photo;
    if (Array.isArray(arr) && arr.length) {
      return arr[arr.length - 1]?.file_id;
    }
  }

  return null;
}

function normalizeFileType(ft, fileUrl) {
  const t = String(ft || "").toLowerCase().trim();

  if (["img","image","imagem","foto","photo","jpg","jpeg","png","webp"].includes(t)) return "photo";
  if (["vid","video","mp4","mov","mkv"].includes(t)) return "video";
  if (["voice","ogg","opus"].includes(t)) return "voice";
  if (["audio","mp3","wav","m4a"].includes(t)) return "audio";
  if (["video_note","videonote","round"].includes(t)) return "video_note";
  if (["doc","document","pdf","zip"].includes(t)) return "document";

  if (fileUrl) return "document";
  return "";
}

function isHttpUrl(s) {
  return /^https?:\/\//i.test(String(s || "").trim());
}

/* =========================
   TELEGRAM SEND
========================= */

async function sendTelegram(bot, tokenKey, payload) {
  const chatId = payload.chatId;
  const campaignId = payload.campaignId;

  if (!chatId) throw new Error("chatId ausente");

  if (campaignId && (await isCampaignCanceled(campaignId))) {
    return { canceled: true };
  }

  const text = payload.text;
  const caption = payload.caption;
  const fileUrl = String(payload.fileUrl || "").trim();
  const fileType = normalizeFileType(payload.fileType, fileUrl);

  if (!fileUrl) {
    if (!text) throw new Error("text ausente");
    return bot.sendMessage(chatId, text, { disable_web_page_preview: true });
  }

  let input = fileUrl;
  let cacheKey = null;

  if (isHttpUrl(fileUrl)) {
    cacheKey = mediaCacheKey(tokenKey, fileType, fileUrl);
    const cached = await redis.get(cacheKey);
    if (cached) {
      input = cached;
    }
  }

  let msg;

  if (fileType === "photo") msg = await bot.sendPhoto(chatId, input, { caption });
  else if (fileType === "video") msg = await bot.sendVideo(chatId, input, { caption });
  else if (fileType === "document") msg = await bot.sendDocument(chatId, input, { caption });
  else if (fileType === "audio") msg = await bot.sendAudio(chatId, input, { caption });
  else if (fileType === "voice") msg = await bot.sendVoice(chatId, input, { caption });
  else if (fileType === "video_note") msg = await bot.sendVideoNote(chatId, input);
  else msg = await bot.sendDocument(chatId, input, { caption });

  if (cacheKey && isHttpUrl(fileUrl)) {
    const fid = extractFileId(fileType, msg);
    if (fid) {
      await redis.set(cacheKey, fid, "EX", 60 * 60 * 24 * 30);
    }
  }

  return msg;
}

/* =========================
   WORKER
========================= */

const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    const data = job.data || {};
    const campaignId = data.campaignId;

    if (await isCampaignCanceled(campaignId)) {
      await incCampaign(campaignId, "canceledCount");
      return { ok: true, canceled: true };
    }

    const bot = getBot(data.botToken);
    const tokenKey = data.botToken || DEFAULT_TOKEN || "";

    try {
      await sendTelegram(bot, tokenKey, data);
      await incCampaign(campaignId, "sent");
      return { ok: true };
    } catch (err) {
      const sc = err?.response?.statusCode;

      if (sc === 429) {
        const ra = Number(err?.response?.body?.parameters?.retry_after) || 3;
        await sleep(ra * 1000);
        throw err;
      }

      await incCampaign(campaignId, "failed");
      throw err;
    }
  },
  {
    connection,
    concurrency: Number(process.env.WORKER_CONCURRENCY) || 2,
    lockDuration: Number(process.env.WORKER_LOCK_MS) || 10 * 60 * 1000,
    autorun: true,
  }
);

worker.on("ready", () => console.log(`✅ Worker ready | queue=${QUEUE_NAME}`));
worker.on("error", (err) => console.error("❌ Worker error:", err?.message || err));
