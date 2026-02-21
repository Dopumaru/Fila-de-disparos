// worker.js
require("dotenv").config();
const { Worker } = require("bullmq");
const TelegramBot = require("node-telegram-bot-api");
const connection = require("./redis");
const fs = require("fs");
const path = require("path");
const { pipeline } = require("stream/promises");
const { Readable } = require("stream");

const DEFAULT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;

if (!DEFAULT_TOKEN) {
  console.warn("⚠️ TELEGRAM_BOT_TOKEN não definido (ok se você sempre mandar botToken no job).");
}

// ===== Campaign helpers =====
function campaignKey(id) {
  return `campaign:${id}`;
}

async function isCampaignPaused(campaignId) {
  if (!campaignId) return false;
  const v = await connection.hget(campaignKey(campaignId), "paused");
  return String(v || "0") === "1";
}

async function incCampaignSent(campaignId) {
  if (!campaignId) return;
  await connection.hincrby(campaignKey(campaignId), "sent", 1);
}

async function incCampaignFailed(campaignId) {
  if (!campaignId) return;
  await connection.hincrby(campaignKey(campaignId), "failed", 1);
}

function shouldCountFinalFailure(job) {
  const maxAttempts = Number(job?.opts?.attempts ?? 1);
  const attemptIndex = Number(job?.attemptsMade ?? 0) + 1;
  return attemptIndex >= maxAttempts;
}

// ===== Controle de log de pausa (ANTI-SPAM) =====
const pauseLogControl = new Map();
function logPausedThrottled(campaignId) {
  const now = Date.now();
  const last = pauseLogControl.get(campaignId) || 0;

  if (now - last > 10000) {
    console.log(`⏸️ Campaign ${campaignId} pausada. Aguardando retomar...`);
    pauseLogControl.set(campaignId, now);
  }
}

// ===== Cache de bots =====
const botCache = new Map();
function getBot(token) {
  const t = token || DEFAULT_TOKEN;
  if (!t) throw new Error("Nenhum token disponível");
  if (botCache.has(t)) return botCache.get(t);
  const bot = new TelegramBot(t, { polling: false });
  botCache.set(t, bot);
  return bot;
}

function maskToken(t) {
  if (!t) return "";
  const s = String(t);
  if (s.length <= 10) return "***";
  return s.slice(0, 4) + "..." + s.slice(-4);
}

function isHttpUrl(u) {
  return /^https?:\/\//i.test(String(u || "").trim());
}

function safeUnlink(p) {
  try {
    if (p && fs.existsSync(p)) fs.unlinkSync(p);
  } catch {}
}

async function downloadUrlToTmp(url) {
  const u = String(url || "").trim();
  const urlObj = new URL(u);
  let ext = path.extname(urlObj.pathname || "");
  if (!ext) ext = ".bin";

  const tmpPath = path.join(
    "/tmp",
    `tg-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}${ext}`
  );

  const r = await fetch(u);
  if (!r.ok) throw new Error(`Falha ao baixar arquivo (${r.status})`);
  const body = r.body;
  if (!body) throw new Error("Resposta sem body");

  const nodeStream = Readable.fromWeb(body);
  await pipeline(nodeStream, fs.createWriteStream(tmpPath));
  return tmpPath;
}

async function resolveTelegramInput(file) {
  if (!file) throw new Error("payload.file não foi enviado");
  if (typeof file !== "string") throw new Error("payload.file deve ser string");

  if (isHttpUrl(file)) {
    const tmpPath = await downloadUrlToTmp(file);
    return { input: fs.createReadStream(tmpPath), cleanupPath: tmpPath };
  }

  if (fs.existsSync(file)) {
    return { input: fs.createReadStream(file), cleanupPath: null };
  }

  return { input: file, cleanupPath: null };
}

// ===== Rate limit =====
const tokenWindows = new Map();
async function waitForRateLimit(token, max, ms) {
  const key = token || DEFAULT_TOKEN || "no-token";
  const safeMax = Math.max(1, Number(max) || 1);
  const safeMs = Math.max(200, Number(ms) || 1100);

  if (!tokenWindows.has(key)) tokenWindows.set(key, []);
  const arr = tokenWindows.get(key);

  while (true) {
    const now = Date.now();
    while (arr.length && now - arr[0] >= safeMs) arr.shift();

    if (arr.length < safeMax) {
      arr.push(now);
      return;
    }

    const wait = safeMs - (now - arr[0]);
    await new Promise((r) => setTimeout(r, Math.max(wait, 50)));
  }
}

// ===== WORKER =====
const worker = new Worker(
  "disparos",
  async (job) => {
    let tmpToCleanup = null;
    const campaignId = job.data?.campaignId || null;

    try {
      // 🔥 PAUSA CORRIGIDA
      if (campaignId) {
        const paused = await isCampaignPaused(campaignId);
        if (paused) {
          logPausedThrottled(campaignId);
          throw new Error("CAMPAIGN_PAUSED");
        }
      }

      const { chatId } = job.data || {};
      if (!chatId) throw new Error("chatId ausente");

      const lim = job.data?.limit || { max: 1, ms: 1100 };
      await waitForRateLimit(job.data?.botToken, lim.max, lim.ms);

      const bot = getBot(job.data?.botToken);

      if (job.data?.mensagem && !job.data?.type) {
        await bot.sendMessage(chatId, job.data.mensagem);
        await incCampaignSent(campaignId);
        return;
      }

      const { type, payload } = job.data || {};
      if (!type) throw new Error("type ausente");

      switch (type) {
        case "text":
          await bot.sendMessage(chatId, payload?.text ?? payload?.mensagem, payload?.options);
          break;

        case "audio":
          var r = await resolveTelegramInput(payload?.file);
          tmpToCleanup = r.cleanupPath;
          await bot.sendAudio(chatId, r.input, payload?.options);
          break;

        case "video":
          var r = await resolveTelegramInput(payload?.file);
          tmpToCleanup = r.cleanupPath;
          await bot.sendVideo(chatId, r.input, payload?.options);
          break;

        case "voice":
          var r = await resolveTelegramInput(payload?.file);
          tmpToCleanup = r.cleanupPath;
          await bot.sendVoice(chatId, r.input, payload?.options);
          break;

        case "photo":
          var r = await resolveTelegramInput(payload?.file);
          tmpToCleanup = r.cleanupPath;
          await bot.sendPhoto(chatId, r.input, payload?.options);
          break;

        case "document":
          var r = await resolveTelegramInput(payload?.file);
          tmpToCleanup = r.cleanupPath;
          await bot.sendDocument(chatId, r.input, payload?.options);
          break;

        default:
          throw new Error(`type inválido: ${type}`);
      }

      await incCampaignSent(campaignId);

    } catch (err) {

      if (err.message === "CAMPAIGN_PAUSED") {
        throw err; // retry natural
      }

      console.error("❌ Telegram erro:", err.message);

      if (shouldCountFinalFailure(job)) {
        await incCampaignFailed(campaignId);
      }

      throw err;
    } finally {
      if (tmpToCleanup) safeUnlink(tmpToCleanup);
    }
  },
  {
    connection,
    removeOnComplete: { count: 1000 },
    removeOnFail: { count: 5000 },
  }
);

worker.on("failed", (job, err) => {
  if (err.message !== "CAMPAIGN_PAUSED") {
    console.error("❌ Job falhou:", job?.id, err.message);
  }
});

worker.on("error", (err) =>
  console.error("❌ Worker error:", err.message)
);
