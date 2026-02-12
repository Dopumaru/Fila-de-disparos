require("dotenv").config();
const { Worker } = require("bullmq");
const TelegramBot = require("node-telegram-bot-api");
const connection = require("./redis");
const fs = require("fs");
const path = require("path");

const DEFAULT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const UPLOAD_DIR = process.env.UPLOAD_DIR || "/app/uploads";

if (!DEFAULT_TOKEN) {
  console.warn("⚠️ TELEGRAM_BOT_TOKEN não definido (ok se você sempre mandar botToken no job).");
}

// cache de bots por token
const botCache = new Map();
function getBot(token) {
  const t = token || DEFAULT_TOKEN;
  if (!t) throw new Error("Nenhum token disponível (botToken do job ou TELEGRAM_BOT_TOKEN no .env)");
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

function safeUnlink(p) {
  try { if (p && fs.existsSync(p)) fs.unlinkSync(p); } catch {}
}

// ✅ resolve "upload:<filename>" e path local
function resolveTelegramInput(file) {
  if (!file) throw new Error("payload.file não foi enviado");
  if (typeof file !== "string") throw new Error("payload.file deve ser string");

  // URL
  if (/^https?:\/\//i.test(file)) return file;

  // upload:<filename>
  if (file.startsWith("upload:")) {
    const name = file.slice("upload:".length).trim();
    const full = path.join(UPLOAD_DIR, name);
    if (!fs.existsSync(full)) {
      throw new Error(
        `Arquivo de upload não encontrado no worker: ${full}. (Volume /app/uploads não está compartilhado?)`
      );
    }
    return fs.createReadStream(full);
  }

  // path absoluto/relativo
  if (fs.existsSync(file)) return fs.createReadStream(file);

  // se cair aqui, vai virar file_id e quebrar -> melhor falhar claro
  throw new Error(
    `Arquivo não encontrado no worker (e não é URL): "${file}". Isso viraria file_id e causaria 400.`
  );
}

/**
 * Cleanup de campanha
 */
async function finalizeCampaignIfDone(campaignId) {
  if (!campaignId) return;

  const key = `campaign:${campaignId}`;
  let newPending;
  try {
    newPending = await connection.hincrby(key, "pending", -1);
  } catch {
    return;
  }
  if (typeof newPending !== "number" || newPending > 0) return;

  const lockKey = `campaign:${campaignId}:cleanup`;
  const locked = await connection.set(lockKey, "1", "NX", "EX", 300);
  if (!locked) return;

  try {
    const filePath = await connection.hget(key, "filePath");
    if (filePath) safeUnlink(filePath);
    await connection.del(key);
    await connection.del(lockKey);
    console.log("🧹 Campanha finalizada, arquivo apagado:", campaignId);
  } catch {}
}

// rate limit por token
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

const worker = new Worker(
  "disparos",
  async (job) => {
    const campaignId = job?.data?.payload?.campaignId || null;

    try {
      console.log("Recebi job:", job.id, {
        chatId: job.data?.chatId,
        type: job.data?.type,
        token: maskToken(job.data?.botToken),
        campaignId: campaignId || undefined,
      });

      const { chatId } = job.data || {};
      if (!chatId) throw new Error("chatId ausente no job");

      const lim = job.data?.limit || { max: 1, ms: 1100 };
      await waitForRateLimit(job.data?.botToken, lim.max, lim.ms ?? lim.duration ?? lim.limitMs);

      const bot = getBot(job.data?.botToken);
      const { type, payload } = job.data || {};
      if (!type) throw new Error("type ausente no job");

      switch (type) {
        case "text": {
          const text = payload?.text ?? payload?.mensagem;
          if (!text) throw new Error("payload.text ausente");
          await bot.sendMessage(chatId, text, payload?.options);
          break;
        }
        case "audio": {
          const input = resolveTelegramInput(payload?.file);
          await bot.sendAudio(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }
        case "video": {
          const input = resolveTelegramInput(payload?.file);
          await bot.sendVideo(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }
        case "voice": {
          const input = resolveTelegramInput(payload?.file);
          await bot.sendVoice(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }
        case "video_note": {
          const input = resolveTelegramInput(payload?.file);
          await bot.sendVideoNote(chatId, input, { ...(payload?.options || {}) });
          break;
        }
        case "photo": {
          const input = resolveTelegramInput(payload?.file);
          await bot.sendPhoto(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }
        case "document": {
          const input = resolveTelegramInput(payload?.file);
          await bot.sendDocument(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }
        default:
          throw new Error(`type inválido: ${type}`);
      }

      console.log("✅ Enviado!");

      if (campaignId) await finalizeCampaignIfDone(campaignId);
    } catch (err) {
      console.error("❌ Telegram erro:", err.message);
      if (err.response?.body) console.error("Detalhe:", err.response.body);

      // falha final correta
      const attempts = job?.opts?.attempts ?? 1;
      const attemptsMade = job?.attemptsMade ?? 0;
      const isFinalFailure = attemptsMade >= (attempts - 1);

      if (isFinalFailure && campaignId) await finalizeCampaignIfDone(campaignId);

      throw err;
    }
  },
  { connection }
);

worker.on("failed", (job, err) => console.error("❌ Job falhou:", job?.id, err.message));
worker.on("error", (err) => console.error("❌ Worker error:", err.message));
