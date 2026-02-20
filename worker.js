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

// ===== Campaign helpers (Redis keys) =====
function cKey(id, suffix) {
  return `campaign:${id}:${suffix}`;
}

async function isCampaignPaused(campaignId) {
  if (!campaignId) return false;
  const v = await connection.get(cKey(campaignId, "paused"));
  return String(v || "0") === "1";
}

async function incCampaignSent(campaignId) {
  if (!campaignId) return;
  await connection.hincrby(cKey(campaignId, "counts"), "sent", 1);
}

async function incCampaignFailed(campaignId) {
  if (!campaignId) return;
  await connection.hincrby(cKey(campaignId, "counts"), "failed", 1);
}

// cache de bots por token
const botCache = new Map();
function getBot(token) {
  const t = token || DEFAULT_TOKEN;
  if (!t) throw new Error("Nenhum token disponível (botToken do job ou TELEGRAM_BOT_TOKEN no env)");
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

// baixa URL (inclusive interna do docker) -> salva em /tmp -> retorna caminho
async function downloadUrlToTmp(url) {
  const u = String(url || "").trim();
  const urlObj = new URL(u);

  // tenta manter extensão
  let ext = path.extname(urlObj.pathname || "");
  if (!ext) ext = ".bin";

  const tmpPath = path.join(
    "/tmp",
    `tg-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}${ext}`
  );

  const r = await fetch(u);
  if (!r.ok) throw new Error(`Falha ao baixar arquivo (${r.status})`);

  // stream web -> node
  const body = r.body;
  if (!body) throw new Error("Resposta sem body ao baixar arquivo");

  const nodeStream = Readable.fromWeb(body);
  await pipeline(nodeStream, fs.createWriteStream(tmpPath));
  return tmpPath;
}

/**
 * Resolve input do Telegram:
 * - URL: baixa pra /tmp e manda como ReadStream (garante funcionar com url interna)
 * - path local existente: ReadStream
 * - senão: assume file_id do Telegram
 */
async function resolveTelegramInput(file) {
  if (!file) throw new Error("payload.file não foi enviado");
  if (typeof file !== "string") throw new Error("payload.file deve ser string");

  // URL -> baixar e virar stream
  if (isHttpUrl(file)) {
    const tmpPath = await downloadUrlToTmp(file);
    return { input: fs.createReadStream(tmpPath), cleanupPath: tmpPath };
  }

  // path local -> stream
  if (fs.existsSync(file)) {
    return { input: fs.createReadStream(file), cleanupPath: null };
  }

  // file_id do Telegram
  return { input: file, cleanupPath: null };
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

const PAUSE_RETRY_MS = Number(process.env.CAMPAIGN_PAUSE_RETRY_MS || 2000);

const worker = new Worker(
  "disparos",
  async (job) => {
    let tmpToCleanup = null;
    const campaignId = job.data?.campaignId || null;

    try {
      // ✅ pausa por campanha (não trava o worker)
      if (campaignId) {
        const paused = await isCampaignPaused(campaignId);
        if (paused) {
          // joga o job pra frente e sai sem erro
          const delayMs = Math.max(500, PAUSE_RETRY_MS);
          await job.moveToDelayed(Date.now() + delayMs);
          console.log("⏸️ Campaign pausada. Job adiado:", job.id, "campaignId:", campaignId);
          return;
        }
      }

      console.log("Recebi job:", job.id, {
        campaignId: campaignId || undefined,
        chatId: job.data?.chatId,
        type: job.data?.type,
        token: maskToken(job.data?.botToken),
      });

      const { chatId } = job.data || {};
      if (!chatId) throw new Error("chatId ausente no job");

      const lim = job.data?.limit || { max: 1, ms: 1100 };
      await waitForRateLimit(job.data?.botToken, lim.max, lim.ms ?? lim.duration ?? lim.limitMs);

      const bot = getBot(job.data?.botToken);

      // legado
      if (job.data?.mensagem && !job.data?.type) {
        await bot.sendMessage(chatId, job.data.mensagem);
        await incCampaignSent(campaignId);
        console.log("✅ Enviado (texto legado)!");
        return;
      }

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
          const { input, cleanupPath } = await resolveTelegramInput(payload?.file);
          tmpToCleanup = cleanupPath;
          await bot.sendAudio(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "video": {
          const { input, cleanupPath } = await resolveTelegramInput(payload?.file);
          tmpToCleanup = cleanupPath;
          await bot.sendVideo(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "voice": {
          const { input, cleanupPath } = await resolveTelegramInput(payload?.file);
          tmpToCleanup = cleanupPath;
          await bot.sendVoice(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "video_note": {
          const { input, cleanupPath } = await resolveTelegramInput(payload?.file);
          tmpToCleanup = cleanupPath;
          await bot.sendVideoNote(chatId, input, { ...(payload?.options || {}) });
          break;
        }

        case "photo": {
          const { input, cleanupPath } = await resolveTelegramInput(payload?.file);
          tmpToCleanup = cleanupPath;
          await bot.sendPhoto(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "document": {
          const { input, cleanupPath } = await resolveTelegramInput(payload?.file);
          tmpToCleanup = cleanupPath;
          await bot.sendDocument(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        default:
          throw new Error(`type inválido: ${type}`);
      }

      await incCampaignSent(campaignId);
      console.log("✅ Enviado!");
    } catch (err) {
      console.error("❌ Telegram erro:", err.message);
      if (err.response?.body) console.error("Detalhe:", err.response.body);

      // ✅ marca failed no progresso (somente erro real)
      await incCampaignFailed(campaignId);

      throw err;
    } finally {
      // limpa o arquivo temp baixado pelo worker
      if (tmpToCleanup) safeUnlink(tmpToCleanup);
    }
  },
  { connection }
);

worker.on("failed", (job, err) => console.error("❌ Job falhou:", job?.id, err.message));
worker.on("error", (err) => console.error("❌ Worker error:", err.message));
