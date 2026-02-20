// worker.js
require("dotenv").config();
const { Worker } = require("bullmq");
const TelegramBot = require("node-telegram-bot-api");
const connection = require("./redis");
const IORedis = require("ioredis");
const fs = require("fs");
const path = require("path");
const { pipeline } = require("stream/promises");
const { Readable } = require("stream");

// ===== ✅ Redis (status campanha) — usa REDIS_URL (mesmo do EasyPanel) =====
const REDIS_URL = (process.env.REDIS_URL || "").trim();
if (!REDIS_URL) {
  throw new Error(
    "REDIS_URL não definido. Defina (ex): redis://default:SENHA@redis-fila:6379/0"
  );
}

const redis = new IORedis(REDIS_URL, { maxRetriesPerRequest: null });
redis.on("error", (e) => console.error("❌ Redis(status) error:", e.message));
redis.on("connect", () => console.log("✅ Redis(status) conectado via", REDIS_URL));

const DEFAULT_TOKEN = (process.env.TELEGRAM_BOT_TOKEN || "").trim();
if (!DEFAULT_TOKEN) {
  console.warn("⚠️ TELEGRAM_BOT_TOKEN não definido (ok se você sempre mandar botToken no job).");
}

// ===== cache de bots por token =====
const botCache = new Map();
function getBot(token) {
  const t = (token || DEFAULT_TOKEN || "").trim();
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

// baixa URL -> /tmp -> retorna caminho
async function downloadUrlToTmp(url) {
  const u = String(url || "").trim();
  const urlObj = new URL(u);

  let ext = path.extname(urlObj.pathname || "");
  if (!ext) ext = ".bin";

  const tmpPath = path.join(
    "/tmp",
    `tg-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}${ext}`
  );

  const timeoutMs = Number(process.env.DOWNLOAD_TIMEOUT_MS || 20000);
  const maxBytes = Number(process.env.DOWNLOAD_MAX_BYTES || 80 * 1024 * 1024);

  const controller = new AbortController();
  const to = setTimeout(() => controller.abort(), timeoutMs);

  const r = await fetch(u, { signal: controller.signal }).finally(() => clearTimeout(to));
  if (!r.ok) throw new Error(`Falha ao baixar arquivo (${r.status})`);

  const body = r.body;
  if (!body) throw new Error("Resposta sem body ao baixar arquivo");

  // limita bytes (evita explodir disco)
  let downloaded = 0;
  const nodeStream = Readable.fromWeb(body).on("data", (chunk) => {
    downloaded += chunk.length;
    if (downloaded > maxBytes) {
      controller.abort();
      throw new Error(`Download excedeu limite de ${maxBytes} bytes`);
    }
  });

  await pipeline(nodeStream, fs.createWriteStream(tmpPath));
  return tmpPath;
}

/**
 * Resolve input do Telegram:
 * - URL: baixa pra /tmp e manda como ReadStream
 * - path local existente: ReadStream
 * - senão: assume file_id do Telegram
 */
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

// ===== Rate limit simples por token =====
const tokenWindows = new Map();
async function waitForRateLimit(token, max, ms) {
  const key = (token || DEFAULT_TOKEN || "no-token").trim();
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

console.log("✅ Worker conectando ao Redis (BullMQ) via", `${connection.host}:${connection.port}/${connection.db ?? 0}`);

const worker = new Worker(
  "disparos",
  async (job) => {
    let tmpToCleanup = null;
    let ok = false;

    try {
      console.log("Recebi job:", job.id, {
        chatId: job.data?.chatId,
        type: job.data?.type,
        token: maskToken(job.data?.botToken),
        campaignId: job.data?.campaignId,
      });

      const { chatId } = job.data || {};
      if (!chatId) throw new Error("chatId ausente no job");

      const lim = job.data?.limit || { max: 1, ms: 1100 };
      await waitForRateLimit(job.data?.botToken, lim.max, lim.ms ?? lim.duration ?? lim.limitMs);

      const bot = getBot(job.data?.botToken);

      // legado
      if (job.data?.mensagem && !job.data?.type) {
        await bot.sendMessage(chatId, job.data.mensagem);
        console.log("✅ Enviado (texto legado)!");
        ok = true;
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

      console.log("✅ Enviado!");
      ok = true;
    } catch (err) {
      console.error("❌ Telegram erro:", err.message);
      if (err.response?.body) console.error("Detalhe:", err.response.body);
      throw err;
    } finally {
      // ✅ atualiza status da campanha
      const campaignId = job.data?.campaignId;
      if (campaignId) {
        const key = `campaign:${campaignId}`;
        try {
          if (ok) await redis.hincrby(key, "sent", 1);
          else await redis.hincrby(key, "failed", 1);
        } catch (e) {
          console.error("❌ campaign counter error:", e.message);
        }
      }

      if (tmpToCleanup) safeUnlink(tmpToCleanup);
    }
  },
  { connection }
);

worker.on("completed", (job) => console.log("✅ Job completed:", job.id));
worker.on("failed", (job, err) => console.error("❌ Job falhou:", job?.id, err.message));
worker.on("error", (err) => console.error("❌ Worker error:", err.message));
