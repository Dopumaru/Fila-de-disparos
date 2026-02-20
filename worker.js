// worker.js
require("dotenv").config();
const { Worker } = require("bullmq");
const IORedis = require("ioredis");
const TelegramBot = require("node-telegram-bot-api");
const fs = require("fs");
const path = require("path");
const { pipeline } = require("stream/promises");
const { Readable } = require("stream");

// ===== ✅ Redis/BullMQ connection (ÚNICA fonte de verdade: REDIS_URL) =====
const REDIS_URL = (process.env.REDIS_URL || "").trim();
if (!REDIS_URL) {
  throw new Error("REDIS_URL não definido no worker. Ex: redis://default:SENHA@redis-fila:6379/0");
}

const connection = new IORedis(REDIS_URL, {
  maxRetriesPerRequest: null, // obrigatório pro BullMQ
  enableReadyCheck: true,
});

connection.on("connect", () => console.log("✅ Worker conectado ao Redis via", REDIS_URL));
connection.on("error", (e) => console.error("❌ Redis Worker error:", e.message));

const redis = connection;

// ===== Bot token fallback =====
const DEFAULT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
if (!DEFAULT_TOKEN) {
  console.warn("⚠️ TELEGRAM_BOT_TOKEN não definido (ok se você sempre mandar botToken no job).");
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

// ===== Download com timeout e limite =====
const DOWNLOAD_TIMEOUT_MS = Number(process.env.DOWNLOAD_TIMEOUT_MS || 20000);
const DOWNLOAD_MAX_BYTES = Number(process.env.DOWNLOAD_MAX_BYTES || 80 * 1024 * 1024); // 80MB default

async function downloadUrlToTmp(url) {
  const u = String(url || "").trim();
  const urlObj = new URL(u);

  let ext = path.extname(urlObj.pathname || "");
  if (!ext) ext = ".bin";

  const tmpPath = path.join(
    "/tmp",
    `tg-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}${ext}`
  );

  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), DOWNLOAD_TIMEOUT_MS);

  const r = await fetch(u, { signal: ac.signal }).finally(() => clearTimeout(t));
  if (!r.ok) throw new Error(`Falha ao baixar arquivo (${r.status})`);

  const body = r.body;
  if (!body) throw new Error("Resposta sem body ao baixar arquivo");

  // stream web -> node, com limite de bytes
  const nodeStream = Readable.fromWeb(body);

  let total = 0;
  nodeStream.on("data", (chunk) => {
    total += chunk.length;
    if (total > DOWNLOAD_MAX_BYTES) {
      nodeStream.destroy(new Error(`Download excedeu limite (${DOWNLOAD_MAX_BYTES} bytes)`));
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

// ===== Rate limit por token (simples) =====
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

// ===== Job attempts/backoff no lado do Worker (re-tentativa segura) =====
const BACKOFF_MAX_ATTEMPTS = Number(process.env.BACKOFF_MAX_ATTEMPTS || 5);

function is429(err) {
  const msg = String(err?.message || "");
  const code = err?.response?.statusCode || err?.response?.status;
  return code === 429 || msg.includes("429");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function backoffIfNeeded(job, err) {
  // tenta extrair retry_after (Telegram às vezes manda)
  const retryAfter =
    Number(err?.response?.body?.parameters?.retry_after) ||
    Number(err?.response?.body?.retry_after) ||
    null;

  const attempt = Number(job?.attemptsMade || 0) + 1;

  if (!is429(err)) return;

  const base = retryAfter ? retryAfter * 1000 : 1500;
  const jitter = Math.floor(Math.random() * 500);
  const wait = Math.min(60_000, base * attempt + jitter);

  console.warn(`⚠️ 429 detectado. Backoff ${wait}ms (attempt ${attempt}/${BACKOFF_MAX_ATTEMPTS})`);
  await sleep(wait);
}

// ===== Worker =====
const worker = new Worker(
  "disparos",
  async (job) => {
    let tmpToCleanup = null;
    let ok = false;

    try {
      console.log("📦 Recebi job:", job.id, {
        chatId: job.data?.chatId,
        type: job.data?.type,
        token: maskToken(job.data?.botToken),
        campaignId: job.data?.campaignId,
      });

      const { chatId } = job.data || {};
      if (!chatId) throw new Error("chatId ausente no job");

      // rate limit
      const lim = job.data?.limit || { max: 1, ms: 1100 };
      await waitForRateLimit(job.data?.botToken, lim.max, lim.ms ?? lim.duration ?? lim.limitMs);

      const bot = getBot(job.data?.botToken);

      // legado
      if (job.data?.mensagem && !job.data?.type) {
        await bot.sendMessage(chatId, job.data.mensagem);
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

      ok = true;
      console.log("✅ Enviado job:", job.id);
    } catch (err) {
      console.error("❌ Telegram erro:", err.message);
      if (err.response?.body) console.error("Detalhe:", err.response.body);

      // se for 429, aplica backoff e deixa falhar pra BullMQ retry (se tiver attempts no job)
      await backoffIfNeeded(job, err);

      throw err;
    } finally {
      // ✅ atualiza status da campanha (se tiver campaignId)
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

      // limpa arquivo temp baixado pelo worker
      if (tmpToCleanup) safeUnlink(tmpToCleanup);
    }
  },
  {
    connection,
    // retries do BullMQ (se o job não setou attempts, isso aqui ajuda)
    // OBS: attempts/backoff é melhor setar no queue.add, mas isso já evita “morrer” fácil
    // (não quebra nada se você deixar assim)
    // @ts-ignore
    limiter: undefined,
    // @ts-ignore
    settings: undefined,
  }
);

worker.on("failed", (job, err) => {
  console.error("❌ Job falhou:", job?.id, err.message);
});
worker.on("error", (err) => {
  console.error("❌ Worker error:", err.message);
});
worker.on("completed", (job) => {
  console.log("✅ Job completed:", job?.id);
});
