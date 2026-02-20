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

// ===== ✅ Redis client (status campanha) =====
const redis = new IORedis(connection);
redis.on("error", (e) => console.error("❌ Redis (status) error:", e.message));
redis.on("connect", () => console.log("✅ Redis (status) conectado"));

// ===== Token default (opcional) =====
const DEFAULT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
if (!DEFAULT_TOKEN) {
  console.warn(
    "⚠️ TELEGRAM_BOT_TOKEN não definido (ok se você sempre mandar botToken no job)."
  );
}

// ===== cache de bots por token =====
const botCache = new Map();
function getBot(token) {
  const t = token || DEFAULT_TOKEN;
  if (!t)
    throw new Error(
      "Nenhum token disponível (botToken do job ou TELEGRAM_BOT_TOKEN no env)"
    );
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

async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

// ===== Download seguro: URL -> /tmp (timeout + limite) =====
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
  const ac = new AbortController();
  const to = setTimeout(() => ac.abort(), timeoutMs);

  const maxBytes = Number(process.env.DOWNLOAD_MAX_BYTES || 80 * 1024 * 1024); // 80MB

  try {
    const r = await fetch(u, { signal: ac.signal });
    if (!r.ok) throw new Error(`Falha ao baixar arquivo (${r.status})`);

    const body = r.body;
    if (!body) throw new Error("Resposta sem body ao baixar arquivo");

    const len = Number(r.headers.get("content-length") || 0);
    if (len && len > maxBytes)
      throw new Error(`Arquivo grande demais: ${len} bytes (max ${maxBytes})`);

    const nodeStream = Readable.fromWeb(body);

    let written = 0;
    const ws = fs.createWriteStream(tmpPath);
    nodeStream.on("data", (chunk) => {
      written += chunk.length;
      if (written > maxBytes) {
        try {
          ws.destroy(new Error("Arquivo excedeu limite de download"));
        } catch {}
        try {
          ac.abort();
        } catch {}
      }
    });

    await pipeline(nodeStream, ws);
    return tmpPath;
  } finally {
    clearTimeout(to);
  }
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

  return { input: file, cleanupPath: null }; // file_id
}

// ===== Rate limit por token (janela) =====
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
    await sleep(Math.max(wait, 50));
  }
}

// ===== Backoff 429 =====
function getRetryAfterSeconds(err) {
  const ra = err?.response?.body?.parameters?.retry_after;
  if (Number.isFinite(Number(ra)) && Number(ra) > 0) return Number(ra);

  const msg = String(err?.response?.body?.description || err?.message || "");
  const m = msg.match(/retry after (\d+)/i);
  if (m) return Number(m[1]);

  return null;
}

async function sendWithBackoff(sendFn, opts = {}) {
  const maxAttempts = Number(opts.maxAttempts || process.env.BACKOFF_MAX_ATTEMPTS || 5);
  let attempt = 0;
  let extraBackoffMs = 0;

  while (true) {
    try {
      return await sendFn();
    } catch (err) {
      const code = err?.response?.body?.error_code;
      const is429 = code === 429 || /too many requests/i.test(String(err?.message || ""));

      if (!is429) throw err;

      attempt++;
      const retryAfter = getRetryAfterSeconds(err); // seconds
      const baseWaitMs = retryAfter ? retryAfter * 1000 : 2000;

      extraBackoffMs = Math.min(10000, extraBackoffMs ? extraBackoffMs * 2 : 500);
      const waitMs = baseWaitMs + extraBackoffMs;

      console.warn(
        `🚨 429 (flood). Tentativa ${attempt}/${maxAttempts}. retry_after=${retryAfter ?? "?"}s. Aguardando ${waitMs}ms...`
      );

      if (attempt >= maxAttempts) throw err;

      await sleep(waitMs);
    }
  }
}

// ===== Ramp-up (subir taxa aos poucos) =====
const WORKER_STARTED_AT = Date.now();
function rampedRate(base, target, rampSeconds) {
  const elapsed = (Date.now() - WORKER_STARTED_AT) / 1000;
  const t = Math.min(1, Math.max(0, elapsed / rampSeconds));
  return Math.round(base + (target - base) * t);
}

// ===== Adaptive throttle (por token) =====
const tokenPenalty = new Map(); // tokenKey -> { level, untilTs }
const PENALTY_BASE_SECONDS = Number(process.env.PENALTY_BASE_SECONDS || 120); // 2 min
const PENALTY_MAX_LEVEL = Number(process.env.PENALTY_MAX_LEVEL || 6);

function tokenKeyOf(token) {
  return token || DEFAULT_TOKEN || "no-token";
}

function getPenaltyLevel(token) {
  const k = tokenKeyOf(token);
  const st = tokenPenalty.get(k);
  if (!st) return 0;
  if (Date.now() > st.untilTs) {
    tokenPenalty.delete(k);
    return 0;
  }
  return st.level || 0;
}

function register429(token, retryAfterSeconds) {
  const k = tokenKeyOf(token);
  const now = Date.now();
  const cur = tokenPenalty.get(k);
  const curLevel = cur && now <= cur.untilTs ? cur.level : 0;

  const nextLevel = Math.min(PENALTY_MAX_LEVEL, (curLevel || 0) + 1);

  const baseMs = PENALTY_BASE_SECONDS * 1000;
  const levelMs = baseMs * (1 + nextLevel * 0.6);
  const retryMs = (Number(retryAfterSeconds) || 0) * 1000;

  const ttlMs = Math.max(levelMs, retryMs || 0);

  tokenPenalty.set(k, { level: nextLevel, untilTs: now + ttlMs });

  console.warn(
    `🧯 Adaptive throttle: token=${maskToken(k)} level=${nextLevel} por ~${Math.round(
      ttlMs / 1000
    )}s`
  );
}

function applyPenalty(lim, token) {
  const level = getPenaltyLevel(token);
  if (!level) return lim;

  const factor = 1 + level * 0.3;

  const max = Math.max(1, Math.floor(Number(lim.max || 1) / factor));
  const ms = Math.max(200, Math.round(Number(lim.ms || 1000) * factor));

  return { max, ms };
}

function getEffectiveLimit(type, requested, token) {
  const reqMax = Number(requested?.max || 1);
  const reqMs = Number(requested?.ms || 1000);

  let base;
  if (type === "text") {
    base = { max: rampedRate(3, Math.min(reqMax, 25), 180), ms: Math.max(200, reqMs) };
  } else {
    base = { max: rampedRate(1, Math.min(reqMax, 8), 300), ms: Math.max(700, reqMs) };
  }

  return applyPenalty(base, token);
}

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

      const bot = getBot(job.data?.botToken);

      // legado
      if (job.data?.mensagem && !job.data?.type) {
        await sendWithBackoff(() => bot.sendMessage(chatId, job.data.mensagem));
        console.log("✅ Enviado (texto legado)!");
        ok = true;
        return;
      }

      const { type, payload } = job.data || {};
      if (!type) throw new Error("type ausente no job");

      const lim = job.data?.limit || { max: 1, ms: 1100 };
      const eff = getEffectiveLimit(type, lim, job.data?.botToken);

      await waitForRateLimit(job.data?.botToken, eff.max, eff.ms);

      const wrapSend = (fn) =>
        sendWithBackoff(async () => {
          try {
            return await fn();
          } catch (err) {
            const code = err?.response?.body?.error_code;
            if (code === 429) {
              const ra = getRetryAfterSeconds(err);
              register429(job.data?.botToken, ra);
            }
            throw err;
          }
        });

      switch (type) {
        case "text": {
          const text = payload?.text ?? payload?.mensagem;
          if (!text) throw new Error("payload.text ausente");
          await wrapSend(() => bot.sendMessage(chatId, text, payload?.options));
          break;
        }

        case "audio": {
          const { input, cleanupPath } = await resolveTelegramInput(payload?.file);
          tmpToCleanup = cleanupPath;
          await wrapSend(() =>
            bot.sendAudio(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) })
          );
          break;
        }

        case "video": {
          const { input, cleanupPath } = await resolveTelegramInput(payload?.file);
          tmpToCleanup = cleanupPath;
          await wrapSend(() =>
            bot.sendVideo(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) })
          );
          break;
        }

        case "voice": {
          const { input, cleanupPath } = await resolveTelegramInput(payload?.file);
          tmpToCleanup = cleanupPath;
          await wrapSend(() =>
            bot.sendVoice(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) })
          );
          break;
        }

        case "video_note": {
          const { input, cleanupPath } = await resolveTelegramInput(payload?.file);
          tmpToCleanup = cleanupPath;
          await wrapSend(() => bot.sendVideoNote(chatId, input, { ...(payload?.options || {}) }));
          break;
        }

        case "photo": {
          const { input, cleanupPath } = await resolveTelegramInput(payload?.file);
          tmpToCleanup = cleanupPath;
          await wrapSend(() =>
            bot.sendPhoto(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) })
          );
          break;
        }

        case "document": {
          const { input, cleanupPath } = await resolveTelegramInput(payload?.file);
          tmpToCleanup = cleanupPath;
          await wrapSend(() =>
            bot.sendDocument(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) })
          );
          break;
        }

        default:
          throw new Error(`type inválido: ${type}`);
      }

      console.log("✅ Enviado!");
      ok = true;
    } catch (err) {
      const code = err?.response?.body?.error_code;
      if (code === 429) {
        const ra = err?.response?.body?.parameters?.retry_after;
        console.error("🚨 429 flood control. retry_after:", ra);
      }
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

worker.on("failed", (job, err) => console.error("❌ Job falhou:", job?.id, err.message));
worker.on("error", (err) => console.error("❌ Worker error:", err.message));

console.log("✅ Worker iniciado (fila: disparos)");
