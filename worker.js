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

// =====================
// file_id capture (sem polling)
// =====================
const FILE_ID_CAPTURE = String(process.env.FILE_ID_CAPTURE || "0") === "1";
const FILE_ID_CAPTURE_CHAT = String(process.env.FILE_ID_CAPTURE_CHAT || "").trim();

function shouldCaptureFileId(chatId) {
  if (!FILE_ID_CAPTURE) return false;
  if (!FILE_ID_CAPTURE_CHAT) return false;
  return String(chatId) === String(FILE_ID_CAPTURE_CHAT);
}

function extractFileIdFromMessage(msg, fileType) {
  if (!msg) return null;

  const ft = String(fileType || "").toLowerCase();

  if (ft === "photo" && Array.isArray(msg.photo) && msg.photo.length) {
    const largest = msg.photo[msg.photo.length - 1];
    return {
      kind: "photo",
      file_id: largest?.file_id,
      file_unique_id: largest?.file_unique_id,
    };
  }

  if (ft === "video" && msg.video?.file_id) {
    return {
      kind: "video",
      file_id: msg.video.file_id,
      file_unique_id: msg.video.file_unique_id,
    };
  }

  if ((ft === "video_note" || ft === "videonote") && msg.video_note?.file_id) {
    return {
      kind: "video_note",
      file_id: msg.video_note.file_id,
      file_unique_id: msg.video_note.file_unique_id,
    };
  }

  if (ft === "audio" && msg.audio?.file_id) {
    return {
      kind: "audio",
      file_id: msg.audio.file_id,
      file_unique_id: msg.audio.file_unique_id,
    };
  }

  if (ft === "voice" && msg.voice?.file_id) {
    return {
      kind: "voice",
      file_id: msg.voice.file_id,
      file_unique_id: msg.voice.file_unique_id,
    };
  }

  if (msg.document?.file_id) {
    return {
      kind: "document",
      file_id: msg.document.file_id,
      file_unique_id: msg.document.file_unique_id,
    };
  }

  // fallback se o fileType veio estranho
  if (msg.video?.file_id) return { kind: "video", file_id: msg.video.file_id, file_unique_id: msg.video.file_unique_id };
  if (msg.audio?.file_id) return { kind: "audio", file_id: msg.audio.file_id, file_unique_id: msg.audio.file_unique_id };
  if (msg.voice?.file_id) return { kind: "voice", file_id: msg.voice.file_id, file_unique_id: msg.voice.file_unique_id };
  if (msg.video_note?.file_id) return { kind: "video_note", file_id: msg.video_note.file_id, file_unique_id: msg.video_note.file_unique_id };

  return null;
}

function logCapturedFileId({ tokenKey, campaignId, chatId, fileType, fileUrl }, capture) {
  const tokenHint = tokenKey ? `${String(tokenKey).slice(0, 10)}...` : "(no-token)";
  const source = fileUrl && /^https?:\/\//i.test(String(fileUrl)) ? "url" : "id";

  console.log(
    `🧷 CAPTURE file_id | kind=${capture.kind} fileType=${fileType} chatId=${chatId} campaign=${campaignId || "-"} token=${tokenHint}\n` +
      `   file_id=${capture.file_id}\n` +
      `   unique_id=${capture.file_unique_id || "-"}\n` +
      `   source=${source}`
  );
}

// cache de bots por token
const botCache = new Map();
function getBot(token) {
  const t = token || DEFAULT_TOKEN;
  if (!t) throw new Error("botToken ausente");
  if (botCache.has(t)) return botCache.get(t);
  const bot = new TelegramBot(t, { polling: false });
  botCache.set(t, bot);
  return bot;
}

// cache de username por token (pra montar START links)
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

function getRetryAfterSeconds(err) {
  const body = err?.response?.body;
  const p = body?.parameters;
  const ra1 = Number(p?.retry_after);
  if (Number.isFinite(ra1) && ra1 > 0) return ra1;

  const desc = getTelegramDescription(err);
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
    )
      return true;
  }

  if (statusCode === 401) return true;

  if (code === "ETIMEDOUT" || code === "ECONNRESET") return false;
  return false;
}

function isRetryableTransient(err) {
  const code = err?.code;
  const sc = getTelegramStatusCode(err);

  if (sc === 429) return true;
  if (sc >= 500 && sc <= 599) return true;
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
  retry429: 0,
  retryOther: 0,
  lastLogAt: 0,
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
    if (await isCampaignCanceled(campaignId)) return;
    await sleep(1500);
  }
}

// ===== buttons -> reply_markup =====
function normalizeButtons(raw) {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((b) => ({
      text: String(b?.text || "").trim(),
      type: String(b?.type || "").trim().toLowerCase(),
      value: String(b?.value || "").trim(),
    }))
    .filter((b) => b.text && b.value && (b.type === "url" || b.type === "start"))
    .slice(0, 4);
}

async function buildReplyMarkup(bot, tokenKey, buttons) {
  const btns = normalizeButtons(buttons);
  if (!btns.length) return null;

  const username = await getBotUsername(bot, tokenKey);

  const row = [];
  for (const b of btns) {
    if (b.type === "url") {
      row.push({ text: b.text, url: b.value });
      continue;
    }
    if (b.type === "start") {
      if (!username) continue; // sem username não dá pra montar deep-link
      const u = `https://t.me/${username}?start=${encodeURIComponent(b.value)}`;
      row.push({ text: b.text, url: u });
      continue;
    }
  }

  if (!row.length) return null;

  return { inline_keyboard: [row] }; // 1 linha com até 4 botões
}

// ===== mídia =====
async function inputFromUrlOrId(fileUrl) {
  const s = String(fileUrl || "").trim();
  if (!s) return null;

  if (!/^https?:\/\//i.test(s)) return s; // file_id

  if (typeof fetch !== "function") {
    throw new Error("fetch não disponível no Node. Use Node 18+ ou implemente fetch (undici).");
  }

  const r = await fetch(s);
  if (!r.ok) throw new Error(`Falha ao baixar arquivo (${r.status}) ${s}`);
  return Buffer.from(await r.arrayBuffer());
}

async function sendTelegram(bot, tokenKey, payload) {
  const campaignId = payload.campaignId;
  if (campaignId && (await isCampaignCanceled(campaignId))) {
    return { ok: true, canceled: true };
  }

  const chatId = payload.chatId;
  const text = payload.text;
  const caption = payload.caption;
  const fileType = String(payload.fileType || "").toLowerCase();
  const fileUrl = String(payload.fileUrl || "").trim();

  if (!chatId) throw new Error("chatId ausente");

  const reply_markup = await buildReplyMarkup(bot, tokenKey, payload.buttons);

  // Texto puro
  if (!fileUrl) {
    if (!text) throw new Error("text ausente");
    const opts = reply_markup
      ? { reply_markup, disable_web_page_preview: true }
      : { disable_web_page_preview: true };
    return bot.sendMessage(chatId, text, opts);
  }

  const input = await inputFromUrlOrId(fileUrl);
  if (!input) throw new Error("fileUrl inválida/vazia");

  const opts = {};
  if (caption) opts.caption = caption;
  if (reply_markup) opts.reply_markup = reply_markup;

  let msg;
  if (fileType === "photo") msg = await bot.sendPhoto(chatId, input, opts);
  else if (fileType === "video") msg = await bot.sendVideo(chatId, input, opts);
  else if (fileType === "document") msg = await bot.sendDocument(chatId, input, opts);
  else if (fileType === "audio") msg = await bot.sendAudio(chatId, input, opts);
  else if (fileType === "voice") msg = await bot.sendVoice(chatId, input, opts);
  else if (fileType === "video_note") msg = await bot.sendVideoNote(chatId, input, opts);
  else msg = await bot.sendDocument(chatId, input, opts);

  // ✅ Captura file_id SOMENTE para um chat específico (sem polling)
  if (shouldCaptureFileId(chatId)) {
    try {
      const capture = extractFileIdFromMessage(msg, fileType);
      if (capture?.file_id) {
        logCapturedFileId(
          { tokenKey, campaignId, chatId, fileType, fileUrl },
          capture
        );
      } else {
        console.warn("🧷 CAPTURE: mensagem enviada mas não encontrei file_id no retorno.");
      }
    } catch (e) {
      console.warn("🧷 CAPTURE: falha ao extrair/logar file_id:", e?.message || e);
    }
  }

  return msg;
}

// =====================
// Worker
// =====================
const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    const data = job.data || {};
    const campaignId = data.campaignId;

    await waitIfPaused(campaignId);

    if (await isCampaignCanceled(campaignId)) {
      stats.processed++;
      stats.canceled++;
      maybeLogProgress();

      await incCampaign(campaignId, "canceledCount");

      try { job.discard(); } catch {}
      return { ok: true, canceled: true };
    }

    const tokenKey = data.botToken || DEFAULT_TOKEN || "";
    const bot = getBot(data.botToken);

    try {
      const r = await sendTelegram(bot, tokenKey, data);

      if (r && r.canceled) {
        stats.processed++;
        stats.canceled++;
        maybeLogProgress();

        await incCampaign(campaignId, "canceledCount");
        try { job.discard(); } catch {}
        return { ok: true, canceled: true };
      }

      stats.processed++;
      stats.sent++;
      maybeLogProgress();

      await incCampaign(campaignId, "sent");
      return { ok: true };
    } catch (err) {
      if (is429(err)) {
        const ra = getRetryAfterSeconds(err) || 3;
        const jitterMs = Math.floor(Math.random() * 350);
        await sleep(ra * 1000 + jitterMs);

        stats.processed++;
        stats.retry429++;
        stats.last429Count++;
        stats.last429Sec = ra;
        maybeLogProgress();

        await incCampaign(campaignId, "retry429");
        throw err;
      }

      if (isRetryableTransient(err) && !isPermanentTelegramError(err)) {
        stats.processed++;
        stats.retryOther++;
        maybeLogProgress();

        await incCampaign(campaignId, "retryOther");
        throw err;
      }

      stats.processed++;
      stats.failed++;
      maybeLogProgress();

      await incCampaign(campaignId, "failed");

      if (isPermanentTelegramError(err)) {
        try { job.discard(); } catch {}
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

worker.on("failed", (job, err) => {
  const sc = getTelegramStatusCode(err);
  if (sc === 429) return; // resumo sai no maybeLogProgress

  const id = job?.id;
  const desc = getTelegramDescription(err);
  console.warn(`💥 failed job=${id} status=${sc || "-"} desc=${desc || err?.message || err}`);
});
