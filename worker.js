// worker.js
require("dotenv").config();
const { Worker, Queue } = require("bullmq");
const TelegramBot = require("node-telegram-bot-api");
const { redis, connection } = require("./redis");

const QUEUE_NAME = process.env.QUEUE_NAME || "disparos";
const DEFAULT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;

// ====== LOG CONTROL (menos flood) ======
const METRICS_INTERVAL_MS = Number(process.env.METRICS_INTERVAL_MS) || 30000; // 30s
const IDLE_LOG_INTERVAL_MS = Number(process.env.IDLE_LOG_INTERVAL_MS) || 600000; // 10min (se 0 => nunca loga idle)
const CAMPAIGN_RPS_CACHE_MS = Number(process.env.CAMPAIGN_RPS_CACHE_MS) || 2000; // 2s
const RATE_GUARD_MAX_SLEEP_MS = Number(process.env.RATE_GUARD_MAX_SLEEP_MS) || 1000; // 1s

// ✅ Modo PRO de logs
const LOG_ONLY_WHEN_ACTIVE = String(process.env.LOG_ONLY_WHEN_ACTIVE || "1") === "1"; // só loga com campanha/fila
const LOG_CAMPAIGN_START_END = String(process.env.LOG_CAMPAIGN_START_END || "1") === "1"; // log de início/fim

if (!DEFAULT_TOKEN) {
  console.warn("⚠️ TELEGRAM_BOT_TOKEN não definido (ok se sempre mandar botToken no job).");
}

process.on("unhandledRejection", (err) => console.error("❌ unhandledRejection:", err));
process.on("uncaughtException", (err) => console.error("❌ uncaughtException:", err));

// ===== Queue (pra backlog) =====
const queue = new Queue(QUEUE_NAME, { connection });

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

async function getCampaignMeta(campaignId) {
  if (!campaignId) return {};
  try {
    return (await redis.hgetall(campaignKey(campaignId))) || {};
  } catch {
    return {};
  }
}

function toNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function fmtInt(n) {
  return new Intl.NumberFormat("pt-BR").format(Number(n || 0));
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
// Campaign controls
// =====================
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

async function getCampaignRps(campaignId) {
  if (!campaignId) return null;
  const v = await redis.hget(campaignKey(campaignId), "ratePerSecond");
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : null;
}

// =====================
// FileID cache (vídeo único p/ 50k)
// =====================
function assetKeyFromJob(data) {
  const campaignId = data.campaignId || "global";
  const t = String(data.fileType || "file").toLowerCase();
  return data.assetKey || `campaign:${campaignId}:asset:${t}`;
}

function redisAssetKey(assetKey) {
  return `asset:${assetKey}`; // string => file_id
}

async function getCachedFileId(assetKey) {
  if (!assetKey) return null;
  const v = await redis.get(redisAssetKey(assetKey));
  return v ? String(v) : null;
}

async function setCachedFileId(assetKey, fileId) {
  if (!assetKey || !fileId) return;
  await redis.set(redisAssetKey(assetKey), String(fileId), "EX", 60 * 60 * 24 * 7);
}

// =====================
// Rate realtime por campanha (token bucket simples)
// =====================

// cache de ratePerSecond (pra não HGET em todo job)
const rpsCache = new Map(); // campaignId -> { rps, at }
async function getRpsCached(campaignId) {
  if (!campaignId) return 10;
  const now = Date.now();
  const cur = rpsCache.get(campaignId);
  if (cur && now - cur.at < CAMPAIGN_RPS_CACHE_MS) return cur.rps;

  const rps = (await getCampaignRps(campaignId)) || 10;
  rpsCache.set(campaignId, { rps, at: now });
  return rps;
}

// espaçamento mínimo entre envios por campanha (ms)
const nextSendAt = new Map(); // campaignId -> epoch_ms

async function waitIfPaused(campaignId) {
  if (!campaignId) return;
  while (await isCampaignPaused(campaignId)) {
    if (await isCampaignCanceled(campaignId)) return;
    await sleep(1500);
  }
}

async function rateGuard(campaignId) {
  if (!campaignId) return;

  if (await isCampaignCanceled(campaignId)) return;

  while (true) {
    await waitIfPaused(campaignId);
    if (await isCampaignCanceled(campaignId)) return;

    const rps = await getRpsCached(campaignId);
    const spacing = Math.max(1, Math.floor(1000 / Math.max(1, rps)));

    const now = Date.now();
    const next = nextSendAt.get(campaignId) || now;

    if (now >= next) {
      nextSendAt.set(campaignId, now + spacing);
      stats.lastSeenCfgRps = rps;
      return;
    }

    const sleepMs = Math.min(RATE_GUARD_MAX_SLEEP_MS, next - now);
    if (sleepMs > 0) await sleep(sleepMs);
  }
}

// =====================
// Metrics + modo PRO (tabela alinhada)
// =====================
const stats = {
  startedAt: Date.now(),
  processed: 0,
  sent: 0,
  failed: 0,
  canceled: 0,
  retry429: 0,
  retryOther: 0,

  // janela (pra rps atual e 429ps)
  win: [], // [{t, processed, retry429}]
  lastSeenCfgRps: null,

  backlog: { waiting: 0, delayed: 0, paused: 0, active: 0, failed: 0 },
  lastLogAt: 0,
  lastIdleLogAt: 0,
};

function bucketNowSecond() {
  return Math.floor(Date.now() / 1000) * 1000;
}

function winBump(field, by = 1) {
  const t = bucketNowSecond();
  let last = stats.win[stats.win.length - 1];
  if (!last || last.t !== t) {
    stats.win.push({ t, processed: 0, retry429: 0 });
    if (stats.win.length > 40) stats.win.shift(); // guarda ~40s
    last = stats.win[stats.win.length - 1];
  }
  last[field] = (last[field] || 0) + by;
}

function sumWindow(ms, field) {
  const cutoff = Date.now() - ms;
  let s = 0;
  for (const b of stats.win) {
    if (b.t >= cutoff) s += b[field] || 0;
  }
  return s;
}

async function refreshBacklog() {
  try {
    const c = await queue.getJobCounts("waiting", "delayed", "paused", "active", "failed");
    stats.backlog = {
      waiting: c.waiting || 0,
      delayed: c.delayed || 0,
      paused: c.paused || 0,
      active: c.active || 0,
      failed: c.failed || 0,
    };
  } catch {}
}

// ====== tracker de campanhas (log início/fim + para logs quando terminar) ======
const campaignTracker = new Map();
// campaignId -> { startedLogged, finishedLogged, firstSeenAt }

function trackCampaign(campaignId) {
  if (!campaignId) return;
  if (!campaignTracker.has(campaignId)) {
    campaignTracker.set(campaignId, { startedLogged: false, finishedLogged: false, firstSeenAt: Date.now() });
  }
}

function padRight(s, width) {
  const str = String(s ?? "");
  if (str.length >= width) return str.slice(0, width);
  return str + " ".repeat(width - str.length);
}

function padLeft(s, width) {
  const str = String(s ?? "");
  if (str.length >= width) return str.slice(0, width);
  return " ".repeat(width - str.length) + str;
}

function padNum(n, width) {
  return padLeft(fmtInt(n), width);
}

function fmtFixed(n, digits = 2) {
  const x = Number(n);
  if (!Number.isFinite(x)) return "0.00";
  return x.toFixed(digits);
}

async function logCampaignStartIfNeeded(campaignId) {
  if (!LOG_CAMPAIGN_START_END || !campaignId) return;
  const st = campaignTracker.get(campaignId);
  if (!st || st.startedLogged) return;

  st.startedLogged = true;
  const meta = await getCampaignMeta(campaignId);
  const total = toNum(meta.total);
  const rps = toNum(meta.ratePerSecond) || (await getRpsCached(campaignId));

  console.log(`🎬 CAMPANHA INICIADA | id=${campaignId} | total=${fmtInt(total)} | rps=${rps}`);
}

async function logCampaignEndIfDone() {
  if (!LOG_CAMPAIGN_START_END || campaignTracker.size === 0) return;

  for (const [cid, st] of campaignTracker.entries()) {
    if (st.finishedLogged) continue;

    const meta = await getCampaignMeta(cid);
    const total = toNum(meta.total);
    const sent = toNum(meta.sent);
    const failed = toNum(meta.failed);
    const canceledCount = toNum(meta.canceledCount);

    const done = total > 0 && (sent + failed + canceledCount) >= total;

    if (done) {
      st.finishedLogged = true;
      const durSec = Math.max(1, Math.floor((Date.now() - st.firstSeenAt) / 1000));
      console.log(
        `🏁 CAMPANHA FINALIZADA | id=${cid} | ok=${fmtInt(sent)} | fail=${fmtInt(failed)} | cancel=${fmtInt(
          canceledCount
        )} | tempo=${durSec}s`
      );

      campaignTracker.delete(cid);
    }
  }
}

// Header (1x) quando começa a ter atividade
let printedHeader = false;
function printHeaderIfNeeded() {
  if (printedHeader) return;
  printedHeader = true;
  console.log(
    "📊 METRICS\n" +
      [
        padRight("tipo", 7),
        padRight("rps", 8),
        padRight("cfg", 5),
        padRight("429/s", 7),
        padRight("ok", 8),
        padRight("fail", 8),
        padRight("cancel", 8),
        padRight("pend", 7),
        padRight("act", 5),
        padRight("qfail", 6),
      ].join("  ")
  );
  console.log(
    [
      padRight("------", 7),
      padRight("--------", 8),
      padRight("-----", 5),
      padRight("-------", 7),
      padRight("--------", 8),
      padRight("--------", 8),
      padRight("--------", 8),
      padRight("-------", 7),
      padRight("-----", 5),
      padRight("------", 6),
    ].join("  ")
  );
}

async function logMetricsIfNeeded() {
  const now = Date.now();
  if (now - stats.lastLogAt < METRICS_INTERVAL_MS) return;
  stats.lastLogAt = now;

  await refreshBacklog();

  const pending = stats.backlog.waiting + stats.backlog.delayed + stats.backlog.paused;
  const active = stats.backlog.active || 0;
  const failedQ = stats.backlog.failed || 0;

  const anyTracked = campaignTracker.size > 0;
  const totallyIdle = stats.processed === 0 && pending === 0 && active === 0 && failedQ === 0;

  if (LOG_ONLY_WHEN_ACTIVE) {
    if (!anyTracked && pending === 0 && active === 0) {
      if (IDLE_LOG_INTERVAL_MS <= 0) return;
      if (!totallyIdle) return;

      if (now - stats.lastIdleLogAt < IDLE_LOG_INTERVAL_MS) return;
      stats.lastIdleLogAt = now;
      console.log(`😴 WORKER OCIOSO | fila=0`);
      return;
    }
  } else {
    if (totallyIdle) {
      if (IDLE_LOG_INTERVAL_MS <= 0) return;
      if (now - stats.lastIdleLogAt < IDLE_LOG_INTERVAL_MS) return;
      stats.lastIdleLogAt = now;
      console.log(`😴 WORKER OCIOSO | fila=0`);
      return;
    }
  }

  printHeaderIfNeeded();

  const winSec = METRICS_INTERVAL_MS / 1000;
  const pW = sumWindow(METRICS_INTERVAL_MS, "processed");
  const rpsNow = pW / winSec;

  const r429W = sumWindow(METRICS_INTERVAL_MS, "retry429");
  const r429ps = r429W / winSec;

  // Linha tipo “tabela”, alinhada (emoji só no começo)
  const line = [
    padRight("🚀ENVIO", 7),
    padLeft(fmtFixed(rpsNow, 2), 8),
    padLeft(String(stats.lastSeenCfgRps ?? "-"), 5),
    padLeft(fmtFixed(r429ps, 2), 7),
    padNum(stats.sent, 8),
    padNum(stats.failed, 8),
    padNum(stats.canceled, 8),
    padNum(pending, 7),
    padNum(active, 5),
    padNum(failedQ, 6),
  ].join("  ");

  console.log(line);

  await logCampaignEndIfDone();
}

setInterval(() => {
  logMetricsIfNeeded().catch(() => {});
}, 1000);

// =====================
// buttons -> reply_markup
// =====================
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
      if (!username) continue;
      const u = `https://t.me/${username}?start=${encodeURIComponent(b.value)}`;
      row.push({ text: b.text, url: u });
      continue;
    }
  }

  if (!row.length) return null;
  return { inline_keyboard: [row] };
}

// ===== mídia =====
async function inputFromUrlOrId(fileUrl) {
  const s = String(fileUrl || "").trim();
  if (!s) return null;
  if (!/^https?:\/\//i.test(s)) return s; // file_id
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

  const opts = {};
  if (caption) opts.caption = caption;
  if (reply_markup) opts.reply_markup = reply_markup;

  // ===== ✅ VIDEO com cache de file_id =====
  if (fileType === "video") {
    const aKey = assetKeyFromJob(payload);
    const cached = await getCachedFileId(aKey);

    if (cached) {
      try {
        return await bot.sendVideo(chatId, cached, opts);
      } catch (e) {
        const sc = getTelegramStatusCode(e);
        const desc = getTelegramDescription(e);
        console.warn(`⚠️ file_id cache falhou (assetKey=${aKey}) status=${sc || "-"} desc=${desc}. Re-uplodando...`);
        try {
          await redis.del(redisAssetKey(aKey));
        } catch {}
      }
    }

    const input = await inputFromUrlOrId(fileUrl);
    if (!input) throw new Error("fileUrl inválida/vazia");

    const res = await bot.sendVideo(chatId, input, opts);

    const fid = res?.video?.file_id;
    if (fid) await setCachedFileId(aKey, fid);

    return res;
  }

  // Outros tipos (photo/document/etc)
  const input = await inputFromUrlOrId(fileUrl);
  if (!input) throw new Error("fileUrl inválida/vazia");

  if (fileType === "photo") return bot.sendPhoto(chatId, input, opts);
  if (fileType === "document") return bot.sendDocument(chatId, input, opts);
  if (fileType === "audio") return bot.sendAudio(chatId, input, opts);
  if (fileType === "voice") return bot.sendVoice(chatId, input, opts);
  if (fileType === "video_note") return bot.sendVideoNote(chatId, input, opts);

  return bot.sendDocument(chatId, input, opts);
}

// =====================
// Worker
// =====================
const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    const data = job.data || {};
    const campaignId = data.campaignId;

    // ===== logs início campanha (1x por campanha)
    trackCampaign(campaignId);
    await logCampaignStartIfNeeded(campaignId);

    // rate realtime por campanha (sem delay no job)
    await rateGuard(campaignId);

    await waitIfPaused(campaignId);

    if (await isCampaignCanceled(campaignId)) {
      stats.processed++;
      stats.canceled++;
      winBump("processed", 1);

      await incCampaign(campaignId, "canceledCount");
      try {
        job.discard();
      } catch {}
      return { ok: true, canceled: true };
    }

    const tokenKey = data.botToken || DEFAULT_TOKEN || "";
    const bot = getBot(data.botToken);

    try {
      const r = await sendTelegram(bot, tokenKey, data);

      if (r && r.canceled) {
        stats.processed++;
        stats.canceled++;
        winBump("processed", 1);

        await incCampaign(campaignId, "canceledCount");
        try {
          job.discard();
        } catch {}
        return { ok: true, canceled: true };
      }

      stats.processed++;
      stats.sent++;
      winBump("processed", 1);

      await incCampaign(campaignId, "sent");
      return { ok: true };
    } catch (err) {
      if (is429(err)) {
        const ra = getRetryAfterSeconds(err) || 3;
        const jitterMs = Math.floor(Math.random() * 350);
        await sleep(ra * 1000 + jitterMs);

        stats.processed++;
        stats.retry429++;
        winBump("processed", 1);
        winBump("retry429", 1);

        await incCampaign(campaignId, "retry429");
        throw err;
      }

      if (isRetryableTransient(err) && !isPermanentTelegramError(err)) {
        stats.processed++;
        stats.retryOther++;
        winBump("processed", 1);

        await incCampaign(campaignId, "retryOther");
        throw err;
      }

      stats.processed++;
      stats.failed++;
      winBump("processed", 1);

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

worker.on("ready", () => console.log(`✅ Worker pronto | fila=${QUEUE_NAME}`));
worker.on("error", (err) => console.error("❌ Worker error:", err?.message || err));

worker.on("failed", (job, err) => {
  const sc = getTelegramStatusCode(err);
  if (sc === 429) return;

  const id = job?.id;
  const desc = getTelegramDescription(err);
  console.warn(`💥 Falhou job=${id} status=${sc || "-"} desc=${desc || err?.message || err}`);
});
