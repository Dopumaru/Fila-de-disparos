// worker.js
require("dotenv").config();
const { Worker, Queue } = require("bullmq");
const TelegramBot = require("node-telegram-bot-api");
const { redis, connection } = require("./redis");

const QUEUE_NAME = process.env.QUEUE_NAME || "disparos";
const DEFAULT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;

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
// FileID cache (vídeo único p/ 50k)
// =====================
function assetKeyFromJob(data) {
  const campaignId = data.campaignId || "global";
  const t = String(data.fileType || "file").toLowerCase();
  // você pode mandar assetKey no job; se não mandar, vira 1 cache por campanha+tipo
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
  // TTL opcional (7 dias). Se quiser infinito, remove o EX.
  await redis.set(redisAssetKey(assetKey), String(fileId), "EX", 60 * 60 * 24 * 7);
}

// =====================
// Metrics “SaaS” (avg + janela 5s + backlog)
// =====================
const stats = {
  startedAt: Date.now(),
  processed: 0,
  sent: 0,
  failed: 0,
  canceled: 0,
  retry429: 0,
  retryOther: 0,
  lastIdleLogAt: 0,

  // janela por segundo (mantém ~8s)
  win: [], // [{t, processed, retry429}]
  lastSeenCfgRps: null,

  backlog: { waiting: 0, delayed: 0, paused: 0, active: 0, failed: 0 },
  lastTickAt: 0,
};

function bucketNowSecond() {
  return Math.floor(Date.now() / 1000) * 1000;
}

function winBump(field, by = 1) {
  const t = bucketNowSecond();
  let last = stats.win[stats.win.length - 1];
  if (!last || last.t !== t) {
    stats.win.push({ t, processed: 0, retry429: 0 });
    if (stats.win.length > 8) stats.win.shift();
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
  } catch {
    // não mata worker por métrica
  }
}

async function tickLog() {
  const now = Date.now();
  if (now - stats.lastTickAt < 5000) return;
  stats.lastTickAt = now;

  await refreshBacklog();

  const pending = stats.backlog.waiting + stats.backlog.delayed + stats.backlog.paused;
  const active = stats.backlog.active || 0;
  const failed = stats.backlog.failed || 0;

  // ✅ Se estiver totalmente idle, não flooda.
  // Loga "idle" no máximo 1x a cada 3 minutos (pra provar que tá vivo).
  const totallyIdle = stats.processed === 0 && pending === 0 && active === 0 && failed === 0;

  if (totallyIdle) {
    stats.lastIdleLogAt = stats.lastIdleLogAt || 0;
    if (now - stats.lastIdleLogAt < 180000) return; // 3 min
    stats.lastIdleLogAt = now;
    console.log(`METRICS | idle=1 | backlog(pending=0,active=0,failed=0)`);
    return;
  }

  const elapsed = Math.max(1, (now - stats.startedAt) / 1000);
  const rpsAvg = stats.processed / elapsed;

  const p5 = sumWindow(5000, "processed");
  const rps5 = p5 / 5;

  const r429_5 = sumWindow(5000, "retry429");
  const r429ps = r429_5 / 5;

  console.log(
    [
      "METRICS",
      `avg=${rpsAvg.toFixed(2)}/s`,
      `rps_5s=${rps5.toFixed(2)}/s`,
      `cfg_rps=${stats.lastSeenCfgRps ?? "-"}`,
      `429ps=${r429ps.toFixed(2)}`,
      `processed=${stats.processed}`,
      `sent=${stats.sent}`,
      `failed=${stats.failed}`,
      `canceled=${stats.canceled}`,
      `retry429=${stats.retry429}`,
      `retryOther=${stats.retryOther}`,
      `backlog(pending=${pending},active=${active},failed=${failed})`,
      `waiting=${stats.backlog.waiting}`,
      `delayed=${stats.backlog.delayed}`,
      `paused=${stats.backlog.paused}`,
    ].join(" | ")
  );
}

setInterval(() => {
  tickLog().catch(() => {});
}, 1000);

// =====================
// pause loop
// =====================
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
        console.warn(
          `⚠️ cached file_id falhou (assetKey=${aKey}) status=${sc || "-"} desc=${desc}. Re-uplodando...`
        );
        try {
          await redis.del(redisAssetKey(aKey));
        } catch {}
        // cai pro upload abaixo
      }
    }

    const input = await inputFromUrlOrId(fileUrl); // URL => baixa 1x; file_id => manda direto
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

    // rps configurado (pra log)
    if (campaignId) {
      const rps = await getCampaignRps(campaignId);
      if (rps) stats.lastSeenCfgRps = rps;
    }

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

worker.on("ready", () => console.log(`✅ Worker ready | queue=${QUEUE_NAME}`));
worker.on("error", (err) => console.error("❌ Worker error:", err?.message || err));

worker.on("failed", (job, err) => {
  const sc = getTelegramStatusCode(err);
  if (sc === 429) return; // 429 já vai pro METRICS

  const id = job?.id;
  const desc = getTelegramDescription(err);
  console.warn(`💥 failed job=${id} status=${sc || "-"} desc=${desc || err?.message || err}`);
});
