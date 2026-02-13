// worker.js
require("dotenv").config();
const { Worker } = require("bullmq");
const TelegramBot = require("node-telegram-bot-api");
const connection = require("./redis");
const fs = require("fs");

const DEFAULT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;

if (!DEFAULT_TOKEN) {
  console.warn("⚠️ TELEGRAM_BOT_TOKEN não definido (ok se você sempre mandar botToken no job).");
}

// cache de bots por token (em memória)
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

function isHttpUrl(u) {
  return /^https?:\/\//i.test(String(u || "").trim());
}

/**
 * ✅ Se vier URL: o worker baixa e manda BUFFER (Telegram aceita).
 * ✅ Se vier path local: manda stream
 * ✅ Senão: assume file_id
 */
async function resolveTelegramInput(file) {
  if (!file) throw new Error("payload.file não foi enviado");
  if (typeof file !== "string") throw new Error("payload.file deve ser string");

  // URL -> baixa no worker (assim Telegram NÃO precisa acessar sua URL)
  if (isHttpUrl(file)) {
    let res;
    try {
      res = await fetch(file, { redirect: "follow" });
    } catch (e) {
      throw new Error(`Falha ao fazer fetch da URL: ${file} (${e.message})`);
    }

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`Falha ao baixar arquivo (${res.status}) da URL: ${file} ${txt ? "- " + txt.slice(0, 120) : ""}`);
    }

    const buf = Buffer.from(await res.arrayBuffer());

    // proteção básica (evitar explodir memória com arquivos gigantes)
    const maxMb = Number(process.env.MAX_DOWNLOAD_MB || 25);
    const maxBytes = maxMb * 1024 * 1024;
    if (buf.length > maxBytes) {
      throw new Error(`Arquivo muito grande para baixar no worker (${(buf.length / 1024 / 1024).toFixed(1)}MB). Aumente MAX_DOWNLOAD_MB ou use outro fluxo.`);
    }

    return buf;
  }

  // path local (se existir no container do worker)
  if (fs.existsSync(file)) return fs.createReadStream(file);

  // se não é URL nem path local, assume file_id do Telegram
  return file;
}

/**
 * Cleanup de campanha (controlado no Redis)
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
  try {
    const locked = await connection.set(lockKey, "1", "NX", "EX", 300);
    if (!locked) return;
  } catch {
    return;
  }

  try {
    const filePath = await connection.hget(key, "filePath");
    if (filePath && fs.existsSync(filePath)) {
      try { fs.unlinkSync(filePath); } catch {}
    }

    await connection.del(key);
    await connection.del(lockKey);
    console.log("🧹 Campanha finalizada (redis limpo):", campaignId);
  } catch {
    // best-effort
  }
}

// rate limit por token (sliding window em memória)
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

      // legado
      if (job.data?.mensagem && !job.data?.type) {
        await bot.sendMessage(chatId, job.data.mensagem);
        console.log("✅ Enviado (texto legado)!");
        if (campaignId) await finalizeCampaignIfDone(campaignId);
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
          const input = await resolveTelegramInput(payload?.file);
          await bot.sendAudio(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "video": {
          const input = await resolveTelegramInput(payload?.file);
          await bot.sendVideo(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "voice": {
          const input = await resolveTelegramInput(payload?.file);
          await bot.sendVoice(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "video_note": {
          const input = await resolveTelegramInput(payload?.file);
          await bot.sendVideoNote(chatId, input, { ...(payload?.options || {}) });
          break;
        }

        case "photo": {
          const input = await resolveTelegramInput(payload?.file);
          await bot.sendPhoto(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "document": {
          const input = await resolveTelegramInput(payload?.file);
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
