require("dotenv").config();
const { Worker } = require("bullmq");
const TelegramBot = require("node-telegram-bot-api");
const connection = require("./redis");
const fs = require("fs");

// fallback (se chegar job sem botToken)
const DEFAULT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;

if (!DEFAULT_TOKEN) {
  console.warn("⚠️ TELEGRAM_BOT_TOKEN não definido (ok se você sempre mandar botToken no job).");
}

// cache de bots por token (não salva em disco)
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

function resolveTelegramInput(file) {
  if (!file) throw new Error("payload.file não foi enviado");
  if (typeof file !== "string") throw new Error("payload.file deve ser string");

  if (/^https?:\/\//i.test(file)) return file; // URL
  if (fs.existsSync(file)) return fs.createReadStream(file); // path local
  return file; // file_id (compat)
}

function safeUnlink(p) {
  try {
    if (p && fs.existsSync(p)) fs.unlinkSync(p);
  } catch {
    // best-effort
  }
}

/**
 * CAMPANHA (cleanup automático)
 * - API cria: campaign:<id> com { filePath, pending }
 * - Cada job carrega payload.campaignId
 * - Worker decrementa pending quando job "finaliza"
 * - Quando pending chega em 0 => apaga filePath e del key (uma única vez)
 */
async function finalizeCampaignIfDone(campaignId) {
  if (!campaignId) return;

  const key = `campaign:${campaignId}`;

  // decrementa pending de forma atômica
  let newPending;
  try {
    newPending = await connection.hincrby(key, "pending", -1);
  } catch {
    return;
  }

  // se não chegou em 0 ainda, sai
  if (typeof newPending !== "number" || newPending > 0) return;

  // ===== GARANTIR QUE SÓ UM JOB FAÇA O CLEANUP =====
  // Se vários jobs chegarem juntos no 0/negativo, só 1 apaga.
  // SETNX em uma chave de lock:
  const lockKey = `campaign:${campaignId}:cleanup`;
  const locked = await connection.set(lockKey, "1", "NX", "EX", 300); // 5 min
  if (!locked) return; // outro job já está limpando

  try {
    const filePath = await connection.hget(key, "filePath");
    if (filePath) safeUnlink(filePath);

    await connection.del(key);
    await connection.del(lockKey);

    console.log("🧹 Campanha finalizada, arquivo apagado:", campaignId);
  } catch {
    // best-effort
  }
}

// rate limit por token (sliding window)
const tokenWindows = new Map(); // token -> [timestamps]
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
    // esse delete é apenas para "tempFile por job"
    let tempPathToDelete = null;

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

      // rate limit por token (usa job.data.limit)
      const lim = job.data?.limit || { max: 1, ms: 1100 };
      await waitForRateLimit(job.data?.botToken, lim.max, lim.ms ?? lim.duration ?? lim.limitMs);

      const bot = getBot(job.data?.botToken);

      // legado: mensagem sem type
      if (job.data?.mensagem && !job.data?.type) {
        await bot.sendMessage(chatId, job.data.mensagem);
        console.log("✅ Enviado (texto legado)!");
        if (campaignId) await finalizeCampaignIfDone(campaignId);
        return;
      }

      const { type, payload } = job.data || {};
      if (!type) throw new Error("type ausente no job");

      // helper: só marca pra apagar se for arquivo local existente E tempFile=true E NÃO for campanha
      const markTempIfLocal = () => {
        if (
          payload?.tempFile &&
          !payload?.campaignId &&
          typeof payload?.file === "string" &&
          fs.existsSync(payload.file)
        ) {
          tempPathToDelete = payload.file;
        }
      };

      switch (type) {
        case "text": {
          const text = payload?.text ?? payload?.mensagem;
          if (!text) throw new Error("payload.text ausente");
          await bot.sendMessage(chatId, text, payload?.options);
          break;
        }

        case "audio": {
          const input = resolveTelegramInput(payload?.file);
          markTempIfLocal();
          await bot.sendAudio(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "video": {
          const input = resolveTelegramInput(payload?.file);
          markTempIfLocal();
          await bot.sendVideo(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "voice": {
          const input = resolveTelegramInput(payload?.file);
          markTempIfLocal();
          await bot.sendVoice(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "video_note": {
          const input = resolveTelegramInput(payload?.file);
          markTempIfLocal();
          await bot.sendVideoNote(chatId, input, { ...(payload?.options || {}) });
          break;
        }

        case "photo": {
          const input = resolveTelegramInput(payload?.file);
          markTempIfLocal();
          await bot.sendPhoto(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "document": {
          const input = resolveTelegramInput(payload?.file);
          markTempIfLocal();
          await bot.sendDocument(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        default:
          throw new Error(`type inválido: ${type}`);
      }

      console.log("✅ Enviado!");

      // campanha: decrementa no sucesso
      if (campaignId) await finalizeCampaignIfDone(campaignId);

      // tempFile por job (não campanha)
      if (tempPathToDelete) safeUnlink(tempPathToDelete);
    } catch (err) {
      console.error("❌ Telegram erro:", err.message);
      if (err.response?.body) console.error("Detalhe:", err.response.body);

      // falha final: attemptsMade >= attempts - 1
      const attempts = job?.opts?.attempts ?? 1;
      const attemptsMade = job?.attemptsMade ?? 0;
      const isFinalFailure = attemptsMade >= (attempts - 1);

      if (isFinalFailure && campaignId) {
        await finalizeCampaignIfDone(campaignId);
      }

      if (tempPathToDelete) safeUnlink(tempPathToDelete);
      throw err;
    }
  },
  { connection }
);

worker.on("failed", (job, err) => console.error("❌ Job falhou:", job?.id, err.message));
worker.on("error", (err) => console.error("❌ Worker error:", err.message));
