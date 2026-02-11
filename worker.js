require("dotenv").config();
const { Worker } = require("bullmq");
const TelegramBot = require("node-telegram-bot-api");
const connection = require("./redis");
const fs = require("fs");

// fallback (se chegar job sem botToken)
const DEFAULT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || process.env.TELEGRAM_BOT_TOKEN;

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

  if (/^https?:\/\//i.test(file)) return file;        // URL
  if (fs.existsSync(file)) return fs.createReadStream(file); // path local
  return file; // file_id
}

// rate limit por token (sliding window)
const tokenWindows = new Map(); // token -> [timestamps]
async function waitForRateLimit(token, max, ms) {
  const key = token || DEFAULT_TOKEN || "no-token";
  if (!tokenWindows.has(key)) tokenWindows.set(key, []);
  const arr = tokenWindows.get(key);

  while (true) {
    const now = Date.now();
    // remove antigos
    while (arr.length && now - arr[0] >= ms) arr.shift();

    if (arr.length < max) {
      arr.push(now);
      return;
    }

    const wait = ms - (now - arr[0]);
    await new Promise(r => setTimeout(r, Math.max(wait, 50)));
  }
}

const worker = new Worker(
  "disparos",
  async (job) => {
    let tempPathToDelete = null;

    try {
      console.log("Recebi job:", job.id, {
        chatId: job.data?.chatId,
        type: job.data?.type,
        token: maskToken(job.data?.botToken),
      });

      const { chatId } = job.data;
      if (!chatId) throw new Error("chatId ausente no job");

      // legado: mensagem sem type
      if (job.data.mensagem && !job.data.type) {
        const bot = getBot(job.data.botToken);
        // rate limit default
        const lim = job.data.limit || { max: 1, ms: 1100 };
        await waitForRateLimit(job.data.botToken, lim.max, lim.ms);

        await bot.sendMessage(chatId, job.data.mensagem);
        console.log("✅ Enviado (texto legado)!");
        return;
      }

      const { type, payload } = job.data;
      if (!type) throw new Error("type ausente no job");

      const bot = getBot(job.data.botToken);
      const lim = job.data.limit || { max: 1, ms: 1100 };
      await waitForRateLimit(job.data.botToken, lim.max, lim.ms);

      switch (type) {
        case "text": {
          const text = payload?.text ?? payload?.mensagem;
          if (!text) throw new Error("payload.text ausente");
          await bot.sendMessage(chatId, text, payload?.options);
          break;
        }

        case "audio": {
          const input = resolveTelegramInput(payload?.file);
          if (payload?.tempFile && typeof payload?.file === "string") tempPathToDelete = payload.file;
          await bot.sendAudio(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "video": {
          const input = resolveTelegramInput(payload?.file);
          if (payload?.tempFile && typeof payload?.file === "string") tempPathToDelete = payload.file;
          await bot.sendVideo(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "voice": {
          const input = resolveTelegramInput(payload?.file);
          if (payload?.tempFile && typeof payload?.file === "string") tempPathToDelete = payload.file;
          await bot.sendVoice(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "video_note": {
          const input = resolveTelegramInput(payload?.file);
          if (payload?.tempFile && typeof payload?.file === "string") tempPathToDelete = payload.file;
          await bot.sendVideoNote(chatId, input, { ...(payload?.options || {}) });
          break;
        }

        case "photo": {
          const input = resolveTelegramInput(payload?.file);
          if (payload?.tempFile && typeof payload?.file === "string") tempPathToDelete = payload.file;
          await bot.sendPhoto(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        case "document": {
          const input = resolveTelegramInput(payload?.file);
          if (payload?.tempFile && typeof payload?.file === "string") tempPathToDelete = payload.file;
          await bot.sendDocument(chatId, input, { caption: payload?.caption, ...(payload?.options || {}) });
          break;
        }

        default:
          throw new Error(`type inválido: ${type}`);
      }

      console.log("✅ Enviado!");

      // limpa arquivo temp (upload)
      if (tempPathToDelete && fs.existsSync(tempPathToDelete)) {
        fs.unlink(tempPathToDelete, () => {});
      }
    } catch (err) {
      console.error("❌ Telegram erro:", err.message);
      if (err.response?.body) console.error("Detalhe:", err.response.body);
      throw err;
    }
  },
  {
    connection,
    // IMPORTANTE: não use limiter global aqui, porque agora limit é por token
    // limiter: { max: 1, duration: 1000 },
  }
);

worker.on("failed", (job, err) => console.error("❌ Job falhou:", job?.id, err.message));
worker.on("error", (err) => console.error("❌ Worker error:", err.message));

