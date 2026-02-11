require("dotenv").config();
const { Worker } = require("bullmq");
const TelegramBot = require("node-telegram-bot-api");
const connection = require("./redis");
const fs = require("fs");

const botToken = process.env.TELEGRAM_BOT_TOKEN;

if (!botToken) {
  console.error("❌ TELEGRAM_BOT_TOKEN não definido nas variáveis de ambiente!");
  process.exit(1);
}

const bot = new TelegramBot(botToken, { polling: false });

function resolveTelegramInput(file) {
  if (!file) throw new Error("payload.file não foi enviado");
  if (typeof file !== "string") throw new Error("payload.file deve ser string");

  if (/^https?:\/\//i.test(file)) return file; 
  if (fs.existsSync(file)) return fs.createReadStream(file); 
  return file; 
}

const worker = new Worker(
  "disparos",
  async (job) => {
    try {
      console.log("Recebi job:", job.id, job.data);

      const { chatId } = job.data;
      if (!chatId) throw new Error("chatId ausente no job");

      
      if (job.data.mensagem && !job.data.type) {
        await bot.sendMessage(chatId, job.data.mensagem);
        console.log("✅ Enviado (texto legado)!");
        return;
      }

      const { type, payload } = job.data;
      if (!type) throw new Error("type ausente no job");

      switch (type) {
        case "text": {
          const text = payload?.text ?? payload?.mensagem;
          if (!text) throw new Error("payload.text ausente");
          await bot.sendMessage(chatId, text, payload?.options);
          break;
        }

        case "audio": {
          const input = resolveTelegramInput(payload?.file);
          await bot.sendAudio(chatId, input, {
            caption: payload?.caption,
            ...(payload?.options || {}),
          });
          break;
        }

        case "video": {
          const input = resolveTelegramInput(payload?.file);
          await bot.sendVideo(chatId, input, {
            caption: payload?.caption,
            ...(payload?.options || {}),
          });
          break;
        }

        case "voice": {
          const input = resolveTelegramInput(payload?.file);
          await bot.sendVoice(chatId, input, {
            caption: payload?.caption,
            ...(payload?.options || {}),
          });
          break;
        }

        case "video_note": {
          const input = resolveTelegramInput(payload?.file);
          await bot.sendVideoNote(chatId, input, {
            ...(payload?.options || {}),
          });
          break;
        }

        case "photo": {
          const input = resolveTelegramInput(payload?.file);
          await bot.sendPhoto(chatId, input, {
          caption: payload?.caption,
           ...(payload?.options || {}),
           });
           break;
        }


        default:
          throw new Error(`type inválido: ${type}`);
      }

      console.log("✅ Enviado!");
    } catch (err) {
      console.error("❌ Telegram erro:", err.message);
      if (err.response?.body) console.error("Detalhe:", err.response.body);
      throw err;
    }
  },
  {
    connection,
    limiter: { max: 1, duration: 1000 },
  }
);

worker.on("failed", (job, err) =>
  console.error("❌ Job falhou:", job?.id, err.message)
);
worker.on("error", (err) => console.error("❌ Worker error:", err.message));
