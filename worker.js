require("dotenv").config();
const { Worker } = require("bullmq");
const TelegramBot = require("node-telegram-bot-api");
const connection = require("./redis");

const botToken = process.env.TELEGRAM_BOT_TOKEN;

if (!botToken) {
  console.error("❌ TELOEGAM_BOT_TOKEN não definido nas variáveis de ambiente!");
  process.exit(1);
}

const bot = new TelegramBot(botToken, { polling: false });

const worker = new Worker(
  "disparos",
  async (job) => {
    const { chatId, mensagem } = job.data;

    try {
      console.log("Recebi job:", job.id, job.data);
      await bot.sendMessage(chatId, mensagem);
      console.log("✅ Enviado!");
    } catch (err) {
      console.error("❌ Telegram erro:", err.message);
      if (err.response?.body) console.error("Detalhe:", err.response.body);
      throw err; // marca o job como failed
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
