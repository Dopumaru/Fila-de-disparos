require("dotenv").config();
const { Worker } = require("bullmq");
const TelegramBot = require("node-telegram-bot-api");
const connection = require("./redis");

const bot = new TelegramBot(process.env.8552687174:AAHa4GYl9Av_gm4_7dHG6IMIusIS2YzZrME, { polling: false });

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
      // isso aqui costuma trazer o motivo real (401/403/400 etc)
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
