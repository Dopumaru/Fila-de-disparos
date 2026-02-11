require("dotenv").config();
const TelegramBot = require("node-telegram-bot-api");

const botToken = process.env.TELEGRAM_BOT_TOKEN;

if (!botToken) {
  console.error("❌ TELEGRAM_BOT_TOKEN não definido no .env");
  process.exit(1);
}


const ADMINS = [
  1532301009, // seu id (exemplo)
  1264397026,
];

const bot = new TelegramBot(botToken, { polling: true });

console.log("✅ getFileId rodando (polling ON)");
console.log("📌 Somente ADMINS podem gerar file_id");
console.log("👉 Envie áudio/vídeo/voz/bolinha para o bot e veja o file_id aqui.");

bot.on("message", (msg) => {
  const fromId = msg.from?.id;

  if (!fromId) return;

  // 🔒 Bloqueia quem não é admin
  if (!ADMINS.includes(fromId)) {

    return;
  }

  console.log("\n--- NOVA MENSAGEM (admin) ---");
  console.log("from:", fromId);
  console.log("chatId:", msg.chat?.id);

  if (msg.video_note) {
    console.log("🔵 video_note file_id:", msg.video_note.file_id);
    console.log("   duration:", msg.video_note.duration, "s");
    console.log("   length:", msg.video_note.length);
  }

  if (msg.voice) {
    console.log("🗣️ voice file_id:", msg.voice.file_id);
    console.log("   duration:", msg.voice.duration, "s");
    console.log("   mime:", msg.voice.mime_type);
  }

  if (msg.audio) {
    console.log("🎧 audio file_id:", msg.audio.file_id);
    console.log("   duration:", msg.audio.duration, "s");
    console.log("   mime:", msg.audio.mime_type);
    console.log("   title:", msg.audio.title);
    console.log("   performer:", msg.audio.performer);
  }

  if (msg.video) {
    console.log("🎬 video file_id:", msg.video.file_id);
    console.log("   duration:", msg.video.duration, "s");
    console.log("   width/height:", msg.video.width, "/", msg.video.height);
    console.log("   mime:", msg.video.mime_type);
  }

  if (msg.document) {
    console.log("📄 document file_id:", msg.document.file_id);
    console.log("   file_name:", msg.document.file_name);
    console.log("   mime:", msg.document.mime_type);
  }

  if (!msg.video_note && !msg.voice && !msg.audio && !msg.video && !msg.document) {
    console.log("ℹ️ Não veio mídia (talvez texto/foto/sticker).");
  }
});


bot.on("polling_error", (err) => {
  console.error("❌ polling_error:", err.message);
});
