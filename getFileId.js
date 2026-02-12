require("dotenv").config();
const TelegramBot = require("node-telegram-bot-api");

const botToken = process.env.TELEGRAM_BOT_TOKEN;

if (!botToken) {
  console.error("❌ TELEGRAM_BOT_TOKEN não definido no .env");
  process.exit(1);
}

// IDs autorizados (só eles conseguem gerar file_id)
const ADMINS = [
  1532301009,
  1264397026,
];

const bot = new TelegramBot(botToken, { polling: true });

console.log("✅ FileID Helper rodando (polling ON)");
console.log("📌 Somente ADMINS podem gerar file_id");

function isAdmin(fromId) {
  return ADMINS.includes(fromId);
}

function replyHelp(chatId) {
  return bot.sendMessage(
    chatId,
    [
      "📎 *Gerador de file_id*",
      "",
      "Me envie uma mídia (foto/vídeo/áudio/voz/documento/animation/sticker) e eu te devolvo o *file_id* pra colar no painel.",
      "",
      "Comandos:",
      "• /whoami — mostra seu id",
      "• /help — ajuda",
    ].join("\n"),
    { parse_mode: "Markdown" }
  );
}

bot.onText(/\/start|\/help/i, (msg) => {
  const fromId = msg.from?.id;
  if (!fromId || !isAdmin(fromId)) return;
  replyHelp(msg.chat.id);
});

bot.onText(/\/whoami/i, (msg) => {
  const fromId = msg.from?.id;
  if (!fromId || !isAdmin(fromId)) return;
  bot.sendMessage(msg.chat.id, `🆔 Seu id é: ${fromId}`);
});

bot.on("message", async (msg) => {
  const fromId = msg.from?.id;
  if (!fromId) return;

  // 🔒 Bloqueia quem não é admin
  if (!isAdmin(fromId)) return;

  const chatId = msg.chat?.id;
  if (!chatId) return;

  // Ignora comandos (já tratados)
  if (msg.text && msg.text.startsWith("/")) return;

  // Coleta possíveis mídias
  const out = [];

  // Foto vem em array — pega a maior
  if (msg.photo && msg.photo.length) {
    const best = msg.photo[msg.photo.length - 1];
    out.push(`🖼️ photo file_id:\n\`${best.file_id}\``);
  }

  if (msg.video_note) {
    out.push(`🔵 video_note file_id:\n\`${msg.video_note.file_id}\``);
  }

  if (msg.voice) {
    out.push(`🗣️ voice file_id:\n\`${msg.voice.file_id}\``);
  }

  if (msg.audio) {
    out.push(`🎧 audio file_id:\n\`${msg.audio.file_id}\``);
  }

  if (msg.video) {
    out.push(`🎬 video file_id:\n\`${msg.video.file_id}\``);
  }

  if (msg.document) {
    out.push(`📄 document file_id:\n\`${msg.document.file_id}\``);
  }

  // Extras comuns
  if (msg.animation) {
    out.push(`🎞️ animation file_id:\n\`${msg.animation.file_id}\``);
  }

  if (msg.sticker) {
    out.push(`🧩 sticker file_id:\n\`${msg.sticker.file_id}\``);
  }

  if (!out.length) {
    await bot.sendMessage(chatId, "ℹ️ Não veio mídia reconhecida. Envie foto/vídeo/áudio/voz/documento etc.");
    return;
  }

  // Responde no próprio Telegram (pro cliente copiar)
  const text = [
    "✅ *Aqui está o file_id* (copie e cole no painel):",
    "",
    ...out,
    "",
    "⚠️ *Importante:* esse file_id só funciona para *este mesmo bot/token*.",
  ].join("\n\n");

  await bot.sendMessage(chatId, text, { parse_mode: "Markdown" });

  // (opcional) log no console também
  console.log("\n--- NOVA MÍDIA (admin) ---");
  console.log("from:", fromId, "chatId:", chatId);
  out.forEach((x) => console.log(x.replace(/`/g, "")));
});

bot.on("polling_error", (err) => {
  console.error("❌ polling_error:", err.message);
});
