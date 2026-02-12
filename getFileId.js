require("dotenv").config();
const TelegramBot = require("node-telegram-bot-api");
const fs = require("fs");
const path = require("path");

const botToken = process.env.TELEGRAM_BOT_TOKEN;
const SUPER_ADMIN_ID = Number(process.env.SUPER_ADMIN_ID);

if (!botToken) {
  console.error("❌ TELEGRAM_BOT_TOKEN não definido no .env");
  process.exit(1);
}
if (!SUPER_ADMIN_ID) {
  console.error("❌ SUPER_ADMIN_ID não definido no .env");
  process.exit(1);
}

const STORE_PATH = path.join(__dirname, "admins.json");

function loadAdmins() {
  try {
    const raw = fs.readFileSync(STORE_PATH, "utf8");
    const data = JSON.parse(raw);
    if (Array.isArray(data)) return new Set(data.map(Number).filter(Boolean));
  } catch {}
  return new Set([SUPER_ADMIN_ID]); // garante que você sempre entra
}

function saveAdmins(set) {
  fs.writeFileSync(STORE_PATH, JSON.stringify([...set], null, 2));
}

let ADMINS = loadAdmins();

function isAdmin(id) {
  return ADMINS.has(Number(id));
}
function isSuper(id) {
  return Number(id) === SUPER_ADMIN_ID;
}

const bot = new TelegramBot(botToken, { polling: true });

console.log("✅ FileID Helper (dinâmico) rodando");
console.log("SUPER_ADMIN_ID:", SUPER_ADMIN_ID);
console.log("ADMINS:", [...ADMINS]);

function helpText() {
  return [
    "📎 *Gerador de file_id*",
    "",
    "Me envie uma mídia e eu devolvo o *file_id* pra colar no painel.",
    "",
    "Comandos:",
    "• /whoami — mostra seu id",
    "• /help — ajuda",
    "",
    "Admin (somente super-admin):",
    "• /allow <id> — autoriza usuário",
    "• /deny <id> — remove usuário",
    "• /admins — lista autorizados",
  ].join("\n");
}

bot.onText(/\/start|\/help/i, (msg) => {
  bot.sendMessage(msg.chat.id, helpText(), { parse_mode: "Markdown" });
});

bot.onText(/\/whoami/i, (msg) => {
  const fromId = msg.from?.id;
  bot.sendMessage(msg.chat.id, `🆔 Seu id é: ${fromId}`);
});

bot.onText(/\/admins/i, (msg) => {
  const fromId = msg.from?.id;
  if (!isSuper(fromId)) return;
  bot.sendMessage(msg.chat.id, `✅ ADMINS:\n${[...ADMINS].join("\n")}`);
});

bot.onText(/\/allow\s+(\d+)/i, (msg, match) => {
  const fromId = msg.from?.id;
  if (!isSuper(fromId)) return;

  const id = Number(match[1]);
  ADMINS.add(id);
  ADMINS.add(SUPER_ADMIN_ID);
  saveAdmins(ADMINS);

  bot.sendMessage(msg.chat.id, `✅ Autorizado: ${id}`);
});

bot.onText(/\/deny\s+(\d+)/i, (msg, match) => {
  const fromId = msg.from?.id;
  if (!isSuper(fromId)) return;

  const id = Number(match[1]);
  if (id === SUPER_ADMIN_ID) return bot.sendMessage(msg.chat.id, "❌ Não pode remover o super-admin.");

  ADMINS.delete(id);
  ADMINS.add(SUPER_ADMIN_ID);
  saveAdmins(ADMINS);

  bot.sendMessage(msg.chat.id, `✅ Removido: ${id}`);
});

bot.on("message", async (msg) => {
  const fromId = msg.from?.id;
  const chatId = msg.chat?.id;
  if (!fromId || !chatId) return;

  // ignora comandos
  if (msg.text && msg.text.startsWith("/")) return;

  // 🔒 só admins geram file_id
  if (!isAdmin(fromId)) return;

  const out = [];

  if (msg.photo && msg.photo.length) {
    const best = msg.photo[msg.photo.length - 1];
    out.push(`🖼️ photo file_id:\n\`${best.file_id}\``);
  }
  if (msg.video_note) out.push(`🔵 video_note file_id:\n\`${msg.video_note.file_id}\``);
  if (msg.voice) out.push(`🗣️ voice file_id:\n\`${msg.voice.file_id}\``);
  if (msg.audio) out.push(`🎧 audio file_id:\n\`${msg.audio.file_id}\``);
  if (msg.video) out.push(`🎬 video file_id:\n\`${msg.video.file_id}\``);
  if (msg.document) out.push(`📄 document file_id:\n\`${msg.document.file_id}\``);
  if (msg.animation) out.push(`🎞️ animation file_id:\n\`${msg.animation.file_id}\``);
  if (msg.sticker) out.push(`🧩 sticker file_id:\n\`${msg.sticker.file_id}\``);

  if (!out.length) {
    await bot.sendMessage(chatId, "ℹ️ Não veio mídia reconhecida.");
    return;
  }

  await bot.sendMessage(
    chatId,
    ["✅ *file_id (copie e cole no painel):*", "", ...out, "", "⚠️ Funciona só neste bot/token."].join("\n\n"),
    { parse_mode: "Markdown" }
  );
});

bot.on("polling_error", (err) => console.error("❌ polling_error:", err.message));
