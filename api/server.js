require("dotenv").config();
const path = require("path");
const fs = require("fs");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const { Queue } = require("bullmq");
const connection = require("../redis");

const app = express();
app.use(cors());

// garante pasta uploads
const UPLOAD_DIR = path.join(__dirname, "..", "uploads");
fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const upload = multer({ dest: UPLOAD_DIR });

const queue = new Queue("disparos", { connection });

app.get("/health", (req, res) => res.json({ ok: true }));

function maskToken(t) {
  if (!t) return "";
  const s = String(t);
  if (s.length <= 10) return "***";
  return s.slice(0, 4) + "..." + s.slice(-4);
}

function isHttpUrl(u) {
  return /^https?:\/\//i.test(u || "");
}

// Busca username do bot (sem salvar)
async function getBotUsername(botToken) {
  // Node 18+ tem fetch global
  const r = await fetch(`https://api.telegram.org/bot${botToken}/getMe`);
  const data = await r.json().catch(() => null);

  if (!data || !data.ok || !data.result) {
    throw new Error("Token inválido (getMe)");
  }

  return data.result.username; // sem "@"
}

// Converte array buttons -> options.reply_markup.inline_keyboard
function buildOptionsFromButtons(buttons, botUsername) {
  if (!Array.isArray(buttons) || buttons.length === 0) return undefined;

  // 2 botões por linha (fica bonito)
  const inline_keyboard = [];
  for (let i = 0; i < buttons.length; i += 2) {
    const row = [];

    for (let j = i; j < i + 2 && j < buttons.length; j++) {
      const b = buttons[j];
      const text = String(b.text || "").trim();
      const type = String(b.type || "").trim(); // url | start
      const value = String(b.value || "").trim();

      if (!text || !type || !value) continue;

      if (type === "url") {
        if (!isHttpUrl(value)) continue;
        row.push({ text, url: value });
      } else if (type === "start") {
        if (!botUsername) continue;
        // param do start (encode)
        const param = encodeURIComponent(value);
        row.push({ text, url: `https://t.me/${botUsername}?start=${param}` });
      }
    }

    if (row.length) inline_keyboard.push(row);
  }

  if (!inline_keyboard.length) return undefined;

  return {
    reply_markup: {
      inline_keyboard,
    },
  };
}

app.post("/disparar", upload.single("file"), async (req, res) => {
  try {
    const botToken = (req.body.botToken || "").trim();
    const type = (req.body.type || "").trim(); // text/photo/video/audio/document/voice/video_note
    const caption = req.body.caption ?? "";
    const limitMax = Number(req.body.limitMax || 1);
    const limitMs = Number(req.body.limitMs || 1100);

    // ids
    let ids = [];
    try {
      ids = JSON.parse(req.body.ids || "[]");
    } catch {
      return res.status(400).json({ ok: false, error: 'ids precisa ser JSON (ex: ["123","456"]).' });
    }

    // buttons (opcional)
    let buttons = [];
    try {
      buttons = JSON.parse(req.body.buttons || "[]");
    } catch {
      return res.status(400).json({ ok: false, error: "buttons precisa ser JSON." });
    }

    if (!botToken) return res.status(400).json({ ok: false, error: "botToken é obrigatório." });
    if (!type) return res.status(400).json({ ok: false, error: "type é obrigatório." });
    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ ok: false, error: "ids precisa ser array com pelo menos 1 item." });
    }
    if (!(limitMax >= 1) || !(limitMs >= 200)) {
      return res.status(400).json({ ok: false, error: "limitMax>=1 e limitMs>=200." });
    }

    // valida arquivo x tipo
    if (type === "text") {
      if (!String(caption).trim()) {
        return res.status(400).json({ ok: false, error: "Para text, caption (mensagem) é obrigatório." });
      }
    } else {
      if (!req.file) {
        return res.status(400).json({ ok: false, error: "Para mídia/documento, envie o arquivo no campo file." });
      }
    }

    // limita botões
    if (!Array.isArray(buttons)) buttons = [];
    if (buttons.length > 4) buttons = buttons.slice(0, 4);

    // se tiver botão START, precisamos do username
    const hasStart = buttons.some((b) => String(b?.type || "").trim() === "start");
    let botUsername = null;
    if (hasStart) {
      botUsername = await getBotUsername(botToken);
      if (!botUsername) {
        return res.status(400).json({ ok: false, error: "Não consegui obter username do bot para botão START." });
      }
    }

    const options = buildOptionsFromButtons(buttons, botUsername);

    // caminho do arquivo no servidor (worker lê path local)
    const filePath = req.file?.path || null;

    let total = 0;

    for (const rawId of ids) {
      const chatId = String(rawId).trim();
      if (!chatId) continue;

      const jobData =
        type === "text"
          ? {
              chatId,
              botToken,
              limit: { max: limitMax, ms: limitMs },
              type: "text",
              payload: {
                text: caption,
                options, // ✅ injeta botões aqui
              },
            }
          : {
              chatId,
              botToken,
              limit: { max: limitMax, ms: limitMs },
              type,
              payload: {
                file: filePath,
                caption: caption || "",
                options, // ✅ injeta botões aqui
                tempFile: true,
              },
            };

      await queue.add("envio", jobData);
      total++;
    }

    console.log(
      `✅ Enfileirado: total=${total} type=${type} buttons=${buttons.length} token=${maskToken(botToken)}`
    );

    return res.json({ ok: true, total, buttons: buttons.length });
  } catch (err) {
    console.error("❌ /disparar erro:", err.message);
    return res.status(500).json({ ok: false, error: "Erro interno" });
  }
});

const PORT = process.env.API_PORT || 3000;
app.listen(PORT, () => console.log("✅ API rodando na porta", PORT));
