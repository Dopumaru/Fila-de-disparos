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

app.post("/disparar", upload.single("file"), async (req, res) => {
  try {
    // Campos vindo do front
    const botToken = (req.body.botToken || "").trim();
    const type = (req.body.type || "").trim(); // text/photo/video/audio/document/voice/video_note
    const caption = req.body.caption ?? ""; // pode ser vazio
    const limitMax = Number(req.body.limitMax || 1);
    const limitMs = Number(req.body.limitMs || 1100);

    let ids = [];
    try {
      ids = JSON.parse(req.body.ids || "[]");
    } catch {
      return res.status(400).json({ ok: false, error: "ids precisa ser JSON (ex: [\"123\",\"456\"])." });
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

    // caminho do arquivo no servidor (o worker já sabe ler arquivo local)
    const filePath = req.file?.path || null;

    let total = 0;

    for (const rawId of ids) {
      const chatId = String(rawId).trim();
      if (!chatId) continue;

      const jobData =
        type === "text"
          ? {
              chatId,
              // IMPORTANTE: token vai no job (não salvar em DB)
              botToken,
              // limite por token
              limit: { max: limitMax, ms: limitMs },

              type: "text",
              payload: { text: caption },
            }
          : {
              chatId,
              botToken,
              limit: { max: limitMax, ms: limitMs },

              type,
              payload: {
                // worker resolve: URL, path local ou file_id
                file: filePath,
                caption: caption || "",
                // marca pra apagar depois do envio
                tempFile: true,
              },
            };

      await queue.add("envio", jobData);
      total++;
    }

    // NÃO loga token completo
    console.log(`✅ Enfileirado: total=${total} type=${type} token=${maskToken(botToken)}`);

    return res.json({ ok: true, total });
  } catch (err) {
    console.error("❌ /disparar erro:", err.message);
    return res.status(500).json({ ok: false, error: "Erro interno" });
  }
});

const PORT = process.env.API_PORT || 3000;
app.listen(PORT, () => console.log("✅ API rodando na porta", PORT));
