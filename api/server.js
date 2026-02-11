require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { Queue } = require("bullmq");
const connection = require("../redis");

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const queue = new Queue("disparos", { connection });

app.get("/health", (req, res) => res.json({ ok: true }));

function normalizeType(tipo) {
  // do front: "texto/imagem/video/audio/documento"
  // pro worker: "text/photo/video/audio/document"
  const map = {
    texto: "text",
    imagem: "photo",
    video: "video",
    audio: "audio",
    voz: "voice",
    video_note: "video_note",
    documento: "document",
  };
  return map[tipo] || tipo; // se já vier no padrão do worker, mantém
}

app.post("/disparar", async (req, res) => {
  const { tipo, mensagem, fileId, ids, options } = req.body || {};

  if (!tipo) return res.status(400).json({ ok: false, error: "tipo é obrigatório" });
  if (!Array.isArray(ids) || ids.length === 0) {
    return res.status(400).json({ ok: false, error: "ids deve ser um array com pelo menos 1 item" });
  }

  const type = normalizeType(tipo);

  // validações
  if (type !== "text" && !fileId) {
    return res.status(400).json({ ok: false, error: "fileId é obrigatório quando tipo != texto" });
  }
  if (type === "text" && (!mensagem || !String(mensagem).trim())) {
    return res.status(400).json({ ok: false, error: "mensagem é obrigatória quando tipo = texto" });
  }

  let total = 0;

  for (const raw of ids) {
    const chatId = String(raw).trim();
    if (!chatId) continue;

    // Monta job no formato do seu worker
    const jobData =
      type === "text"
        ? {
            chatId,
            type: "text",
            payload: { text: mensagem, options: options || undefined },
          }
        : {
            chatId,
            type,
            payload: {
              file: fileId,              // aqui entra o file_id do telegram
              caption: mensagem || "",   // caption opcional
              options: options || undefined,
            },
          };

    await queue.add("envio", jobData);
    total++;
  }

  return res.json({ ok: true, total });
});

const PORT = process.env.API_PORT || 3000;
app.listen(PORT, () => console.log("✅ API rodando na porta", PORT));
