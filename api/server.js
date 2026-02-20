// api/server.js
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
app.use(express.json({ limit: "2mb" }));
app.set("trust proxy", 1);

// FRONT
app.use(express.static(path.join(__dirname, "..", "public")));

const queue = new Queue("disparos", { connection });

// upload para /tmp
const upload = multer({ dest: "/tmp" });

app.get("/health", (_req, res) => res.json({ ok: true }));

// upload de arquivo (retorna path local pro worker ler)
app.post("/upload", upload.single("file"), (req, res) => {
  if (!req.file) return res.status(400).json({ ok: false, error: "Arquivo não enviado" });
  return res.json({
    ok: true,
    tmpPath: req.file.path,
    originalName: req.file.originalname,
    mimetype: req.file.mimetype,
    size: req.file.size,
  });
});

// cria campanha: recebe leads e cria jobs
app.post("/enqueue", async (req, res) => {
  try {
    const { botToken, type, payload, leads, limit, campaignId } = req.body || {};

    if (!Array.isArray(leads) || !leads.length) {
      return res.status(400).json({ ok: false, error: "leads vazio" });
    }
    if (!type) return res.status(400).json({ ok: false, error: "type ausente" });

    const jobs = leads.map((chatId) => ({
      name: `send:${type}`,
      data: {
        chatId,
        botToken,
        type,
        payload,
        limit,
        campaignId,
      },
      opts: {
        removeOnComplete: true,
        removeOnFail: 1000,
      },
    }));

    await queue.addBulk(jobs);
    res.json({ ok: true, queued: jobs.length });
  } catch (e) {
    console.error("❌ enqueue error:", e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, () => console.log("✅ API online na porta", PORT));
