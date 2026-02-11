const { Queue } = require("bullmq");
const connection = require("./redis");

const queue = new Queue("disparos", { connection });

const IDS = [ 1532301009];


const MODO = "video_note"; // text | audio | video | voice | video_note

const PRESETS = {
  text: (chatId) => ({
    chatId,
    type: "text",
    payload: { text: "✅ TESTE TEXTO OK" },
  }),

  audio: (chatId) => ({
    chatId,
    type: "audio",
    payload: {
      file: "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3",
      caption: "🎧 teste",
    },
  }),

  video: (chatId) => ({
    chatId,
    type: "video",
    payload: {
      file: "https://sample-videos.com/video321/mp4/720/big_buck_bunny_720p_1mb.mp4",
      caption: "🎬 Vídeo",
    },
  }),

  voice: (chatId) => ({
    chatId,
    type: "voice",
    payload: {
      file: "AwACAgEAAxkBAAMPaYwEf1bEVGUJ1y9FNNcTiT-Xj-kAAg8IAALPTWBEAAEhJc8nLFwgOgQ", // ideal: file_id de um ogg/opus
    },
  }),

  video_note: (chatId) => ({
    chatId,
    type: "video_note",
    payload: {
      file: "DQACAgEAAxkBAAMUaYwINfTfKPbIyfQGydOt8oZJ4esAAhEIAALPTWBErF0kSjhT8g46BA",
    },
  }),
};

(async () => {
  const makeJob = PRESETS[MODO];
  if (!makeJob) {
    console.error("❌ MODO inválido:", MODO);
    process.exit(1);
  }

  for (const chatId of IDS) {
    await queue.add("envio", makeJob(chatId));
  }

  console.log(`✅ ${IDS.length} jobs enviados (modo: ${MODO})`);
  process.exit(0);
})();
