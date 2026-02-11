const { Queue } = require("bullmq");
const connection = require("./redis");

const queue = new Queue("disparos", { connection });


const IDS = [
  1532301009,
  1264397026,
];


const MODO = "text"; // text | audio | video | voice | video_note

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
    
      file: "COLE_AQUI_FILE_ID_OU_URL",
      caption: "🎧 TESTE AUDIO",
    },
  }),

  video: (chatId) => ({
    chatId,
    type: "video",
    payload: {
      file: "COLE_AQUI_FILE_ID_OU_URL",
      caption: "🎬 TESTE VIDEO",
    },
  }),

  voice: (chatId) => ({
    chatId,
    type: "voice",
    payload: {
      file: "COLE_AQUI_FILE_ID_VOICE",
    },
  }),

  video_note: (chatId) => ({
    chatId,
    type: "video_note",
    payload: {
      file: "COLE_AQUI_FILE_ID_BOLINHA",
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
