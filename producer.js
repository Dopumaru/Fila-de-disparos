const { Queue } = require("bullmq");
const connection = require("./redis");

const queue = new Queue("disparos", { connection });


const IDS = [
  1532301009,
  //1264397026,
];


const MODO = "photo"; // text | audio | video | voice | video_note | photo

const PRESETS = {
  text: (chatId) => ({
    chatId,
    type: "text",
    payload: { text: "teste final!" },
  }),

  audio: (chatId) => ({
    chatId,
    type: "audio",
    payload: {
    
      file: "AwACAgEAAxkBAAMZaYwPeApVGlZMCDEAAejJm0q77vuBAAITCAACz01gRInXewm2P1ImOgQ",
      caption: "🎧 TESTE AUDIO",
    },
  }),

  video: (chatId) => ({
    chatId,
    type: "video",
    payload: {
      file: "BAACAgEAAxkBAAMbaYwQVZTksfV2eAABM6lIj1FZX6oRAAIUCAACz01gRJSsl6jiSWfDOgQ",
      caption: "🎬 TESTE VIDEO",
    },
  }),

  voice: (chatId) => ({
    chatId,
    type: "voice",
    payload: {
      file: "AwACAgEAAxkBAAMZaYwPeApVGlZMCDEAAejJm0q77vuBAAITCAACz01gRInXewm2P1ImOgQ",
    },
  }),

  video_note: (chatId) => ({
    chatId,
    type: "video_note",
    payload: {
      file: "DQACAgEAAxkBAAMWaYwOp0jRA-n37Nusjkj50yZV_KMAAhIIAALPTWBEO__usJ_gr-A6BA",
    },
  }),

   photo: (chatId) => ({
    chatId,
    type: "photo",
    payload: {
    file: "https://picsum.photos/600/600",
    caption: "🖼️ TESTE FOTO",
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
