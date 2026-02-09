const { Queue } = require("bullmq");
const connection = require("./redis");

const queue = new Queue("disparos", { connection });

(async () => {
  await queue.add("envio", {
    id: 1,
    chatId: 1234234, // <-- seu ID 
    mensagem: "🚀 TESTE REAL — chegou?",
  });

  console.log("1 job enviado para a fila");
  process.exit(0);
})();

