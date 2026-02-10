const { Queue } = require('bullmq');
const connection = require('./redis');

const queue = new Queue('disparos', { connection });

const IDS = [
  1264397026,
  1532301009,
];

(async () => {
  for (const chatId of IDS) {
    await queue.add('envio', {
      chatId,
      mensagem: '🚀 TESTE REAL — chegou?',
    });
  }

  console.log(`✅ ${IDS.length} jobs enviados para a fila`);
  process.exit(0);
})();

