// redis.js
// Conexão SOMENTE para BullMQ (Queue/Worker).
// Importante: maxRetriesPerRequest precisa ser null pro BullMQ.

const connection = {
  host: process.env.REDIS_HOST || "redis-fila",
  port: Number(process.env.REDIS_PORT || 6379),
  username: (process.env.REDIS_USERNAME || "default").trim(),
  password: (process.env.REDIS_PASSWORD || "").trim() || undefined,
  db: Number.isFinite(Number(process.env.REDIS_DB)) ? Number(process.env.REDIS_DB) : 0,

  // ✅ BullMQ exige isso
  maxRetriesPerRequest: null,
  enableReadyCheck: true,
};

module.exports = connection;
