// redis.js
const IORedis = require("ioredis");

function buildOptions() {
  // ✅ prioridade: REDIS_URL
  const url = (process.env.REDIS_URL || "").trim();
  if (url) return url;

  // fallback: host/port/password
  return {
    host: process.env.REDIS_HOST || "127.0.0.1",
    port: Number(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD || undefined,
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
  };
}

const redis = new IORedis(buildOptions());

redis.on("connect", () => {
  console.log("✅ Redis conectado");
});
redis.on("error", (err) => {
  console.error("❌ Erro no Redis:", err.message);
});

module.exports = redis;
