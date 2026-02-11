const IORedis = require("ioredis");

const redis = new IORedis({
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
  enableReadyCheck: true,
});

redis.on("connect", () => {
  console.log(
    `✅ Redis conectado em ${process.env.REDIS_HOST || "127.0.0.1"}:${
      process.env.REDIS_PORT || 6379
    }`
  );
});

redis.on("error", (err) => {
  console.error("❌ Erro no Redis:", err.message);
});

module.exports = redis;

