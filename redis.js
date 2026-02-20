// redis.js
require("dotenv").config();

function buildRedisUrl() {
  // Prioridade total pro REDIS_URL (recomendado)
  const url = (process.env.REDIS_URL || "").trim();
  if (url) return url;

  // Fallback (se você ainda usa host/port/password)
  const host = (process.env.REDIS_HOST || "127.0.0.1").trim();
  const port = Number(process.env.REDIS_PORT || 6379);
  const password = (process.env.REDIS_PASSWORD || "").trim();
  const db = Number.isFinite(Number(process.env.REDIS_DB))
    ? Number(process.env.REDIS_DB)
    : 0;

  const auth = password ? `:${encodeURIComponent(password)}@` : "";
  return `redis://${auth}${host}:${port}/${db}`;
}

// ✅ Exporta CONFIG pro BullMQ (Queue/Worker)
// (BullMQ recomenda passar connection options/url, não um IORedis client já criado)
const redisUrl = buildRedisUrl();

console.log("✅ BullMQ redis:", redisUrl.replace(/:\/\/:(.*?)@/, "://:***@"));

module.exports = {
  connection: redisUrl,
};
