// redis.js
require("dotenv").config();
const IORedis = require("ioredis");

// Suporta REDIS_URL (prioridade) e fallback pra REDIS_HOST/PORT/PASSWORD/DB
function getRedisConnectionOptions() {
  const url = process.env.REDIS_URL;

  if (url && typeof url === "string" && url.trim()) {
    // ioredis aceita string URL, mas bullmq prefere options.
    // Vamos parsear usando URL nativa pra evitar divergência.
    const u = new URL(url);

    const isTls = u.protocol === "rediss:";
    const host = u.hostname;
    const port = u.port ? Number(u.port) : (isTls ? 6380 : 6379);

    // URL pode ser redis://:password@host:port/0
    const password = u.password ? decodeURIComponent(u.password) : undefined;

    // pathname "/0"
    const dbStr = (u.pathname || "").replace("/", "");
    const db = dbStr ? Number(dbStr) : 0;

    const opts = {
      host,
      port,
      password,
      db: Number.isFinite(db) ? db : 0,
      // Recomendações BullMQ
      maxRetriesPerRequest: null,
      enableReadyCheck: true,
      // Se for TLS
      ...(isTls ? { tls: {} } : {}),
    };

    return opts;
  }

  // fallback
  const host = process.env.REDIS_HOST || "127.0.0.1";
  const port = Number(process.env.REDIS_PORT) || 6379;
  const password = process.env.REDIS_PASSWORD || undefined;
  const db = Number(process.env.REDIS_DB) || 0;

  return {
    host,
    port,
    password,
    db,
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
  };
}

const connection = getRedisConnectionOptions();

// Client único (API pode usar direto; worker pode duplicar)
const redis = new IORedis(connection);

redis.on("connect", () => console.log("✅ Redis conectado"));
redis.on("ready", () => console.log("✅ Redis ready"));
redis.on("reconnecting", () => console.log("🔄 Redis reconnecting"));
redis.on("error", (err) => console.error("❌ Redis error:", err?.message || err));

module.exports = { redis, connection };
