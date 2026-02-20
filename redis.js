// redis.js
require("dotenv").config();
const IORedis = require("ioredis");

function getRedisOptions() {
  // ✅ Prioridade: REDIS_URL
  const url = (process.env.REDIS_URL || "").trim();
  if (url) {
    // BullMQ aceita string URL, mas a gente garante os opts que ele exige:
    return {
      // ioredis aceita "url" via lazyConnect? não precisa
      // aqui vamos parsear via IORedis usando string mesmo no create abaixo
      url,
      maxRetriesPerRequest: null,
      enableReadyCheck: true,
    };
  }

  // ✅ Fallback: HOST/PORT/PASSWORD
  return {
    host: process.env.REDIS_HOST || "127.0.0.1",
    port: Number(process.env.REDIS_PORT) || 6379,
    username: process.env.REDIS_USER || undefined, // se usar ACL default
    password: process.env.REDIS_PASSWORD || undefined,
    maxRetriesPerRequest: null,     // 🔥 obrigatório pro BullMQ
    enableReadyCheck: true,
  };
}

// Se veio por URL, instanciamos com string (mais simples) e passamos opts separados
const opts = getRedisOptions();

let redis;
if (opts.url) {
  const { url, ...rest } = opts;
  redis = new IORedis(url, rest);
} else {
  redis = new IORedis(opts);
}

redis.on("connect", () => console.log("✅ Redis conectado"));
redis.on("error", (err) => console.error("❌ Erro no Redis:", err.message));

module.exports = redis;
