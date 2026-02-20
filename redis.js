// redis.js
require("dotenv").config();

const connection = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  db: process.env.REDIS_DB ? Number(process.env.REDIS_DB) : undefined,
  maxRetriesPerRequest: null,
  enableReadyCheck: true,
};

module.exports = connection;
