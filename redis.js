// redis.js
require("dotenv").config();

const url = (process.env.REDIS_URL || "").trim();
if (url) module.exports = url; // BullMQ e ioredis aceitam string

module.exports = {
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT) || 6379,
  username: process.env.REDIS_USERNAME || "default",
  password: process.env.REDIS_PASSWORD,
  maxRetriesPerRequest: null,
  enableReadyCheck: true,
};
