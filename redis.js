const IORedis = require('ioredis');

const redis = new IORedis({
  host: process.env.REDIS_HOST || 'redis-fila',
  port: process.env.REDIS_PORT || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
});

module.exports = redis;
