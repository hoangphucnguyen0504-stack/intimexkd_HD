'use strict';
const Redis = require('ioredis');
const url = new URL(process.env.REDIS_URL || 'redis://127.0.0.1:6379');
console.log(JSON.stringify({host: url.hostname, port: url.port || '6379'}));
const redis = new Redis(url.toString(), {lazyConnect: true, retryStrategy: () => null, connectTimeout: 5000, commandTimeout: 5000});
redis.on('error', error => console.error(JSON.stringify({code: error.code || 'REDIS_ERROR', message: String(error.message).replace(/rediss?:\/\/\S+/g, '[redacted]')})));
(async () => {
  try {
    await redis.connect();
    console.log(JSON.stringify({ping: await redis.ping()}));
  } catch {
    process.exitCode = 1;
  } finally {
    redis.disconnect();
  }
})();
