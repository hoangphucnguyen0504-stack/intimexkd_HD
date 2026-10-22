'use strict';
function config(env = process.env) {
  const namespace = env.APP_NAMESPACE || 'gate';
  if (!/^[a-zA-Z0-9_-]{1,64}$/.test(namespace)) throw Error('INVALID_NAMESPACE');
  if (!env.REDIS_URL) throw Error('REDIS_URL_REQUIRED');
  if (!env.QUEUE_API_TOKEN || env.QUEUE_API_TOKEN.length < 32 || env.QUEUE_API_TOKEN.startsWith('REPLACE_')) throw Error('QUEUE_API_TOKEN_MIN_32');
  const integer = (name, fallback, max) => {
    const value = Number(env[name] || fallback);
    if (!Number.isSafeInteger(value) || value < 1 || value > max) throw Error('INVALID_' + name);
    return value;
  };
  return {namespace, redisUrl: env.REDIS_URL, token: env.QUEUE_API_TOKEN,
    prefix: 'hdoi-' + namespace, port: integer('PORT', 3100, 65535),
    dispatchOnCommit: env.DISPATCH_ON_COMMIT !== '0' && env.DISPATCH_ON_COMMIT !== 'false',
    dispatchTimeoutMs: integer('DISPATCH_TIMEOUT_MS', 1500, 5000),
    concurrency: integer('WORKER_CONCURRENCY', 2, 16),
    imagesEnabled: env.IMAGE_HOLD_ENABLED === 'true',
    imageStorageDir: env.IMAGE_STORAGE_DIR || '/data/images',
    imageConcurrency: integer('IMAGE_WORKER_CONCURRENCY', 2, 8),
    imageAlertAfterMs: integer('IMAGE_ALERT_AFTER_MS', 900000, 86400000),
    foregroundReserve: integer('SHEETS_FOREGROUND_RESERVE', 8, 40),
    reads: integer('SHEETS_READS_PER_MINUTE', 40, 50),
    writes: integer('SHEETS_WRITES_PER_MINUTE', 40, 50),
    backendUrl: env.BACKEND_URL, backendToken: env.BACKEND_WORKER_TOKEN};
}
module.exports = {config};
