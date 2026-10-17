'use strict';
const {Worker, DelayedError, UnrecoverableError} = require('bullmq');
const {config: loadConfig} = require('../config');
const {connect, queues: createQueues} = require('../queues');
const {createStore} = require('../services/store');
const {createGovernor} = require('../services/governor');
const {retryDelay} = require('../utils/protocol');
const {createBackendProcessor} = require('../processors/backend');
const {createGoogleAdapter, createSheetsProcessor} = require('../processors/sheets');
function processor({store, execute, queue}) {
  return async (job, token) => {
    if (!await store.ready(job.data)) {
      await store.status(job.data, 'delayed', 'WAITING_FOR_PREDECESSOR');
      await job.moveToDelayed(Date.now() + 5000, token);
      throw new DelayedError();
    }
    await store.status(job.data, 'active');
    try {
      const result = await execute(job);
      await store.status(job.data, 'completed');
      return result;
    } catch (error) {
      const code = String(error.code || 'PROCESSOR_FAILED').slice(0, 100);
      if (error.unknown || error.retryable !== true) {
        await store.status(job.data, error.unknown ? 'unknown' : 'failed', code);
        throw new UnrecoverableError(code);
      }
      if (job.attemptsMade + 1 >= job.opts.attempts) {
        await store.status(job.data, 'failed', code);
        throw new UnrecoverableError(code);
      }
      // Store Retry-After in durable job data for BullMQ's custom backoff.
      const next = {...job.data, retryAfterMs: error.retryAfterMs || 0};
      await job.updateData(next); job.data = next;
      await store.status(job.data, 'delayed', code);
      throw Object.assign(Error(code), {retryAfterMs: error.retryAfterMs});
    }
  };
}
async function main() {
  const config = loadConfig(), redis = connect(config, true), queues = createQueues(config, redis);
  const store = createStore(redis, queues, config);
  await redis.ping();
  // Global limits are Redis-backed; adding replicas cannot increase them.
  await queues.sheets.setGlobalConcurrency(1);
  await queues.background.setGlobalConcurrency(config.concurrency);
  const google = await createGoogleAdapter();
  const sheets = createSheetsProcessor({google, governor: createGovernor(redis, config)});
  const background = createBackendProcessor(config);
  const workers = Object.entries({sheets, background}).map(([kind, execute]) => {
    const worker = new Worker(kind, processor({store, execute, queue: queues[kind]}), {
      connection: redis, prefix: config.prefix, concurrency: kind === 'sheets' ? 1 : config.concurrency,
      lockDuration: 180000, stalledInterval: 30000, maxStalledCount: 1,
      settings: {backoffStrategy: (attempt, _type, err, job) => retryDelay(attempt, err.retryAfterMs || job?.data.retryAfterMs || 0)}
    });
    worker.on('error', () => console.error(JSON.stringify({event: 'worker-error', queue: kind})));
    worker.on('failed', (job, error) => {
      console.error(JSON.stringify({event: 'job-failed', queue: kind, id: job?.id, code: error.name}));
      // BullMQ itself may fail a job after repeated stalls, bypassing processor.
      if (job && error.message.includes('stalled')) store.status(job.data, 'unknown', 'STALLED_MANUAL_REVIEW').catch(() => {});
    });
    return worker;
  });
  let stopped = false;
  await require('node:fs/promises').writeFile('/tmp/hdoi-worker-heartbeat', String(Date.now()));
  const heartbeat = setInterval(() => require('node:fs').writeFile('/tmp/hdoi-worker-heartbeat', String(Date.now()), () => {}), 10000);
  // Dispatcher only scans Redis's pending-intent index, never Neon or Sheets.
  const dispatch = async () => {
    while (!stopped) {
      try {await store.dispatch();} catch {console.error(JSON.stringify({event: 'dispatch-unavailable'}));}
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  };
  const loop = dispatch();
  async function stop() {
    if (stopped) return; stopped = true; clearInterval(heartbeat);
    await Promise.all(workers.map(worker => worker.close())); await loop;
    await Promise.all(Object.values(queues).map(queue => queue.close())); await redis.quit();
  }
  process.once('SIGTERM', stop); process.once('SIGINT', stop);
}
if (require.main === module) main().catch(() => {console.error('queue-worker-start-failed'); process.exitCode = 1;});
module.exports = {processor};
