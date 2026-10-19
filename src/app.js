'use strict';
const express = require('express');
const helmet = require('helmet');
const {config: loadConfig} = require('./config');
const {connect, queues: createQueues} = require('./queues');
const {createStore, publicStatus} = require('./services/store');
const {hash} = require('./utils/protocol');
function createApp({redis, queues, store, config}) {
  const app = express();
  app.disable('x-powered-by'); app.use(helmet());
  app.get('/health/live', (_req, res) => res.json({ok: true}));
  app.use(require('./middleware/auth')(config.token));
  app.use(express.json({limit: '2mb', strict: true}));
  const route = handler => (req, res, next) => Promise.resolve(handler(req, res)).catch(next);
  const coordination = require('./services/coordination').createCoordination(redis, config);
  const outbox = require('./services/sheetOutbox').createSheetOutbox({redis, store, queues});
  const declarations = require('./services/declarations').createDeclarations({redis, config, store});
  app.post('/v1/coordination', route(async (req, res) => {
    const body = req.body;
    if (body.namespace !== config.namespace) return res.status(403).json({ok:false,code:'NAMESPACE_MISMATCH'});
    let value;
    if (['declarationAccept','declarationGet','declarationUpdate'].includes(body.op)) value = await declarations.execute(body);
    else if (body.op === 'commit') value = await store.commit(body);
    else if (typeof body.op === 'string' && body.op.startsWith('sheet')) value = await outbox.execute(body.op, body);
    else if (body.op === 'queueDiagnostics') {
      const groups=[];
      for(const kind of ['sheets','background'])for(const state of ['waiting','delayed','active','completed','failed','unknown']){
        const key=store.base+'queue-status:'+kind+':'+state,count=await redis.zcard(key);
        if(!count)continue;
        const [id]=await redis.zrange(key,0,0),first=id&&await store.get(id);
        groups.push({queue:kind==='background'?'jobs':'sheets',status:require('./services/sheetOutbox').mapStatus(state),count,
          attemptsBeyondFirst:null,oldestOutstandingAgeMs:['waiting','delayed','active','unknown'].includes(state)&&first?Math.max(0,Date.now()-Number(first.createdAt)):null,
          averageStartDelayMs:null,manualReconciliation:state==='unknown'?count:0,expiredLeases:null});
      }
      value={available:true,provider:'hdoi',observedAt:new Date().toISOString(),scope:'current namespace; Redis status indexes; null means not measured',groups,
        dispatchPending:await redis.zcard(store.base+'dispatch')};
    }
    else if (body.op === 'writeFailure') {
      const code = String(body.entry?.code || 'SHEETS_WRITE_FAILED').slice(0,100);
      await redis.xadd(config.prefix+':write-failures','MAXLEN','~',1000,'*','code',code,'at',String(Date.now())); value = true;
    } else value = await coordination.execute(body);
    res.json({ok:true,value});
  }));
  app.get('/health/ready', route(async (_req, res) => {
    await redis.ping();
    const durableDeclarations = await declarations.health();
    res.json({ok: true, durableDeclarations});
  }));
  app.post('/v1/jobs', route(async (req, res) => {
    const key = config.prefix + ':api-window';
    const hits = await redis.eval("local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('PEXPIRE',KEYS[1],1000) end; return n", 1, key);
    if (hits > 100) return res.status(429).set('Retry-After', '1').json({ok: false, code: 'QUEUE_RATE_LIMITED'});
    res.status(202).json({ok: true, ...await store.submit(req.body)});
  }));
  app.get('/v1/jobs', route(async (req, res) => {
    const offset = Math.max(0, Math.min(100000, Number(req.query.offset) || 0));
    const limit = Math.max(1, Math.min(50, Number(req.query.limit) || 20));
    if (!Number.isInteger(offset) || !Number.isInteger(limit)) throw Object.assign(Error('INVALID_PAGE'), {status: 400});
    res.json({ok: true, items: await store.history(offset, limit)});
  }));
  app.post('/v1/jobs/by-key/retry', route(async (req, res) => {
    if (typeof req.body.key !== 'string') throw Object.assign(Error('INVALID_KEY'), {status: 400});
    res.json({ok: true, ...await store.retry(hash(req.body.key))});
  }));
  app.post('/v1/jobs/by-key/resume', route(async (req, res) => {
    const result = await store.commit({idempotencyKey:require('node:crypto').randomUUID(),operations:[{kind:'control',action:'resume',key:req.body.key}]});
    res.status(202).json({ok:true,status:'pending',accepted:result.accepted});
  }));
  app.get('/v1/jobs/:id', route(async (req, res) => {
    const row = await store.get(req.params.id);
    if (!row) return res.status(404).json({ok: false, code: 'JOB_NOT_FOUND'});
    const job = await queues[row.queue].getJob(row.id);
    res.json({ok: true, ...publicStatus(row), bullState: job ? await job.getState() : 'dispatch-pending', attempts: job?.attemptsMade || 0,
      events: await store.events(row.id)});
  }));
  app.post('/v1/jobs/:id/reconcile', route(async (req, res) => {
    if (typeof req.body.reason !== 'string' || req.body.reason.trim().length < 8) throw Object.assign(Error('REASON_REQUIRED'), {status: 400});
    const row = await store.get(req.params.id);
    if (row?.data.type !== 'sheet-write') throw Object.assign(Error('MANUAL_DELIVERY_CHECK_REQUIRED'), {status: 409});
    res.json({ok: true, ...await store.retry(req.params.id, true)});
  }));
  app.get('/v1/metrics', route(async (_req, res) => {
    let oldestOutstandingAgeMs = null, unknownCount = 0;
    for (const kind of ['sheets', 'background']) {
      unknownCount += Number(await redis.zcard(store.base + 'queue-status:' + kind + ':unknown') || 0);
      for (const state of ['waiting', 'delayed', 'active', 'unknown']) {
        const [id] = await redis.zrange(store.base + 'queue-status:' + kind + ':' + state, 0, 0);
        const first = id && await store.get(id);
        if (!first) continue;
        const age = Math.max(0, Date.now() - Number(first.createdAt));
        if (oldestOutstandingAgeMs == null || age > oldestOutstandingAgeMs) oldestOutstandingAgeMs = age;
      }
    }
    res.json({ok: true, dispatchPending: await redis.zcard(store.base + 'dispatch'), oldestOutstandingAgeMs, unknownCount,
      sheets: queues.sheets ? await queues.sheets.getJobCounts() : {}, background: queues.background ? await queues.background.getJobCounts() : {}});
  }));
  app.use((err, _req, res, _next) => {
    const status = err.status || (err.type === 'entity.parse.failed' ? 400 : 503);
    if (status >= 500) console.error(JSON.stringify({event: 'queue-api-error', code: String(err.message || 'QUEUE_UNAVAILABLE').slice(0, 300)}));
    res.status(status).json({ok: false, code: status < 500 ? err.message : 'QUEUE_UNAVAILABLE'});
  });
  return app;
}
function waitForRedisReady(redis, timeoutMs = 15000) {
  if (redis.status === 'ready') return Promise.resolve();
  return new Promise((resolve, reject) => {
    const finish = error => {
      clearTimeout(timer);
      redis.off('ready', onReady);
      redis.off('end', onEnd);
      redis.off('error', onError);
      if (error) reject(error); else resolve();
    };
    const onReady = () => finish();
    const onEnd = () => finish(Object.assign(new Error('Redis connection closed'), {code: 'REDIS_CONNECTION_CLOSED'}));
    // connect() logs sanitized errors; ioredis can reconnect until end/timeout.
    const onError = () => {};
    const timer = setTimeout(() => finish(Object.assign(new Error('Redis readiness timed out'), {code: 'REDIS_READY_TIMEOUT'})), timeoutMs);
    redis.on('ready', onReady);
    redis.on('end', onEnd);
    redis.on('error', onError);
    if (redis.status === 'ready') onReady();
    else if (redis.status === 'end') onEnd();
  });
}
function logStartupFailure(error) {
  const allowed = ['REDIS_READY_TIMEOUT', 'REDIS_CONNECTION_CLOSED', 'ECONNREFUSED', 'ETIMEDOUT', 'EAI_AGAIN', 'EADDRINUSE'];
  const code = allowed.includes(error?.code) ? error.code : 'STARTUP_FAILED';
  console.error(JSON.stringify({event: 'queue-api-start-failed', code}));
}
async function main() {
  const config = loadConfig(), redis = connect(config), queues = createQueues(config, redis);
  const store = createStore(redis, queues, config);
  await waitForRedisReady(redis);
  await redis.ping();
  const server = createApp({config, redis, queues, store}).listen(config.port, '0.0.0.0');
  const stop = () => server.close(async () => {await Promise.all(Object.values(queues).map(q => q.close())); await redis.quit();});
  process.once('SIGTERM', stop); process.once('SIGINT', stop);
}
if (require.main === module) {
  main().catch(error => {
    logStartupFailure(error);
    process.exit(1);
  });
}
module.exports = {createApp, waitForRedisReady, logStartupFailure};
