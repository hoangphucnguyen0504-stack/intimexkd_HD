'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {createStore, COMMIT, DISPATCH_ACK} = require('../src/services/store');
const {config} = require('../src/config');
const {hash, validate} = require('../src/utils/protocol');

function sheet(key, recordId) {
  return validate({
    type: 'sheet-write', key,
    payload: {spreadsheetId: '1234567890', sheet: 'Vehicles', recordId, op: 'append', rowValues: [recordId, 1]},
  });
}

function alreadyExists(id) {
  return Object.assign(Error('Job ' + id + ' already exists'), {name: 'JobIdAlreadyExistsError'});
}

function memoryRedis(rows, pending) {
  const receipts = new Map();
  return {
    async hgetall(key) {
      const row = rows.get(key);
      return row ? {data: JSON.stringify(row.data), queue: row.queue, status: row.status || 'waiting', createdAt: '1'} : {};
    },
    async zrange(key) { return key.endsWith('dispatch') ? pending.slice() : []; },
    async eval(script, number, ...args) {
      if (script === COMMIT) return args.at(-1);
      assert.equal(script, DISPATCH_ACK);
      assert.equal(number, 1);
      const id = args[1], at = pending.indexOf(id);
      if (at >= 0) pending.splice(at, 1);
      return at >= 0 ? 1 : 0;
    },
    receipts,
  };
}

test('config enables dispatch-on-commit unless DISPATCH_ON_COMMIT=0', () => {
  const env = {REDIS_URL: 'redis://127.0.0.1:6379', QUEUE_API_TOKEN: 'x'.repeat(32)};
  assert.equal(config(env).dispatchOnCommit, true);
  assert.equal(config(env).dispatchTimeoutMs, 1500);
  assert.equal(config({...env, DISPATCH_ON_COMMIT: '0'}).dispatchOnCommit, false);
  assert.equal(config({...env, DISPATCH_ON_COMMIT: 'false'}).dispatchOnCommit, false);
  assert.equal(config({...env, DISPATCH_TIMEOUT_MS: '800'}).dispatchTimeoutMs, 800);
});

test('crash after BullMQ add before dispatched flag: retry keeps one job', async () => {
  const data = sheet('write-crash', 'V1');
  const recordKey = 'test:service:record:' + data.id;
  const pending = [data.id];
  const jobs = new Map();
  let adds = 0, failAck = true;
  const redis = memoryRedis(new Map([[recordKey, {data, queue: 'sheets'}]]), pending);
  const origEval = redis.eval.bind(redis);
  redis.eval = async (script, number, ...args) => {
    if (script === DISPATCH_ACK && failAck) { failAck = false; throw Error('process died after add'); }
    return origEval(script, number, ...args);
  };
  const store = createStore(redis, {sheets: {add: async (_type, job, options) => {
    adds++;
    assert.equal(options.jobId, data.id);
    if (jobs.has(options.jobId)) throw alreadyExists(options.jobId);
    jobs.set(options.jobId, job);
  }}}, {prefix: 'test'});
  await assert.rejects(() => store.dispatch(), /process died after add/);
  assert.equal(jobs.size, 1);
  assert.equal(pending.length, 1);
  assert.equal(await store.dispatch(), 1);
  assert.equal(await store.dispatch(), 0);
  assert.equal(jobs.size, 1);
  assert.equal(adds, 2);
  assert.equal(pending.length, 0);
});

test('repeat commit after completed job does not enqueue a second BullMQ job', async () => {
  const data = sheet('write-done', 'V1');
  const recordKey = 'test:service:record:' + data.id;
  const rows = new Map([[recordKey, {data, queue: 'sheets', status: 'completed'}]]);
  const pending = [];
  const jobs = new Map([[data.id, data]]);
  let adds = 0;
  const redis = memoryRedis(rows, pending);
  redis.eval = async (script, number, ...args) => {
    if (script === COMMIT) {
      const actions = JSON.parse(args[5]);
      for (const action of actions) {
        if (action[0] === 'JOB' && !rows.has(action[1])) {
          rows.set(action[1], {data: JSON.parse(action[3]), queue: action[4], status: 'waiting'});
          pending.push(action[6]);
        }
      }
      return args.at(-1);
    }
    return memoryRedis(rows, pending).eval(script, number, ...args);
  };
  const store = createStore(redis, {sheets: {add: async (_type, job, options) => {
    adds++;
    if (jobs.has(options.jobId)) throw alreadyExists(options.jobId);
    jobs.set(options.jobId, job);
  }}}, {prefix: 'test', dispatchOnCommit: true, dispatchTimeoutMs: 200});
  const operation = {kind: 'sheet', data: {type: data.type, key: data.key, payload: data.payload}};
  await store.commit({idempotencyKey: 'repeat-after-done', operations: [operation]});
  assert.equal(adds, 0);
  assert.equal(jobs.size, 1);
  assert.equal(pending.length, 0);
});

test('repeat commit after job record deleted recovers one job with the same jobId', async () => {
  const data = sheet('write-deleted', 'V1');
  const recordKey = 'test:service:record:' + data.id;
  const rows = new Map();
  const pending = [];
  const jobs = new Map();
  const redis = memoryRedis(rows, pending);
  redis.eval = async (script, number, ...args) => {
    if (script === COMMIT) {
      const actions = JSON.parse(args[5]);
      for (const action of actions) {
        if (action[0] === 'JOB' && !rows.has(action[1])) {
          rows.set(action[1], {data: JSON.parse(action[3]), queue: action[4], status: 'waiting'});
          pending.push(action[6]);
        }
      }
      return args.at(-1);
    }
    const at = pending.indexOf(args[1]);
    if (at >= 0) pending.splice(at, 1);
    return at >= 0 ? 1 : 0;
  };
  const store = createStore(redis, {sheets: {add: async (_type, job, options) => {
    if (jobs.has(options.jobId)) throw alreadyExists(options.jobId);
    jobs.set(options.jobId, job);
  }}}, {prefix: 'test', dispatchOnCommit: true, dispatchTimeoutMs: 200});
  const operation = {kind: 'sheet', data: {type: data.type, key: data.key, payload: data.payload}};
  await store.commit({idempotencyKey: 'first-life', operations: [operation]});
  assert.equal(jobs.size, 1);
  rows.delete(recordKey);
  jobs.delete(data.id);
  await store.commit({idempotencyKey: 'second-life', operations: [operation]});
  assert.equal(jobs.size, 1);
  assert.equal([...jobs.keys()][0], data.id);
});

test('same-tab jobs keep seq order even when two dispatch loops overlap', async () => {
  const first = sheet('tab-a', 'V1');
  const second = sheet('tab-b', 'V2');
  assert.equal(first.payload.sheet, second.payload.sheet);
  const rows = new Map([
    ['test:service:record:' + first.id, {data: first, queue: 'sheets'}],
    ['test:service:record:' + second.id, {data: second, queue: 'sheets'}],
  ]);
  const pending = [first.id, second.id];
  const order = [];
  let release;
  const hold = new Promise(resolve => { release = resolve; });
  const redis = memoryRedis(rows, pending);
  const store = createStore(redis, {sheets: {add: async (_type, _job, options) => {
    order.push(options.jobId);
    if (options.jobId === first.id && order.filter(id => id === first.id).length === 1) await hold;
  }}}, {prefix: 'test'});
  const run = store.dispatch();
  const overlap = store.dispatch();
  await new Promise(resolve => setImmediate(resolve));
  release();
  await Promise.all([run, overlap]);
  assert.equal(order[0], first.id);
  assert.ok(order.indexOf(second.id) === -1 || order.indexOf(first.id) < order.indexOf(second.id));
});

test('availableAt stays a delay and dependsOn stays on job data', async () => {
  const data = sheet('write-delay', 'V1');
  data.availableAt = Date.now() + 4000;
  data.dependsOn = hash('predecessor-key');
  const recordKey = 'test:service:record:' + data.id;
  const pending = [data.id];
  let options;
  const store = createStore(memoryRedis(new Map([[recordKey, {data, queue: 'sheets'}]]), pending), {
    sheets: {add: async (_type, job, opts) => { options = {delay: opts.delay, dependsOn: job.dependsOn, jobId: opts.jobId}; }},
  }, {prefix: 'test'});
  await store.dispatch();
  assert.equal(options.jobId, data.id);
  assert.ok(options.delay >= 3000);
  assert.equal(options.dependsOn, data.dependsOn);
});

test('dispatch-on-commit uses the same dispatch function; errors do not fail commit', async () => {
  const data = sheet('write-commit', 'V1');
  const recordKey = 'test:service:record:' + data.id;
  const pending = [data.id];
  const rows = new Map([[recordKey, {data, queue: 'sheets'}]]);
  const redis = memoryRedis(rows, pending);
  redis.eval = async (script, number, ...args) => {
    if (script === COMMIT) return args.at(-1);
    throw Error('dispatch redis down');
  };
  const logs = [];
  const original = console.error;
  console.error = value => logs.push(String(value));
  const store = createStore(redis, {sheets: {add: async () => { throw Error('should not add after ack fail'); }}}, {
    prefix: 'test', dispatchOnCommit: true, dispatchTimeoutMs: 200,
  });
  try {
    const receipt = await store.commit({
      idempotencyKey: 'commit-keeps-ok',
      operations: [{kind: 'sheet', data: {type: data.type, key: data.key, payload: data.payload}}],
    });
    assert.equal(receipt.accepted, true);
    assert.equal(receipt.jobs[0].id, data.id);
  } finally { console.error = original; }
  assert.ok(logs.some(line => line.includes('dispatch-on-commit-failed')));
});

test('dispatch-on-commit off skips BullMQ add; timeout still returns commit', async () => {
  const data = sheet('write-flag', 'V1');
  const recordKey = 'test:service:record:' + data.id;
  const pending = [data.id];
  let adds = 0;
  const redis = memoryRedis(new Map([[recordKey, {data, queue: 'sheets'}]]), pending);
  const queues = {sheets: {add: async () => { adds++; await new Promise(resolve => setTimeout(resolve, 100)); }}};
  const off = createStore(redis, queues, {prefix: 'test', dispatchOnCommit: false});
  await off.commit({
    idempotencyKey: 'flag-off',
    operations: [{kind: 'sheet', data: {type: data.type, key: data.key, payload: data.payload}}],
  });
  assert.equal(adds, 0);
  const on = createStore(redis, queues, {prefix: 'test', dispatchOnCommit: true, dispatchTimeoutMs: 30});
  const started = Date.now();
  const receipt = await on.commit({
    idempotencyKey: 'flag-on-timeout',
    operations: [{kind: 'sheet', data: {type: data.type, key: data.key, payload: data.payload}}],
  });
  assert.equal(receipt.accepted, true);
  assert.ok(Date.now() - started < 400);
});
