'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const {createHash} = require('node:crypto');
const {createCoordination, keyOf, lockKey} = require('../src/services/coordination');
const {createRouter} = require('../src/routes/coordination');
const config = {prefix: 'isolated', reads: 40, writes: 35};
const digest = key => createHash('sha256').update(key).digest('hex');
test('JSON state, batch entries and missing TTL use isolated hashed keys', async () => {
  const values = new Map();
  const redis = {
    set: async (key, value, ex, ttl) => {assert.equal(ex, 'EX'); assert.equal(ttl, 60); values.set(key, value);},
    get: async key => values.get(key) ?? null,
    mget: async (...keys) => keys.map(key => values.get(key) ?? null),
    pttl: async () => -2,
    del: async (...keys) => keys.reduce((n, key) => n + Number(values.delete(key)), 0)
  };
  const {execute} = createCoordination(redis, config);
  await execute({op: 'set', key: '../jobs', value: {a: 1}, ttl: 60});
  assert.ok(values.has('isolated:technical:' + digest('../jobs')));
  assert.equal(keyOf('../jobs', config), createCoordination(redis, config).keyOf('../jobs'));
  assert.equal(lockKey('name', config), createCoordination(redis, config).lockKey('name'));
  assert.deepEqual(await execute({op: 'get', key: '../jobs'}), {a: 1});
  assert.deepEqual(await execute({op: 'getMany', keys: ['missing', '../jobs', '../jobs']}), [['../jobs', {a: 1}]]);
  assert.equal(await execute({op: 'ttl', key: 'missing'}), 0);
  assert.equal(await execute({op: 'removeMany', keys: ['../jobs']}), 1);
});
test('all invalid operations fail before Redis and batches validate before writes', async () => {
  const {execute} = createCoordination(new Proxy({}, {get() {return () => {throw Error('REDIS_MUST_NOT_BE_USED');};}}), config);
  for (const body of [null, [], {op: 'eval'}, {op: 'get', key: ''}, {op: 'get', key: 'a\n'},
    {op: 'set', key: 'k', ttl: 0, value: 1}, {op: 'set', key: 'k', ttl: 1},
    {op: 'increment', key: 'k', ttl: 1.5}, {op: 'getMany', keys: Array(501).fill('x')},
    {op: 'lockAcquire', name: 'n', owner: 'o', ttlMs: 180001},
    {op: 'lockRenew', name: 'n', owner: '', ttlMs: 1000},
    {op: 'quotaReserve', kind: 'other', limit: 40}, {op: 'quotaReserve', kind: 'read', limit: 51},
    {op: 'setMany', items: [{key: 'valid', value: 1, ttl: 60}, null]},
    {op: 'set', key: 'k', ttl: 60, value: 'x'.repeat(300 * 1024)}]) {
    await assert.rejects(execute(body), error => error.status === 400);
  }
});
test('server-owned evaluator receives atomic counter and owner-safe lock scripts', async () => {
  const calls = [];
  let result = [3, 42];
  const {execute} = createCoordination({eval: async (...args) => {calls.push(args); return result;}}, config);
  assert.deepEqual(await execute({op: 'incrementWindow', key: 'rate/x', ttl: 60}), {count: 3, ttl: 42});
  assert.match(calls.at(-1)[0], /INCR/); assert.match(calls.at(-1)[0], /EXPIRE/);
  assert.equal(await execute({op: 'increment', key: 'rate/x', ttl: 60}), 3);
  result = 0;
  assert.equal(await execute({op: 'decrement', key: 'rate/x'}), 0);
  for (const op of ['lockAcquire', 'lockRenew', 'lockRelease']) {
    result = 1;
    assert.equal(await execute({op, name: 'sheet/x', owner: 'token', ttlMs: 180000}), true);
    assert.equal(calls.at(-1)[2], 'isolated:lock:' + digest('sheet/x'));
    assert.equal(calls.at(-1)[3], 'token');
    assert.match(calls.at(-1)[0], op === 'lockAcquire' ? /'NX','PX'/ : /==ARGV\[1\]/);
    result = 0;
    assert.equal(await execute({op, name: 'sheet/x', owner: 'other', ttlMs: 1}), false);
  }
});
test('quota shares worker buckets, clamps limit and atomically checks cooldown', async () => {
  const calls = [];
  let delay = 0;
  const {execute} = createCoordination({eval: async (...args) => {calls.push(args); return delay;}}, config);
  assert.deepEqual(await execute({op: 'quotaReserve', kind: 'write', limit: 50}), {accepted: true, retryAfterSeconds: 0});
  const [script, count, bucket, cooldown, limit] = calls[0];
  assert.equal(count, 2); assert.equal(bucket, 'isolated:quota:write'); assert.equal(limit, 35);
  assert.equal(cooldown, 'isolated:technical:' + digest('sheets-cooldown/write'));
  assert.match(script, /tonumber\(ARGV\[3\]\)/); assert.doesNotMatch(script, /redis\.call\('TIME'\)/);
  assert.match(script, /ZREMRANGEBYSCORE/);
  assert.equal(Number.isSafeInteger(calls[0][6]), true);
  assert.match(script, /cjson.decode/);
  delay = 12;
  assert.deepEqual(await execute({op: 'quotaReserve', kind: 'read', limit: 20}), {accepted: false, retryAfterSeconds: 12});
});
test('router returns envelope, rejects invalid JSON and forwards Redis errors', async t => {
  const app = express();
  app.use('/v1/coordination', createRouter({config, redis: {get: async key => {
    if (key.endsWith(digest('broken'))) throw Error('offline');
    return null;
  }}}));
  app.use((error, _req, res, _next) => res.status(error.status || 503).json({ok: false}));
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const post = body => fetch('http://127.0.0.1:' + server.address().port + '/v1/coordination', {
    method: 'POST', headers: {'content-type': 'application/json'}, body
  });
  const response = await post(JSON.stringify({op: 'get', key: 'missing'}));
  assert.deepEqual(await response.json(), {ok: true, value: null});
  assert.equal((await post('{')).status, 400);
  assert.equal((await post('{"op":"eval"}')).status, 400);
  assert.equal((await post(JSON.stringify({op: 'get', key: 'broken'}))).status, 503);
  assert.equal((await post(JSON.stringify({op: 'get', key: 'x'.repeat(310 * 1024)}))).status, 413);
});
