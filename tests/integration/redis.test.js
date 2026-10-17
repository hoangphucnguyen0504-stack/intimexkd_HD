'use strict';
// Explicitly isolated local Redis only; never loads backend .env or Neon URLs.
const {test} = require('node:test'); const assert = require('node:assert/strict');
const {randomUUID} = require('node:crypto');
const {connect, queues: createQueues} = require('../../src/queues');
const {createStore} = require('../../src/services/store');
const {createGovernor} = require('../../src/services/governor');
const redisUrl = process.env.HDOI_TEST_REDIS_URL;
test('real Redis: parallel dedup, intent restart recovery, entity ordering and shared quota', {skip: !redisUrl}, async t => {
  const url = new URL(redisUrl);
  assert.ok(['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname), 'Only explicitly configured local disposable Redis is allowed');
  const config = {redisUrl, prefix: 'hdoi-test-' + randomUUID(), reads: 2, writes: 2};
  const redis = connect(config), queues = createQueues(config, redis), store = createStore(redis, queues, config);
  t.after(async () => {
    await Promise.all(Object.values(queues).map(q => q.close()));
    let cursor = '0';
    do {const [next, keys] = await redis.scan(cursor, 'MATCH', config.prefix + ':*', 'COUNT', 100); cursor = next; if (keys.length) await redis.del(...keys);} while (cursor !== '0');
    await redis.quit();
  });
  const input = {type: 'qr-recovery', key: 'one', entityKey: 'entity-one', payload: {key: 'one'}};
  const receipts = await Promise.all(Array.from({length: 100}, () => store.submit(input)));
  assert.equal(new Set(receipts.map(row => row.id)).size, 1);
  await assert.rejects(store.submit({...input, payload: {key: 'changed'}}), /IDEMPOTENCY_CONFLICT/);
  // Durable intent survives a service restart before Queue.add is called.
  const restarted = createStore(redis, queues, config);
  assert.equal(await restarted.dispatch(), 1);
  assert.equal(await restarted.dispatch(), 0);
  const first = await queues.background.getJob(receipts[0].id);
  assert.ok(first);
  const second = await store.submit({...input, key: 'two', payload: {key: 'two'}});
  const next = await store.get(second.id);
  assert.equal(await store.ready(next.data), false);
  await store.status(first.data, 'completed');
  assert.equal(await store.ready(next.data), true);
  const limit = createGovernor(redis, config);
  await limit('read'); await limit('read');
  await assert.rejects(limit('read'), error => error.retryable && error.retryAfterMs > 0);
});
