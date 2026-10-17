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
  // Real Lua group commit must reject lost owners before writing TTL or jobs.
  const coordinated=require('../../src/services/coordination').createCoordination(redis,{...config,reads:2,writes:2});
  await coordinated.execute({op:'lockAcquire',name:'record/one',owner:'owner-one',ttlMs:180000});
  const group={idempotencyKey:'atomic-group',locks:[{name:'record/one',owner:'owner-one'}],operations:[
    {kind:'technical',op:'set',key:'test/accepted',value:true,ttl:60},
    {kind:'sheet',logicalSheet:'VehicleTrips',data:{type:'sheet-write',key:'atomic-write',payload:{spreadsheetId:'1234567890',sheet:'Vehicles',recordId:'V1',op:'append',rowValues:['V1',4000]}}},
    {kind:'job',data:{type:'email',key:'atomic-email',payload:{key:'atomic-email'},dependsOn:require('../../src/utils/protocol').hash('atomic-write')}}
  ]};
  await assert.rejects(store.commit({...group,idempotencyKey:'lost-group',locks:[{name:'record/one',owner:'wrong'}]}),/HDOI_LEASE_LOST/);
  assert.equal(await coordinated.execute({op:'get',key:'test/accepted'}),null);
  const accepted=await store.commit(group);
  assert.deepEqual(await store.commit(group),accepted);
  assert.equal(await coordinated.execute({op:'get',key:'test/accepted'}),true);
  const dependent=await store.get(accepted.jobs[1].id);
  assert.equal(await store.ready(dependent.data),false);
  // A stale BullMQ owner cannot publish completion or release the next entity.
  const source=await store.get(accepted.jobs[0].id);
  const lock=config.prefix+':sheets:'+source.id+':lock';await redis.set(lock,'current-token','PX',10000);
  await assert.rejects(store.status(source.data,'completed','',{queueName:'sheets',token:'stale-token'}),/WORKER_LEASE_LOST/);
  assert.equal((await store.get(source.id)).status,'waiting');
  await store.status(source.data,'completed','',{queueName:'sheets',token:'current-token'});
  assert.equal(await store.ready(dependent.data),true);
});
