'use strict';
const {validate, hash} = require('../utils/protocol');
// A durable intent precedes BullMQ insertion. A crash between the two is repaired
// by the worker dispatcher; HTTP retries reuse the same ID and immutable digest.
const RESERVE = `
local old=redis.call('HGET',KEYS[1],'fingerprint')
if old then if old~=ARGV[1] then return -1 end return 0 end
local seq=redis.call('INCR',KEYS[3])
redis.call('HSET',KEYS[1],'fingerprint',ARGV[1],'data',ARGV[2],'queue',ARGV[3],'status','waiting','createdAt',ARGV[4],'seq',seq)
redis.call('ZADD',KEYS[2],seq,ARGV[5])
redis.call('ZADD',KEYS[4],seq,ARGV[5])
if ARGV[6]~='' then redis.call('ZADD',KEYS[5],seq,ARGV[5]) end
return 1`;
function createStore(redis, queues, config) {
  const base = config.prefix + ':service:';
  const record = id => base + 'record:' + id;
  const entity = key => base + 'entity:' + hash(key || '');
  async function get(id) {
    if (!/^[a-f0-9]{64}$/.test(id)) return null;
    const row = await redis.hgetall(record(id));
    if (!row.data) return null;
    return {...row, data: JSON.parse(row.data), id};
  }
  async function submit(input) {
    const data = validate(input);
    if (data.dependsOn && !await get(data.dependsOn)) throw Object.assign(Error('DEPENDENCY_NOT_FOUND'), {status: 409});
    const kind = data.type === 'sheet-write' ? 'sheets' : 'background';
    const reserved = await redis.eval(RESERVE, 5, record(data.id), base + 'dispatch', base + 'sequence', base + 'history', entity(data.entityKey),
      data.fingerprint, JSON.stringify(data), kind, Date.now(), data.id, data.entityKey || '');
    if (reserved === -1) throw Object.assign(Error('IDEMPOTENCY_CONFLICT'), {status: 409});
    // Accepted means the Redis durable intent exists, not that Sheets is saved.
    return {id: data.id, status: (await get(data.id)).status, accepted: true};
  }
  async function dispatch(limit = 100) {
    const ids = await redis.zrange(base + 'dispatch', 0, limit - 1);
    for (const id of ids) {
      const row = await get(id);
      if (!row) throw Error('INTENT_MISSING');
      await queues[row.queue].add(row.data.type, row.data, {jobId: id, attempts: row.data.maxAttempts,
        delay: Math.max(0, row.data.availableAt - Date.now())});
      await redis.zrem(base + 'dispatch', id);
    }
    return ids.length;
  }
  async function ready(data) {
    if (data.dependsOn && (await get(data.dependsOn))?.status !== 'completed') return false;
    if (data.entityKey && (await redis.zrange(entity(data.entityKey), 0, 0))[0] !== data.id) return false;
    return true;
  }
  async function status(data, state, error = '') {
    const tx = redis.multi().hset(record(data.id), 'status', state, 'updatedAt', Date.now(), 'error', String(error).slice(0, 120));
    tx.xadd(base + 'events:' + data.id, 'MAXLEN', '~', 100, '*', 'status', state, 'code', String(error).slice(0, 120));
    if (state === 'completed' && data.entityKey) tx.zrem(entity(data.entityKey), data.id);
    const results = await tx.exec();
    if (results.some(([err]) => err)) throw Error('STATUS_WRITE_FAILED');
  }
  async function retry(id, reconcile = false) {
    const row = await get(id);
    if (!row) throw Object.assign(Error('JOB_NOT_FOUND'), {status: 404});
    if (row.status === 'unknown' && !reconcile) throw Object.assign(Error('RECONCILIATION_REQUIRED'), {status: 409});
    const job = await queues[row.queue].getJob(id);
    if (!job || await job.getState() !== 'failed') throw Object.assign(Error('JOB_NOT_FAILED'), {status: 409});
    await status(row.data, 'waiting');
    try {await job.retry('failed');}
    catch (error) {await status(row.data, row.status, row.error); throw error;}
    return {id, status: 'waiting'};
  }
  async function history(start = 0, count = 20) {
    const ids = await redis.zrevrange(base + 'history', start, start + count - 1);
    const rows = await Promise.all(ids.map(get));
    return rows.filter(Boolean).map(publicStatus);
  }
  async function events(id) {
    if (!/^[a-f0-9]{64}$/.test(id)) return [];
    return (await redis.xrevrange(base + 'events:' + id, '+', '-', 'COUNT', 50)).map(([at, fields]) => ({at, ...Object.fromEntries(Array.from({length: fields.length / 2}, (_, i) => [fields[2 * i], fields[2 * i + 1]]))}));
  }
  return {get, submit, dispatch, ready, status, retry, history, events, record, base};
}
function publicStatus(row) {
  return {id: row.id, type: row.data.type, status: row.status, error: row.error || '',
    createdAt: Number(row.createdAt), updatedAt: Number(row.updatedAt || row.createdAt)};
}
module.exports = {createStore, publicStatus, RESERVE};
