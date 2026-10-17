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
    const result = await commit({idempotencyKey: require('node:crypto').randomUUID(), operations: [{kind: input.type === 'sheet-write' ? 'sheet' : 'job', data: input, logicalSheet: input.logicalSheet}]});
    const receipt = result.jobs[0];
    return {...receipt, status: (await get(receipt.id)).status};
  }
  async function commit({idempotencyKey, operations, locks = []}) {
    if (typeof idempotencyKey !== 'string' || idempotencyKey.length < 8 || idempotencyKey.length > 200 || !Array.isArray(operations) || operations.length > 500 || !Array.isArray(locks) || locks.length > 100) throw Object.assign(Error('INVALID_COMMIT'), {status: 400});
    const actions = [], jobs = [], fingerprints = [], seen = new Map();
    const now = Date.now();
    for (const operation of operations) {
      if (operation.kind === 'control') {
        if (!['retry','resume'].includes(operation.action) || typeof operation.key !== 'string' || !operation.key || operation.key.length > 300) throw Object.assign(Error('INVALID_JOB_CONTROL'),{status:400});
        const id = hash(operation.key), prior = await get(id);
        if (!prior) throw Object.assign(Error('JOB_NOT_FOUND'),{status:404});
        if (prior.status === 'unknown') throw Object.assign(Error('RECONCILIATION_REQUIRED'),{status:409});
        const commandId = hash(idempotencyKey + '/' + operation.action + '/' + operation.key);
        actions.push(['SET',base+'control:'+commandId,JSON.stringify({id,action:operation.action})]);
        actions.push(['ZADD',base+'controls',now,commandId]);
        continue;
      }
      if (operation.kind === 'technical') {
        const {key, op, value, ttl} = operation;
        if (typeof key !== 'string' || key.length < 1 || key.length > 1000 || !['set','remove'].includes(op)) throw Object.assign(Error('INVALID_TECHNICAL_OPERATION'), {status: 400});
        const target = config.prefix + ':technical:' + hash(key);
        if (op === 'remove') actions.push(['DEL', target]);
        else {
          if (!Number.isInteger(ttl) || ttl < 1 || ttl > 2764800 || value === undefined) throw Object.assign(Error('INVALID_TECHNICAL_TTL'), {status: 400});
          actions.push(['SET', target, JSON.stringify(value), 'EX', ttl]);
        }
        continue;
      }
      if (!['sheet', 'job'].includes(operation.kind)) throw Object.assign(Error('INVALID_OPERATION_KIND'), {status: 400});
      const data = validate(operation.data);
      if (seen.has(data.id)) {
        if(seen.get(data.id)!==data.fingerprint)throw Object.assign(Error('IDEMPOTENCY_CONFLICT'),{status:409});
        continue;
      }
      if (data.dependsOn && !seen.has(data.dependsOn) && !await get(data.dependsOn)) throw Object.assign(Error('DEPENDENCY_NOT_FOUND'), {status: 409});
      seen.set(data.id,data.fingerprint);
      if (operation.kind === 'sheet') data.logicalSheet = operation.logicalSheet || data.payload.sheet;
      const kind = data.type === 'sheet-write' ? 'sheets' : 'background';
      const job = {id: data.id, status: 'waiting', accepted: true};
      jobs.push(job); fingerprints.push([record(data.id), data.fingerprint]);
      // Commands are constructed exclusively server-side, never supplied over REST.
      actions.push(['JOB', record(data.id), data.fingerprint, JSON.stringify(data), kind, String(now), data.id,
        data.entityKey ? entity(data.entityKey) : '',
        kind === 'sheets' ? base + 'sheet-open:' + hash(data.payload.spreadsheetId + '/' + data.payload.sheet) : '',
        kind === 'sheets' ? base + 'record-jobs:' + hash(data.payload.recordId) : '',
        kind === 'sheets' ? base + 'sheet-history' : '',base+'queue-status:'+kind+':waiting']);
    }
    const receipt = {accepted: true, jobs};
    const owned = locks.map(lock => {
      if (typeof lock.name !== 'string' || typeof lock.owner !== 'string') throw Object.assign(Error('INVALID_LOCK'), {status: 400});
      return [config.prefix + ':lock:' + hash(lock.name), lock.owner];
    });
    const digest = hash(require('../utils/protocol').canonical({operations, locks}));
    const result = await redis.eval(COMMIT, 4, base + 'commit:' + hash(idempotencyKey), base + 'sequence', base + 'dispatch', base + 'history',
      digest, JSON.stringify(actions), JSON.stringify(fingerprints), JSON.stringify(owned), JSON.stringify(receipt));
    if (result === 'CONFLICT') throw Object.assign(Error('IDEMPOTENCY_CONFLICT'), {status: 409});
    if (result === 'LEASE_LOST') throw Object.assign(Error('HDOI_LEASE_LOST'), {status: 409});
    return JSON.parse(result);
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
    const controls = await redis.zrange(base+'controls',0,limit-1);
    for (const controlId of controls) {
      const raw = await redis.get(base+'control:'+controlId);
      if (!raw) throw Error('CONTROL_INTENT_MISSING');
      const command = JSON.parse(raw), row = await get(command.id);
      const job = row && await queues[row.queue].getJob(command.id);
      if (!job || row.status === 'unknown') {
        await redis.set(base+'control-error:'+controlId,'MANUAL_REVIEW_REQUIRED');
        await redis.zrem(base+'controls',controlId); continue;
      }
      const state = await job.getState();
      if (state === 'active') continue;
      const safeResume = command.action === 'resume' && row.data.type === 'email' && job.data.effect?.status === 'rejected';
      if (state === 'failed' || (state === 'completed' && safeResume)) {
        if (command.action === 'resume' && !safeResume) {
          await redis.set(base+'control-error:'+controlId,'DELIVERY_REVIEW_REQUIRED');
        } else {
          await status(job.data,'waiting'); await job.retry(state);
        }
      }
      await redis.zrem(base+'controls',controlId);
    }
    return ids.length + controls.length;
  }
  async function ready(data) {
    if (data.dependsOn && (await get(data.dependsOn))?.status !== 'completed') return false;
    if (data.entityKey && (await redis.zrange(entity(data.entityKey), 0, 0))[0] !== data.id) return false;
    return true;
  }
  async function status(data, state, error = '', ownership = {}) {
    await redis.eval(STATUS,5,record(data.id),base+'events:'+data.id,
      data.entityKey?entity(data.entityKey):base+'unused',
      data.type==='sheet-write'?base+'sheet-open:'+hash(data.payload.spreadsheetId+'/'+data.payload.sheet):base+'unused',
      ownership.token&&ownership.queueName?config.prefix+':'+ownership.queueName+':'+data.id+':lock':base+'unused',
      state,String(error).slice(0,120),Date.now(),data.id,base+'queue-status:',data.entityKey?'1':'0',data.type==='sheet-write'?'1':'0',ownership.token||'');
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
  return {get, submit, commit, dispatch, ready, status, retry, history, events, record, base};
}
const COMMIT = `
local prior=redis.call('HGET',KEYS[1],'digest')
if prior then if prior~=ARGV[1] then return 'CONFLICT' end return redis.call('HGET',KEYS[1],'receipt') end
local locks=cjson.decode(ARGV[4]); for _,l in ipairs(locks) do if redis.call('GET',l[1])~=l[2] then return 'LEASE_LOST' end end
local checks=cjson.decode(ARGV[3]); for _,c in ipairs(checks) do local old=redis.call('HGET',c[1],'fingerprint'); if old and old~=c[2] then return 'CONFLICT' end end
local actions=cjson.decode(ARGV[2])
for _,a in ipairs(actions) do
 if a[1]=='JOB' then
  if redis.call('EXISTS',a[2])==0 then
   local seq=redis.call('INCR',KEYS[2]); redis.call('HSET',a[2],'fingerprint',a[3],'data',a[4],'queue',a[5],'status','waiting','createdAt',a[6],'seq',seq)
   redis.call('ZADD',KEYS[3],seq,a[7]); redis.call('ZADD',KEYS[4],seq,a[7]);
   for i=8,12 do if a[i]~='' then redis.call('ZADD',a[i],seq,a[7]) end end
  end
 else redis.call(unpack(a)) end
end
redis.call('HSET',KEYS[1],'digest',ARGV[1],'receipt',ARGV[5]); return ARGV[5]`;
const STATUS=`if ARGV[8]~='' and redis.call('GET',KEYS[5])~=ARGV[8] then return redis.error_reply('WORKER_LEASE_LOST') end
local prior=redis.call('HGET',KEYS[1],'status');local kind=redis.call('HGET',KEYS[1],'queue');local seq=redis.call('HGET',KEYS[1],'seq')
if not prior or not kind or not seq then return redis.error_reply('JOB_MISSING') end
redis.call('ZREM',ARGV[5]..kind..':'..prior,ARGV[4]);redis.call('ZADD',ARGV[5]..kind..':'..ARGV[1],seq,ARGV[4])
redis.call('HSET',KEYS[1],'status',ARGV[1],'updatedAt',ARGV[3],'error',ARGV[2])
redis.call('XADD',KEYS[2],'MAXLEN','~',100,'*','status',ARGV[1],'code',ARGV[2])
if ARGV[1]=='completed' then if ARGV[6]=='1' then redis.call('ZREM',KEYS[3],ARGV[4]) end;if ARGV[7]=='1' then redis.call('ZREM',KEYS[4],ARGV[4]) end end
return 1`;
function publicStatus(row) {
  return {id: row.id, type: row.data.type, status: row.status, error: row.error || '',
    createdAt: Number(row.createdAt), updatedAt: Number(row.updatedAt || row.createdAt)};
}
module.exports = {createStore, publicStatus, RESERVE, COMMIT, STATUS};
