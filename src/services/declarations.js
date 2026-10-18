'use strict';
const {hash, validate} = require('../utils/protocol');
const fail = (code, status = 503) => { throw Object.assign(Error(code), {status}); };
const statuses = new Set(['QUEUED', 'PROCESSING', 'SUCCEEDED', 'FAILED']);
function text(value, max = 1000) {
  if (typeof value !== 'string' || !value || Buffer.byteLength(value) > max || /[\x00-\x1f\x7f]/.test(value)) fail('INVALID_DECLARATION', 400);
  return value;
}
// All checks precede writes: Redis Lua errors do not roll back earlier commands.
const ACCEPT = `
local expected={'string','string','set','hash','string','zset','zset','zset'}
for i,k in ipairs(KEYS) do local t=redis.call('TYPE',k).ok; if t~='none' and t~=expected[i] then return 'CORRUPT' end end
local old=redis.call('GET',KEYS[1])
if old then
 local r=cjson.decode(old)
 if r.fingerprint~=ARGV[1] or r.identityKey~=ARGV[2] or r.ticketId~=ARGV[3] then return 'CONFLICT' end
 -- A write on replay makes WAITAOF cover the existing intent on this connection.
 redis.call('SET',KEYS[1],old); return cjson.encode({record=r,duplicate=true})
end
if redis.call('EXISTS',KEYS[4])==1 then return 'CONFLICT' end
local identity=redis.call('GET',KEYS[2]); if identity and identity~=ARGV[4] then return 'PENDING' end
if redis.call('SCARD',KEYS[3])>=5 then return 'TICKET_USED' end
local seq=redis.call('GET',KEYS[5]); if seq and (not tonumber(seq) or tonumber(seq)<0 or tonumber(seq)>=9007199254740990 or tonumber(seq)~=math.floor(tonumber(seq))) then return 'CORRUPT' end
seq=redis.call('INCR',KEYS[5])
redis.call('SET',KEYS[1],ARGV[5]); redis.call('SET',KEYS[2],ARGV[4]); redis.call('SADD',KEYS[3],ARGV[4])
redis.call('HSET',KEYS[4],'fingerprint',ARGV[6],'data',ARGV[7],'queue','background','status','waiting','createdAt',ARGV[8],'seq',seq)
redis.call('ZADD',KEYS[6],seq,ARGV[4]);redis.call('ZADD',KEYS[7],seq,ARGV[4]);redis.call('ZADD',KEYS[8],seq,ARGV[4])
return cjson.encode({record=cjson.decode(ARGV[5]),duplicate=false})`;
const UPDATE = `
if redis.call('GET',KEYS[2])~=ARGV[1] then return 'LEASE_LOST' end
local typ=redis.call('TYPE',KEYS[3]).ok;if typ~='none' and typ~='string' then return 'CORRUPT' end
local raw=redis.call('GET',KEYS[1]); if not raw then return 'MISSING' end
local r=cjson.decode(raw)
if r.revision~=tonumber(ARGV[2]) then return 'REVISION_CONFLICT' end
if r.status=='SUCCEEDED' or r.status=='FAILED' then return 'TERMINAL' end
local patch=cjson.decode(ARGV[3])
local attempts=tonumber(patch.attempts)
if not attempts or attempts<0 or attempts>5 or attempts~=math.floor(attempts) then return 'TRANSITION' end
if patch.status=='PROCESSING' then
 if r.status=='QUEUED' and (attempts~=r.attempts+1 or attempts>5) then return 'TRANSITION' end
 if r.status=='PROCESSING' and attempts~=r.attempts then return 'TRANSITION' end
elseif patch.status=='QUEUED' then
 if r.status~='PROCESSING' or attempts~=r.attempts or attempts>=5 then return 'TRANSITION' end
elseif patch.status=='SUCCEEDED' then
 if r.status~='PROCESSING' or attempts~=r.attempts then return 'TRANSITION' end
elseif patch.status=='FAILED' then
 if attempts~=r.attempts or (r.status~='PROCESSING' and not (r.status=='QUEUED' and attempts==5)) then return 'TRANSITION' end
else return 'TRANSITION' end
for k,v in pairs(patch) do r[k]=v end
r.revision=r.revision+1;r.updatedAt=tonumber(ARGV[4])
if patch.status=='SUCCEEDED' and redis.call('GET',KEYS[3])==r.id then redis.call('DEL',KEYS[3]) end
local value=cjson.encode(r);redis.call('SET',KEYS[1],value);return value`;
function createDeclarations({redis, config, store}) {
  const base = store.base, record = id => base + 'declaration:' + id;
  const idOf = input => input.id ? (/^[a-f0-9]{64}$/.test(input.id) ? input.id : fail('INVALID_DECLARATION', 400)) : (/^[a-f0-9]{64}$/.test(input.key || '') ? input.key : hash(text(input.key, 300)));
  async function health() {
    try {
      // Independent, fresh read-only probes: overlap transport latency without
      // caching durability state or moving the write/AOF acknowledgment barrier.
      const [settings, persistence, command] = await Promise.all([
        redis.config('GET', 'appendonly', 'appendfsync', 'maxmemory-policy'),
        redis.info('persistence'),
        redis.call('COMMAND', 'INFO', 'WAITAOF'),
      ]);
      const values = Object.fromEntries(Array.from({length: settings.length / 2}, (_, i) => [settings[i * 2], settings[i * 2 + 1]]));
      const info = Object.fromEntries(String(persistence).split(/\r?\n/).filter(line => line.includes(':')).map(line => { const at = line.indexOf(':'); return [line.slice(0, at), line.slice(at + 1)]; }));
      const supported = Array.isArray(command?.[0]) && String(command[0][0]).toLowerCase() === 'waitaof';
      return {version: 1, available: supported && values.appendonly === 'yes' && values.appendfsync === 'always' && values['maxmemory-policy'] === 'noeviction' && info.aof_enabled === '1' && info.aof_last_write_status === 'ok'};
    } catch { return {version: 1, available: false}; }
  }
  async function requireDurability() { if (!(await health()).available) fail('DURABLE_DECLARATIONS_UNAVAILABLE'); }
  async function barrier() {
    try { const ack = await redis.call('WAITAOF', 1, 0, 5000); if (!Array.isArray(ack) || !Number.isSafeInteger(Number(ack[0])) || Number(ack[0]) < 1) fail('DURABLE_DECLARATION_ACK_UNCONFIRMED'); }
    catch { fail('DURABLE_DECLARATION_ACK_UNCONFIRMED'); }
  }
  async function accept(input) {
    const key = text(input.key, 300), identityKey = text(input.identityKey), ticketId = text(input.ticketId);
    const job = input.job;
    if (!job || typeof job !== 'object' || Array.isArray(job) || !job.data || typeof job.data !== 'object' || Array.isArray(job.data) || !/^[a-f0-9]{64}$/.test(job.fingerprint || '')) fail('INVALID_DECLARATION', 400);
    let encoded; try { encoded = JSON.stringify(job); } catch { fail('INVALID_DECLARATION', 400); }
    if (Buffer.byteLength(encoded) > 300 * 1024) fail('DECLARATION_TOO_LARGE', 413);
    const id = hash(key), now = Date.now();
    const row = {id, key, job, fingerprint: job.fingerprint, identityKey, ticketId, status: 'QUEUED', attempts: 0, revision: 1, createdAt: now, updatedAt: now};
    const native = validate({type: 'qr-recovery', key, payload: {key: id, durable: true}}, now);
    await requireDurability();
    const result = await redis.eval(ACCEPT, 8, record(id), base + 'declaration-identity:' + hash(identityKey), base + 'declaration-ticket:' + hash(ticketId),
      store.record(id), base + 'sequence', base + 'dispatch', base + 'history', base + 'queue-status:background:waiting',
      job.fingerprint, identityKey, ticketId, id, JSON.stringify(row), native.fingerprint, JSON.stringify(native), String(now));
    const errors = {CONFLICT: ['IDEMPOTENCY_CONFLICT', 409], PENDING: ['SUBMISSION_PENDING', 409], TICKET_USED: ['TICKET_USED', 409], CORRUPT: ['DURABLE_DECLARATION_STATE_INVALID', 503]};
    if (errors[result]) fail(...errors[result]);
    await barrier();
    const receipt = JSON.parse(result);
    return {id, accepted: true, durable: true, status: 'QUEUED', duplicate: receipt.duplicate};
  }
  async function get(input) {
    const id = idOf(input), raw = await redis.get(record(id));
    if (!raw) return null;
    const row = JSON.parse(raw);
    if (row.status === 'SUCCEEDED') {
      const ids = row.result?._sheetJobs;
      let synchronization = {status: 'PENDING'};
      if (ids != null && (!Array.isArray(ids) || ids.length > 500 || ids.some(value => typeof value !== 'string' || !/^[a-f0-9]{64}$/.test(value)))) {
        synchronization = {status: 'FAILED', manualReconciliation: true};
      } else if (ids?.length) {
        const unique = [...new Set(ids)], sheets = await Promise.all(unique.map(id => store.get(id)));
        const invalid = sheets.some((job, index) => !job || job.id !== unique[index] || job.data?.type !== 'sheet-write' ||
          typeof row.job?.scope !== 'string' || !row.job.scope || job.data?.payload?.spreadsheetId !== row.job.scope || ['failed', 'unknown'].includes(job.status));
        synchronization = invalid ? {status: 'FAILED', manualReconciliation: true} : {status: sheets.every(job => job.status === 'completed') ? 'SUCCEEDED' : 'PENDING'};
      }
      return {...row, synchronization};
    }
    if (!['SUCCEEDED', 'FAILED'].includes(row.status)) {
      const native = await store.get(id);
      if (!native) fail('DURABLE_DECLARATION_INTENT_MISSING');
      if (['failed', 'unknown'].includes(native.status)) return {...row, status: 'FAILED', manualReconciliation: true,
        error: {status: 503, code: 'DURABLE_DECLARATION_WORKER_' + native.status.toUpperCase(), message: 'Worker delivery requires reconciliation'}};
    }
    return row;
  }
  async function update(input) {
    const id = idOf(input), owner = text(input.owner, 2048), revision = input.expectedRevision;
    if (!Number.isSafeInteger(revision) || revision < 1 || !statuses.has(input.status) || !Number.isSafeInteger(input.attempts) || input.attempts < 0 || input.attempts > 5) fail('INVALID_DECLARATION_UPDATE', 400);
    // Immutable submission fields cannot be replaced by a worker update.
    const allowed = ['status', 'attempts', 'result', 'error'];
    const patch = Object.fromEntries(allowed.filter(k => Object.hasOwn(input, k)).map(k => [k, input[k]]));
    let encoded; try { encoded = JSON.stringify(patch); } catch { fail('INVALID_DECLARATION_UPDATE', 400); }
    if (Buffer.byteLength(encoded) > 300 * 1024) fail('DECLARATION_TOO_LARGE', 413);
    await requireDurability();
    const prior = await get({id}); if (!prior) fail('JOB_NOT_FOUND', 404);
    const result = await redis.eval(UPDATE, 3, record(id), config.prefix + ':lock:' + hash('durable-declaration/' + id), base + 'declaration-identity:' + hash(text(prior.identityKey)), owner, String(revision), encoded, String(Date.now()));
    const errors = {LEASE_LOST: ['HDOI_LEASE_LOST', 409], MISSING: ['JOB_NOT_FOUND', 404], REVISION_CONFLICT: ['DECLARATION_REVISION_CONFLICT', 409], TERMINAL: ['DECLARATION_TERMINAL', 409], TRANSITION: ['DECLARATION_TRANSITION', 409], CORRUPT: ['DURABLE_DECLARATION_STATE_INVALID', 503]};
    if (errors[result]) fail(...errors[result]);
    await barrier(); return {...JSON.parse(result), durable: true};
  }
  async function execute(input) {
    if (input.op === 'declarationAccept') return accept(input);
    if (input.op === 'declarationGet') return get(input);
    if (input.op === 'declarationUpdate') return update(input);
    fail('INVALID_DECLARATION_OPERATION', 400);
  }
  return {accept, get, update, health, execute};
}
module.exports = {createDeclarations, ACCEPT, UPDATE};
