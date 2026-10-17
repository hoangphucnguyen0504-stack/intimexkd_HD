'use strict';
const {createHash, randomUUID} = require('node:crypto');
const hash = value => createHash('sha256').update(value).digest('hex');
const MAX_TTL = 32 * 86400;
function invalid() {throw Object.assign(Error('INVALID_COORDINATION_REQUEST'), {status: 400});}
function string(value, max = 2048) {
  if (typeof value !== 'string' || !value.length || Buffer.byteLength(value) > max || /[\x00-\x1f\x7f]/.test(value)) invalid();
  return value;
}
function integer(value, max) {if (!Number.isSafeInteger(value) || value < 1 || value > max) invalid(); return value;}
function list(value) {if (!Array.isArray(value) || value.length > 500) invalid(); return value;}
function json(value) {
  let encoded;
  try {encoded = JSON.stringify(value);} catch {invalid();}
  if (encoded === undefined || Buffer.byteLength(encoded) > 300 * 1024) invalid();
  return encoded;
}
function keyOf(key, config) {return config.prefix + ':technical:' + hash(string(key));}
function lockKey(name, config) {return config.prefix + ':lock:' + hash(string(name));}
const INCREMENT = `local n=redis.call('INCR',KEYS[1])
if n==1 or redis.call('PTTL',KEYS[1])<0 then redis.call('EXPIRE',KEYS[1],ARGV[1]) end
return {n,math.max(1,math.ceil(redis.call('PTTL',KEYS[1])/1000))}`;
const DECREMENT = `local v=redis.call('GET',KEYS[1]); if not v then return 0 end
local n=tonumber(v); if not n or n<0 or n~=math.floor(n) then return redis.error_reply('INVALID_COUNTER') end
if n>0 then return redis.call('DECR',KEYS[1]) end; return 0`;
const ACQUIRE = `if redis.call('SET',KEYS[1],ARGV[1],'NX','PX',ARGV[2]) then return 1 end; return 0`;
const RENEW = `if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('PEXPIRE',KEYS[1],ARGV[2]) end; return 0`;
const RELEASE = `if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end; return 0`;
const SET_MANY = `for i,key in ipairs(KEYS) do redis.call('SET',key,ARGV[i*2-1],'EX',ARGV[i*2]) end; return 1`;
const QUOTA = `local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000)
local cooldown=redis.call('GET',KEYS[2])
if cooldown then
 local value=cjson.decode(cooldown)
 if type(value)=='table' and tonumber(value.until) and tonumber(value.until)>now then return math.ceil((tonumber(value.until)-now)/1000) end
end
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now-60000)
if redis.call('ZCARD',KEYS[1])>=tonumber(ARGV[1]) then
 local first=redis.call('ZRANGE',KEYS[1],0,0,'WITHSCORES'); return math.max(1,math.ceil((tonumber(first[2])+60000-now)/1000))
end
redis.call('ZADD',KEYS[1],now,ARGV[2]); redis.call('PEXPIRE',KEYS[1],61000); return 0`;

// TTLs are seconds, lock leases are milliseconds. Only server-owned Lua is run.
function createCoordination(redis, config) {
  const stateKey = key => keyOf(key, config);
  const boundLockKey = name => lockKey(name, config);
  async function execute(input) {
    if (!input || typeof input !== 'object' || Array.isArray(input)) invalid();
    // Also enforce the body budget for direct service callers.
    json(input);
    const {op} = input;
    switch (op) {
      case 'get': {const value = await redis.get(stateKey(input.key)); return value === null ? null : JSON.parse(value);}
      case 'getMany': {
        const keys = [...new Set(list(input.keys).map(key => string(key)))];
        if (!keys.length) return [];
        const values = await redis.mget(...keys.map(stateKey));
        return keys.flatMap((key, i) => values[i] === null ? [] : [[key, JSON.parse(values[i])]]);
      }
      case 'set': {
        const key = stateKey(input.key), value = json(input.value), ttl = integer(input.ttl, MAX_TTL);
        await redis.set(key, value, 'EX', ttl); return true;
      }
      case 'setMany': {
        const items = list(input.items).map(item => {
          if (!item || typeof item !== 'object' || Array.isArray(item)) invalid();
          return {key: stateKey(item.key), value: json(item.value), ttl: integer(item.ttl, MAX_TTL)};
        });
        if (items.length) await redis.eval(SET_MANY, items.length, ...items.map(item => item.key), ...items.flatMap(item => [item.value, item.ttl]));
        return true;
      }
      case 'remove': return await redis.del(stateKey(input.key));
      case 'removeMany': {
        const keys = [...new Set(list(input.keys).map(stateKey))];
        return keys.length ? await redis.del(...keys) : 0;
      }
      case 'increment':
      case 'incrementWindow': {
        const key = stateKey(input.key), ttl = integer(input.ttl, MAX_TTL);
        const [count, remaining] = await redis.eval(INCREMENT, 1, key, ttl);
        return op === 'increment' ? Number(count) : {count: Number(count), ttl: Number(remaining)};
      }
      case 'decrement': return Number(await redis.eval(DECREMENT, 1, stateKey(input.key)));
      case 'ttl': return Math.max(0, Math.ceil(Number(await redis.pttl(stateKey(input.key))) / 1000));
      case 'lockAcquire':
      case 'lockRenew':
      case 'lockRelease': {
        const key = boundLockKey(input.name), owner = string(input.owner, 256);
        const script = {lockAcquire: ACQUIRE, lockRenew: RENEW, lockRelease: RELEASE}[op];
        const args = op === 'lockRelease' ? [owner] : [owner, integer(input.ttlMs, 180000)];
        return Number(await redis.eval(script, 1, key, ...args)) === 1;
      }
      case 'quotaReserve': {
        if (input.kind !== 'read' && input.kind !== 'write') invalid();
        const limit = integer(input.limit, 50);
        const configured = integer(input.kind === 'read' ? config.reads : config.writes, 50);
        const delay = Number(await redis.eval(QUOTA, 2, config.prefix + ':quota:' + input.kind,
          stateKey('sheets-cooldown/' + input.kind), Math.min(limit, configured), randomUUID()));
        return {accepted: delay === 0, retryAfterSeconds: delay};
      }
      default: invalid();
    }
  }
  return {execute, keyOf: stateKey, lockKey: boundLockKey};
}
module.exports = {createCoordination, keyOf, lockKey};
