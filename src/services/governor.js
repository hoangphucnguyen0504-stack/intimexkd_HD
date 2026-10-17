'use strict';
// One Redis window shared by every worker. Scope must match the Google project
// and service account; use the same APP_NAMESPACE for all replicas of this stack.
const RESERVE = `local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000)
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now-60000)
if redis.call('ZCARD',KEYS[1])>=tonumber(ARGV[1]) then
 local first=redis.call('ZRANGE',KEYS[1],0,0,'WITHSCORES'); return math.max(1,tonumber(first[2])+60000-now)
end
redis.call('ZADD',KEYS[1],now,ARGV[2]); redis.call('PEXPIRE',KEYS[1],61000); return 0`;
function createGovernor(redis, config) {
  return async kind => {
    const reserved = await require('./coordination').createCoordination(redis,config).execute({op:'quotaReserve',kind,limit:kind==='read'?config.reads:config.writes});
    if(!reserved.accepted)throw Object.assign(Error('SHEETS_LOCAL_RATE_LIMIT'),{code:'SHEETS_LOCAL_RATE_LIMIT',retryable:true,retryAfterMs:reserved.retryAfterSeconds*1000});
    return;
  };
}
module.exports = {createGovernor};
