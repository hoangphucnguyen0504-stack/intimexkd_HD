'use strict';
// One Redis window shared by every worker. Scope must match the Google project
// and service account; use the same APP_NAMESPACE for all replicas of this stack.
function createGovernor(redis, config) {
  return async kind => {
    const reserved = await require('./coordination').createCoordination(redis,config).execute({op:'quotaReserve',kind,priority:'background',limit:kind==='read'?config.reads:config.writes});
    if(!reserved.accepted)throw Object.assign(Error('SHEETS_LOCAL_RATE_LIMIT'),{code:'SHEETS_LOCAL_RATE_LIMIT',retryable:true,retryAfterMs:reserved.retryAfterSeconds*1000});
    return;
  };
}
module.exports = {createGovernor};
