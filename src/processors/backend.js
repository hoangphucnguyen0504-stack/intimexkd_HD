'use strict';
// Only the configured backend is contacted. Job payloads cannot supply URLs.
function createBackendProcessor(config, fetchImpl = fetch) {
  if (!config.backendUrl || !config.backendToken || config.backendToken.length < 32 || config.backendToken.startsWith('REPLACE_')) throw Error('BACKEND_WORKER_CONFIG_REQUIRED');
  const url = new URL('/api/internal/queue/execute', config.backendUrl);
  if (url.protocol !== 'https:' && !['localhost', '127.0.0.1'].includes(url.hostname)) throw Error('BACKEND_HTTPS_REQUIRED');
  return async job => {
    const type = job.data.type;
    const delivery = ['email', 'weighing-email', 'security-alert'].includes(type);
    if (delivery && job.data.effect?.status === 'started') throw Object.assign(Error('DELIVERY_OUTCOME_UNKNOWN'), {code: 'DELIVERY_OUTCOME_UNKNOWN', unknown: true, retryable: false});
    // Never automatically replay an SMTP-capable callback after loss of its ACK.
    if (delivery) {
      const next = {...job.data, effect: {status: 'started', at: Date.now()}};
      await job.updateData(next); job.data = next;
    }
    let response;
    try {
      response = await fetchImpl(url, {method: 'POST', redirect: 'error', signal: AbortSignal.timeout(90000),
        headers: {'content-type': 'application/json', authorization: 'Bearer ' + config.backendToken},
        body: JSON.stringify({id: job.id, type, payload: job.data.payload, attempt: job.attemptsMade + 1})});
    } catch {
      throw Object.assign(Error(delivery ? 'DELIVERY_OUTCOME_UNKNOWN' : 'BACKEND_UNAVAILABLE'), {
        code: delivery ? 'DELIVERY_OUTCOME_UNKNOWN' : 'BACKEND_UNAVAILABLE', unknown: delivery, retryable: !delivery});
    }
    let result;
    try {result = await response.json();} catch {result = null;}
    if (!response.ok || result?.ok !== true) {
      const knownRejected = result?.retryable === true && result?.unknown !== true;
      if (delivery && knownRejected) {
        const next = {...job.data, effect: {status: 'rejected', at: Date.now()}};
        await job.updateData(next); job.data = next;
      }
      throw Object.assign(Error('BACKEND_JOB_FAILED'), {code: 'BACKEND_JOB_FAILED',
        retryable: delivery ? knownRejected : result?.retryable === true || [429, 502, 503, 504].includes(response.status),
        unknown: result?.unknown === true || (delivery && !knownRejected),
        retryAfterMs: Math.max(0, Number(response.headers.get('retry-after')) || 0) * 1000});
    }
    return {confirmed: true};
  };
}
module.exports = {createBackendProcessor};
