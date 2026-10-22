'use strict';
// Only the configured backend is contacted. Job payloads cannot supply URLs.
function createBackendProcessor(config, fetchImpl = fetch, {images} = {}) {
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
      // A durable declaration waiting for local quota has not failed its business
      // attempt. Let the worker delay it without consuming a BullMQ retry.
      if (response.ok && type === 'qr-recovery' && job.data.payload?.durable === true &&
          knownRejected && result.code === 'QR_JOB_QUOTA_RETRY') {
        const delay = Math.ceil(Number(response.headers.get('retry-after')) * 1000);
        throw Object.assign(Error('SHEETS_LOCAL_RATE_LIMIT'), {code:'SHEETS_LOCAL_RATE_LIMIT', retryable:true,
          retryAfterMs: Number.isSafeInteger(delay) && delay > 0 ? delay : 5000});
      }
      if (delivery && knownRejected) {
        const next = {...job.data, effect: {status: 'rejected', at: Date.now()}};
        await job.updateData(next); job.data = next;
      }
      throw Object.assign(Error('BACKEND_JOB_FAILED'), {code: 'BACKEND_JOB_FAILED',
        retryable: delivery ? knownRejected : result?.retryable === true || [429, 502, 503, 504].includes(response.status),
        unknown: result?.unknown === true || (delivery && !knownRejected),
        retryAfterMs: Math.max(0, Number(response.headers.get('retry-after')) || 0) * 1000});
    }
    if (delivery && ['NO_RECIPIENT','DISABLED'].includes(result.deliveryStatus)) {
      const next = {...job.data,effect:{status:'rejected',at:Date.now()}};
      await job.updateData(next); job.data = next;
    }
    const imageLinks=result._imageLinks||result.result?._imageLinks;
    if(Array.isArray(imageLinks)&&imageLinks.length){
      if(!images)throw Object.assign(Error('IMAGE_LINK_SERVICE_REQUIRED'),{code:'IMAGE_LINK_SERVICE_REQUIRED',retryable:true});
      try {
        for(const {imageId,...link} of imageLinks)await images.links(config.namespace,imageId,[link]);
      }catch{throw Object.assign(Error('IMAGE_LINK_COMMIT_FAILED'),{code:'IMAGE_LINK_COMMIT_FAILED',retryable:true});}
    }
    return {confirmed: true, ...(result.deliveryStatus ? {deliveryStatus:result.deliveryStatus} : {})};
  };
}
module.exports = {createBackendProcessor};
