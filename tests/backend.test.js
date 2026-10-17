'use strict';
const {test} = require('node:test'); const assert = require('node:assert/strict');
const {createBackendProcessor} = require('../src/processors/backend');
const config = {backendUrl: 'https://backend.example', backendToken: 'x'.repeat(32)};
const job = type => ({id: 'a', data: {type, payload: {}}, attemptsMade: 0, async updateData(data) {this.data = data;}});
test('SMTP callback timeout is held unknown and never invoked again', async () => {
  let calls = 0; const run = createBackendProcessor(config, async () => {calls++; throw Error('timeout');});
  const mail = job('email');
  await assert.rejects(run(mail), error => error.unknown && !error.retryable);
  await assert.rejects(run(mail), error => error.unknown);
  assert.equal(calls, 1);
});
test('safe QR callback connection failure can retry', async () => {
  const run = createBackendProcessor(config, async () => {throw Error('network');});
  await assert.rejects(run(job('qr-recovery')), error => error.retryable && !error.unknown);
});
test('explicit delivery rejection permits retry, no payload URL callback', async () => {
  const mail = job('email');
  const run = createBackendProcessor(config, async url => {
    assert.equal(String(url), 'https://backend.example/api/internal/queue/execute');
    return {ok: false, status: 503, headers: new Headers({'retry-after': '30'}), json: async () => ({ok: false, retryable: true})};
  });
  await assert.rejects(run(mail), error => error.retryable && error.retryAfterMs === 30000);
  assert.equal(mail.data.effect.status, 'rejected');
});
