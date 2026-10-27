'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { validate, retryDelay } = require('../src/utils/protocol');
const { createBackendProcessor } = require('../src/processors/backend');
test('SALES private reference jobs validate and replay submission idempotently', () => {
  for (const [type, payload] of [['sales-email-scan', { userId: 'a', runId: randomUUID(), generation: 1 }], ['sales-email-schedule', { userId: 'a', generation: 1, scheduleId: '10' }]]) {
    const input = { type, key: type + '/a', payload };
    const a = validate(input, 1), b = validate(input, 1000);
    assert.equal(a.id, b.id); assert.equal(a.fingerprint, b.fingerprint);
    for (const bad of [{ ...payload, token: 'secret' }, { ...payload, userId: '' }, { ...payload, generation: -1 }, { ...payload, generation: 0.5 }]) assert.throws(() => validate({ ...input, payload: bad }));
    assert.throws(() => validate({ ...input, payload: { ...payload, ...(type === 'sales-email-scan' ? { runId: 'invalid' } : { scheduleId: '' }) } }));
  }
});
test('read-only SALES scans retry on lost callback without SMTP unknown-delivery guard', async () => {
  const config = { backendUrl: 'https://backend.example', backendToken: 'x'.repeat(32) };
  const job = { id: 'fixture', data: { type: 'sales-email-scan', payload: { userId: 'a', runId: randomUUID(), generation: 1 } }, attemptsMade: 0, async updateData(data) { this.data = data; } };
  let calls = 0;
  const unavailable = createBackendProcessor(config, async () => { calls++; throw Error('network'); });
  await assert.rejects(unavailable(job), { code: 'BACKEND_UNAVAILABLE', retryable: true, unknown: false });
  await assert.rejects(unavailable(job), { retryable: true }); assert.equal(calls, 2); assert.equal(job.data.effect, undefined);
  const quota = createBackendProcessor(config, async () => Response.json({ ok: false, code: 'SALES_EMAIL_PROVIDER_RETRY', retryable: true }, { headers: { 'retry-after': '73' } }));
  await assert.rejects(quota(job), { retryable: true, retryAfterMs: 73000 });
  assert.equal(retryDelay(3, 73000, () => 0), 73000);
});
