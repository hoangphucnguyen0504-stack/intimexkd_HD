'use strict';
const {test} = require('node:test'); const assert = require('node:assert/strict');
const {processor, sheetsWorkerConcurrency} = require('../src/workers');
function setup(execute, ready = true) {
  const states = []; const store = {ready: async () => ready, status: async (_data, status, error) => states.push({status, error})};
  const job = {id: 'one', data: {id: 'one'}, attemptsMade: 0, opts: {attempts: 3},
    async updateData(data) {this.data = data;}, async moveToDelayed() {this.delayed = true;}};
  return {states, job, run: processor({store, execute})};
}
test('worker preserves Retry-After and caps transient retries', async () => {
  const retry = Object.assign(Error('429'), {code: 'RATE', retryable: true, retryAfterMs: 60000});
  const s = setup(async () => {throw retry;});
  await assert.rejects(s.run(s.job, 'token'), error => error.retryAfterMs === 60000);
  assert.equal(s.job.data.retryAfterMs, 60000); assert.equal(s.states.at(-1).status, 'delayed');
  s.job.attemptsMade = 2;
  await assert.rejects(s.run(s.job, 'token'), error => error.name === 'UnrecoverableError');
  assert.equal(s.states.at(-1).status, 'failed');
});
test('unknown external effects become terminal manual holds', async () => {
  const s = setup(async () => {throw Object.assign(Error('timeout'), {code: 'UNKNOWN', unknown: true});});
  await assert.rejects(s.run(s.job, 'token'), error => error.name === 'UnrecoverableError');
  assert.equal(s.states.at(-1).status, 'unknown');
});
test('blocked dependency defers without calling business processor', async () => {
  let calls = 0; const s = setup(async () => calls++, false);
  await assert.rejects(s.run(s.job, 'token'), error => error.name === 'DelayedError');
  assert.equal(calls, 0); assert.equal(s.job.delayed, true);
});
test('datadongbo uses worker capacity and google stays at 1', () => {
  const previous = process.env.BUSINESS_DATA_PROVIDER;
  const capacity = {concurrency: 8};
  try {
    process.env.BUSINESS_DATA_PROVIDER = 'datadongbo';
    assert.equal(sheetsWorkerConcurrency(capacity), 8);
    process.env.BUSINESS_DATA_PROVIDER = 'sheets';
    assert.equal(sheetsWorkerConcurrency(capacity), 1);
    delete process.env.BUSINESS_DATA_PROVIDER;
    assert.equal(sheetsWorkerConcurrency(capacity), 1);
  } finally {
    if (previous === undefined) delete process.env.BUSINESS_DATA_PROVIDER;
    else process.env.BUSINESS_DATA_PROVIDER = previous;
  }
});
