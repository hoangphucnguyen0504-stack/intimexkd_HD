'use strict';
const {test} = require('node:test'); const assert = require('node:assert/strict');
const {createApp} = require('../src/app');
test('HTTP requires auth, accepts durable receipt and rejects malformed JSON', async t => {
  let submitted = 0;
  const app = createApp({config: {token: 'x'.repeat(32), prefix: 'test'}, redis: {eval: async () => 1}, queues: {},
    store: {submit: async () => {submitted++; return {id: 'abc', status: 'waiting', accepted: true};}}});
  const server = app.listen(0, '127.0.0.1'); await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const url = 'http://127.0.0.1:' + server.address().port;
  assert.equal((await fetch(url + '/health/live')).status, 200);
  assert.equal((await fetch(url + '/v1/jobs')).status, 401);
  const headers = {authorization: 'Bearer ' + 'x'.repeat(32), 'content-type': 'application/json'};
  const response = await fetch(url + '/v1/jobs', {method: 'POST', headers, body: '{}'});
  assert.equal(response.status, 202); assert.equal((await response.json()).status, 'waiting'); assert.equal(submitted, 1);
  assert.equal((await fetch(url + '/v1/jobs', {method: 'POST', headers, body: '{'})).status, 400);
});
test('GET /v1/metrics reports outstanding age and unknown count', async t => {
  const createdAt = Date.now() - 4000;
  const redis = {
    zcard: async key => key.endsWith('unknown') ? 2 : key.endsWith('dispatch') ? 1 : 0,
    zrange: async key => key.endsWith('sheets:waiting') ? ['job-1'] : [],
  };
  const app = createApp({
    config: {token: 'x'.repeat(32), prefix: 'test'},
    redis,
    queues: {sheets: {getJobCounts: async () => ({waiting: 1})}, background: {getJobCounts: async () => ({})}},
    store: {base: 'ns:', get: async id => id === 'job-1' ? {createdAt} : null},
  });
  const server = app.listen(0, '127.0.0.1'); await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const headers = {authorization: 'Bearer ' + 'x'.repeat(32)};
  const body = await (await fetch('http://127.0.0.1:' + server.address().port + '/v1/metrics', {headers})).json();
  assert.equal(body.ok, true);
  assert.equal(body.dispatchPending, 1);
  assert.equal(body.unknownCount, 4);
  assert.ok(body.oldestOutstandingAgeMs >= 4000);
});
