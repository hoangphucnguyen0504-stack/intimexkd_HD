'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {validate, authorized, retryDelay} = require('../src/utils/protocol');
const input = () => ({type: 'qr-recovery', key: 'recovery:one', payload: {key: 'one'}});
test('HTTP retransmission keeps ID/digest despite clock advance and JSON order', () => {
  const a = validate(input(), 1), b = validate({...input(), payload: {key: 'one'}}, 200);
  assert.equal(a.id, b.id); assert.equal(a.fingerprint, b.fingerprint);
  assert.match(a.id, /^[a-f0-9]{64}$/);
  assert.notEqual(a.fingerprint, validate({...input(), payload: {key: 'two'}}).fingerprint);
});
test('reject invalid types, payload, attempts and unresolved legacy holds', () => {
  for (const bad of [{type: 'eval'}, {maxAttempts: 0}, {maxAttempts: 13}, {payload: null}, {availableAt: 'infinity'}, {dependsOn: 'bad'}]) {
    assert.throws(() => validate({...input(), ...bad}));
  }
});
test('sheet identity is derived from target and requires first-column ID', () => {
  const job = {type: 'sheet-write', key: 's1', payload: {spreadsheetId: '1234567890', sheet: 'Vehicles', recordId: 'V1', op: 'append', rowValues: ['V1', 4000]}};
  assert.equal(validate(job).entityKey, '1234567890/Vehicles/V1');
  assert.throws(() => validate({...job, payload: {...job.payload, rowValues: ['V2']}}));
});
test('service authorization requires exact bearer, backoff respects Retry-After', () => {
  assert.equal(authorized('Bearer abc', 'abc'), true);
  assert.equal(authorized('Bearer ab', 'abc'), false);
  assert.equal(authorized('', 'abc'), false);
  assert.equal(retryDelay(2, 60000, () => 0), 60000);
  assert.equal(retryDelay(2, 0, () => 0), 1000);
});
