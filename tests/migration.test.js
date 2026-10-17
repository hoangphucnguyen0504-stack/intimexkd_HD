'use strict';
const {test} = require('node:test'); const assert = require('node:assert/strict');
const {plan, digest} = require('../src/services/migration');
const target = {spreadsheetId: '1234567890', tabs: {vehicles: 'Vehicles'}};
const snapshot = () => ({version: 1, namespace: 'test', sheets: [{id: 'old1', sheet: 'vehicles', record_id: 'V1', op: 'append', entity_version: '1', row_values: ['V1', 4000], status: 'pending', attempts: 0, max_attempts: 5, available_at: '2026-10-17T00:00:00Z'}],
  jobs: [{id: 'mail1', type: 'email', idempotency_key: 'mail:one', payload: {key: 'mail:one', sheetJobId: 'old1'}, status: 'pending', attempts: 0, max_attempts: 5, available_at: 'infinity'}]});
test('migration preserves units, identity and sheet-before-email dependency', () => {
  const source = snapshot(), before = digest(source), result = plan(source, target);
  assert.equal(result.safeToImport, true); assert.equal(result.submissions.length, 2);
  assert.equal(result.submissions[0].payload.rowValues[1], 4000);
  assert.equal(result.submissions[1].dependsOn, require('../src/utils/protocol').hash(result.submissions[0].key));
  assert.equal(digest(source), before);
  assert.deepEqual(plan(source, target), result);
});
test('unknown outcomes, active jobs and missing dependencies block entire import', () => {
  for (const change of [{status: 'unknown'}, {status: 'processing'}, {attempts: 1}, {recovered: true}]) {
    const source = snapshot(); Object.assign(source.sheets[0], change);
    const result = plan(source, target); assert.equal(result.safeToImport, false); assert.ok(result.blocked.length);
  }
  assert.equal(plan(snapshot(), {...target, tabs: {}}).safeToImport, false);
});
