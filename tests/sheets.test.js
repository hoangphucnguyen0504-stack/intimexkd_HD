'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createSheetsProcessor, createGoogleAdapter } = require('../src/processors/sheets.js');

function fixture({ rows = [], op = 'append', effect, failure, mutate, governor } = {}) {
  const events = [];
  let reads = 0;
  const job = { data: { type: 'sheet-write', key: 'k', payload: {
    spreadsheetId: 'book', sheet: "O'Brien", recordId: 'id', op, rowValues: ['id', 'value'],
  }, ...(effect ? { effect: { status: effect } } : {}) }, async updateData(data) {
    events.push(data.effect.status); this.data = data;
  } };
  const google = { async getValues({ range }) {
    events.push(['read', range]); reads++;
    mutate?.(rows, reads);
    if (range.endsWith('!A:A')) return rows.map(r => [r[0]]);
    const index = Number(range.match(/!(?:A)?(\d+)/)[1]) - 1;
    return [range.includes('!A') ? [rows[index]?.[0]] : [...(rows[index] || [])]];
  } };
  for (const method of ['append', 'update', 'clear']) google[method] = async args => {
    events.push([method, args]);
    if (failure) throw failure;
  };
  const process = createSheetsProcessor({ google, governor: async kind => {
    events.push(`quota:${kind}`);
    await governor?.(kind, job);
  } });
  return { job, events, process };
}
const writes = events => events.filter(e => Array.isArray(e) && ['append', 'update', 'clear'].includes(e[0]));

test('append persists started before write and confirms; fresh reads use escaped sheet', async () => {
  const f = fixture(); await f.process(f.job);
  assert.equal(writes(f.events).length, 1);
  assert.ok(f.events.indexOf('started') < f.events.findIndex(e => e[0] === 'append'));
  assert.equal(f.events[f.events.findIndex(e => e[0] === 'append') - 1], 'started');
  assert.equal(f.job.data.effect.status, 'confirmed');
  assert.equal(f.events.filter(e => e === 'quota:read').length, 2);
  assert.equal(f.events.filter(e => e === 'quota:write').length, 1);
  assert.equal(writes(f.events)[0][1].range, "'O''Brien'!A:A");
});
test('existing exact append is confirmed without writing; conflicts and duplicates are permanent', async () => {
  const exact = fixture({ rows: [['id', 'value']] }); await exact.process(exact.job);
  assert.equal(writes(exact.events).length, 0);
  for (const [rows, code] of [[[['id', 'different']], 'SHEET_ID_CONFLICT'],
    [[['id'], ['id']], 'SHEET_DUPLICATE_ID']]) {
    const f = fixture({ rows });
    await assert.rejects(f.process(f.job), e => e.code === code && !e.retryable);
    assert.equal(writes(f.events).length, 0);
  }
});
test('update resolves fresh row and never inserts missing IDs; delete clears entire matching row', async () => {
  for (const [op, method] of [['update', 'update'], ['delete', 'clear']]) {
    const f = fixture({ rows: [['other'], ['id', 'old']], op });
    f.job.data.payload.sheetRow = 99;
    await f.process(f.job);
    assert.equal(writes(f.events)[0][0], method);
    assert.equal(writes(f.events)[0][1].range, op === 'delete' ? "'O''Brien'!2:2" : "'O''Brien'!A2:B2");
  }
  const f = fixture({ op: 'update' });
  await assert.rejects(f.process(f.job), { code: 'SHEET_ID_NOT_FOUND' });
  assert.equal(writes(f.events).length, 0);
});
test('external movement or ID change prevents mutation', async () => {
  for (const changeAt of [3, 4]) {
    const f = fixture({ op: 'update', rows: [['id', 'old']], mutate(rows, reads) {
      if (reads === changeAt) rows[0][0] = 'other';
    } });
    await assert.rejects(f.process(f.job), { code: 'SHEET_ROW_CHANGED' });
    assert.equal(writes(f.events).length, 0);
  }
});
test('stalled started jobs reconcile exact content only and never blindly append', async () => {
  const exact = fixture({ effect: 'started', rows: [['id', 'value']] });
  await exact.process(exact.job); assert.equal(writes(exact.events).length, 0);
  for (const op of ['append', 'update', 'delete']) {
    for (const rows of [[], [['id', 'old']]]) {
      if (op === 'delete' && !rows.length) continue;
      const f = fixture({ effect: 'started', op, rows });
      await assert.rejects(f.process(f.job), e => e.unknown && !e.retryable && e.code === 'SHEET_OUTCOME_UNKNOWN');
      assert.equal(writes(f.events).length, 0);
    }
  }
});
test('absent deletes reconcile both started and confirmed without another write', async () => {
  for (const effect of ['started', 'confirmed']) {
    const f = fixture({ op: 'delete', effect });
    assert.deepEqual(await f.process(f.job), { status: 'confirmed' });
    assert.equal(f.job.data.effect.status, 'confirmed');
    assert.equal(writes(f.events).length, 0);
  }
});
test('quota rejection leaves no started marker and a retry can append', async () => {
  let reject = true;
  const f = fixture({ governor: async (kind, job) => {
    if (kind === 'write') {
      assert.equal(job.data.effect, undefined);
      if (reject) throw new Error('local quota');
    }
  } });
  await assert.rejects(f.process(f.job), /local quota/);
  assert.equal(f.job.data.effect, undefined);
  assert.equal(writes(f.events).length, 0);
  reject = false;
  await f.process(f.job);
  assert.equal(writes(f.events).length, 1);
});
test('final verification read 429 leaves no started marker and allows retry', async () => {
  for (const op of ['append', 'update']) {
    const failAt = op === 'append' ? 2 : 4;
    const f = fixture({ op, rows: op === 'append' ? [] : [['id', 'old']],
      mutate(_rows, reads) { if (reads === failAt) throw { response: { status: 429 } }; } });
    await assert.rejects(f.process(f.job), e => e.retryable && !e.unknown);
    assert.equal(f.job.data.effect, undefined);
    assert.equal(writes(f.events).length, 0);
    await f.process(f.job);
    assert.equal(writes(f.events).length, 1);
  }
});
test('null cells normalize to blanks for writes and exact reconciliation', async () => {
  const f = fixture();
  f.job.data.payload.rowValues = ['id', null, 'value'];
  await f.process(f.job);
  assert.deepEqual(writes(f.events)[0][1].values, [['id', '', 'value']]);
  assert.deepEqual(f.job.data.payload.rowValues, ['id', null, 'value']);
  const g = fixture({ effect: 'started', rows: [['id', '', 'value']] });
  g.job.data.payload.rowValues = ['id', null, 'value'];
  assert.deepEqual(await g.process(g.job), { status: 'confirmed' });
  assert.equal(writes(g.events).length, 0);
});
test('write network, 503 and timeout outcomes are unknown; explicit 429 allows retry', async () => {
  for (const failure of [new Error('network'), { code: 'ETIMEDOUT' }, { response: { status: 503 } }]) {
    const f = fixture({ failure });
    await assert.rejects(f.process(f.job), e => e.unknown && !e.retryable);
    assert.equal(f.job.data.effect.status, 'started');
  }
  const f = fixture({ failure: { response: { status: 429, headers: { 'retry-after': '2' } } } });
  await assert.rejects(f.process(f.job), e => e.retryable && !e.unknown && e.retryAfterMs === 2000);
  assert.equal(f.job.data.effect.status, 'rejected');
});
test('reads retry only 429/503; permission errors are permanent', async () => {
  for (const status of [429, 503, 500, 403]) {
    const process = createSheetsProcessor({ google: { getValues: async () => { throw { response: { status } }; } } });
    await assert.rejects(process(fixture().job), e => e.retryable === [429, 503].includes(status) && !e.unknown);
  }
});
test('failed started persistence prevents writes; failed confirmation remains reconcilable', async () => {
  const f = fixture(); f.job.updateData = async () => { throw new Error('redis unavailable'); };
  await assert.rejects(f.process(f.job), /redis unavailable/); assert.equal(writes(f.events).length, 0);
  const g = fixture(); const update = g.job.updateData.bind(g.job);
  g.job.updateData = async data => { if (data.effect.status === 'confirmed') throw new Error('lost'); await update(data); };
  assert.deepEqual(await g.process(g.job), { status: 'confirmed' });
  assert.equal(g.job.data.effect.status, 'started');
});
test('adapter passes credentials, RAW writes, 20s timeout and disables retries without network', async () => {
  for (const env of [{ GOOGLE_SERVICE_ACCOUNT_JSON: '{"client_email":"test"}' },
    { GOOGLE_APPLICATION_CREDENTIALS: '/credentials.json' }]) {
    const calls = []; let authOptions;
    const values = Object.fromEntries(['get', 'append', 'update', 'clear'].map(method => [method,
      async (...args) => { calls.push([method, ...args]); return { data: { values: [['id']] } }; }]));
    const googleapis = { google: { auth: { GoogleAuth: class { constructor(options) { authOptions = options; } } },
      sheets: () => ({ spreadsheets: { values } }) } };
    const adapter = await createGoogleAdapter({ env, googleapis });
    assert.deepEqual(await adapter.getValues({ range: 'A:A' }), [['id']]);
    await adapter.append({ values: [['id']] }); await adapter.update({ values: [['id']] }); await adapter.clear({ range: '1:1' });
    assert.equal(calls.length, 4);
    for (const call of calls) assert.deepEqual(call[2], { timeout: 20000, retry: false, retryConfig: { retry: 0 } });
    assert.equal(calls[1][1].valueInputOption, 'RAW');
    assert.ok(authOptions.credentials || authOptions.keyFile);
  }
});
