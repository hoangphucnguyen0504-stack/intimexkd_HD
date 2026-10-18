'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {createSheetsProcessor, createGoogleAdapter} = require('../src/processors/sheets');

async function fixture({rows = [], op = 'update', legacy = false, failure, batchFailure, onWriteQuota} = {}) {
  const calls = [], quota = [], effects = [];
  const read = range => {
    if (range.endsWith('!A:A')) return [['ID'], ...rows.map(row => [row[0]])];
    const index = Number(range.match(/!(?:A)?(\d+)/)[1]) - 2;
    return range.includes('!A') ? [[rows[index]?.[0]]] : [rows[index] || []];
  };
  const values = {
    async get(args, options) {
      calls.push({method: 'get', args, options});
      return {data: {values: read(args.range)}};
    },
    async batchGet(args, options) {
      calls.push({method: 'batchGet', args, options});
      if (batchFailure) throw batchFailure;
      return {data: {valueRanges: args.ranges.map(range => ({values: read(range)}))}};
    },
  };
  for (const method of ['append', 'update', 'clear']) values[method] = async (args, options) => {
    calls.push({method, args, options});
    assert.equal(effects.at(-1), 'started');
    if (failure) throw failure;
    return {data: {}};
  };
  const google = await createGoogleAdapter({env: {}, googleapis: {google: {
    auth: {GoogleAuth: class {}},
    sheets: () => ({spreadsheets: {values}}),
  }}});
  if (legacy) delete google.batchGetValues;
  const job = {data: {type: 'sheet-write', key: 'audit', payload: {
    spreadsheetId: 'mock-book', sheet: "O'Brien", recordId: 'id', op, rowValues: ['id', 'new'],
  }}, async updateData(data) {effects.push(data.effect.status); this.data = data;}};
  const process = createSheetsProcessor({google, governor: async kind => {
    quota.push(kind);
    if (kind === 'write') onWriteQuota?.(rows);
  }});
  return {process, job, calls, quota, effects};
}
const counts = f => ({
  reads: f.calls.filter(c => ['get', 'batchGet'].includes(c.method)).length,
  writes: f.calls.filter(c => ['append', 'update', 'clear'].includes(c.method)).length,
});

test('mock SDK boundary baseline: production batch saves one read for existing-row writes', async () => {
  for (const [op, rows, reads, writes] of [
    ['update', [['id', 'old']], 3, 1],
    ['delete', [['id', 'old']], 2, 1],
    ['append', [], 2, 1],
    ['update', [], 2, 1],
    ['append', [['id', 'new']], 2, 0],
    ['delete', [], 1, 0],
  ]) {
    for (const legacy of [false, true]) {
      const f = await fixture({op, rows, legacy});
      await f.process(f.job);
      const expectedReads = reads + (legacy && writes && rows.length ? 1 : 0);
      assert.deepEqual(counts(f), {reads: expectedReads, writes}, op + ' legacy=' + legacy);
      assert.equal(f.quota.filter(k => k === 'read').length, expectedReads);
      assert.equal(f.quota.filter(k => k === 'write').length, writes);
      for (const call of f.calls) {
        assert.deepEqual(call.options, {timeout: 20000, retry: false, retryConfig: {retry: 0}});
        if (['get', 'batchGet'].includes(call.method))
          assert.equal(call.args.valueRenderOption, 'UNFORMATTED_VALUE');
      }
      if (!legacy && writes && rows.length) {
        const batch = f.calls.find(c => c.method === 'batchGet');
        assert.deepEqual(batch.args.ranges, ["'O''Brien'!A:A", "'O''Brien'!A2"]);
      }
    }
  }
});

test('batch verification re-reads after quota wait and rejects movement, replacement and duplicates', async () => {
  for (const change of [
    rows => rows.unshift(['other']),
    rows => {rows[0][0] = 'other';},
    rows => rows.push(['id', 'duplicate']),
  ]) {
    const f = await fixture({rows: [['id', 'old']], onWriteQuota: change});
    await assert.rejects(f.process(f.job), e => ['SHEET_ROW_CHANGED', 'SHEET_DUPLICATE_ID'].includes(e.code));
    assert.deepEqual(counts(f), {reads: 3, writes: 0});
    assert.equal(f.job.data.effect, undefined);
    assert.equal(f.calls.at(-1).method, 'batchGet');
  }
});

test('batch verification rejection cannot persist started or send a write', async () => {
  const f = await fixture({rows: [['id', 'old']], batchFailure: {response: {status: 429}}});
  await assert.rejects(f.process(f.job), e => e.retryable && !e.unknown);
  assert.deepEqual(counts(f), {reads: 3, writes: 0});
  assert.equal(f.job.data.effect, undefined);
});

test('production adapter never retries an ambiguous write; restart only reconciles', async () => {
  for (const [op, rows] of [['append', []], ['update', [['id', 'old']]], ['delete', [['id', 'old']]]]) {
    for (const failure of [new Error('lost response'), {response: {status: 503}}, {code: 'ETIMEDOUT'}]) {
      const f = await fixture({op, rows, failure});
      await assert.rejects(f.process(f.job), e => e.unknown && !e.retryable);
      assert.equal(f.job.data.effect.status, 'started');
      assert.equal(counts(f).writes, 1);
      await assert.rejects(f.process(f.job), e => e.unknown && !e.retryable);
      assert.equal(counts(f).writes, 1);
    }
  }
});

test('landed ambiguous append reconciles exact content including extra-cell rejection', async () => {
  for (const extra of [false, true]) {
    const rows = [];
    const f = await fixture({op: 'append', rows, failure: new Error('lost response')});
    await assert.rejects(f.process(f.job), e => e.unknown);
    rows.push(extra ? ['id', 'new', 'unexpected'] : ['id', 'new']);
    if (extra) await assert.rejects(f.process(f.job), e => e.unknown);
    else assert.deepEqual(await f.process(f.job), {status: 'confirmed'});
    assert.equal(counts(f).writes, 1);
  }
});
