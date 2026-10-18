'use strict';

// Facade: getValues({spreadsheetId, range}) -> rows; append/update accept
// {..., values: [row]}, clear accepts {..., range}. Each method makes ONE
// request, disables SDK retries, and rejects on error. Governor is per request.
// Main must enforce global write concurrency 1. Sheets offers no compare-and-swap:
// external editors can still move rows between verification and mutation.
const fault = (code, message, extra = {}) => Object.assign(new Error(message), {
  code, retryable: false, unknown: false, ...extra,
});
const unknown = (cause) => fault('SHEET_OUTCOME_UNKNOWN',
  'Sheet write outcome requires reconciliation/manual review', { unknown: true, cause });

function classify(error, write) {
  const status = Number(error.response?.status || error.status || error.code);
  const headers = error.response?.headers;
  const retryAfter = headers?.get?.('retry-after') ?? headers?.['retry-after'];
  if (status === 429 || (!write && status === 503)) {
    const seconds = Number(retryAfter);
    const retryAfterMs = retryAfter == null ? undefined : Number.isFinite(seconds)
      ? Math.max(0, seconds * 1000) : Math.max(0, Date.parse(retryAfter) - Date.now());
    return fault('SHEET_RATE_OR_SERVICE_LIMIT', 'Google request explicitly rejected', {
      retryable: true, status, write, retryAfterMs: Number.isFinite(retryAfterMs) ? retryAfterMs : status === 429 ? 60000 : undefined, cause: error,
    });
  }
  if (write && (!status || status >= 500 || status === 408)) return unknown(error);
  return fault('SHEET_REQUEST_FAILED', 'Google request failed', { status, cause: error });
}

function column(n) {
  let result = '';
  while (n) { n--; result = String.fromCharCode(65 + n % 26) + result; n = Math.floor(n / 26); }
  return result;
}

function createSheetsProcessor({ google, governor = async () => {} }) {
  if (!google) throw new TypeError('google facade required');
  return async function processSheet(job) {
    const data = job.data;
    // Protocol permits null cells; RAW writes and reconciliation use blanks.
    const payload = data?.payload;
    const p = payload && { ...payload, rowValues: Array.isArray(payload.rowValues)
      ? payload.rowValues.map(value => value === null ? '' : value) : payload.rowValues };
    if (data?.type !== 'sheet-write' || typeof data.key !== 'string' || !data.key ||
        !p || typeof p.spreadsheetId !== 'string' || !p.spreadsheetId ||
        typeof p.sheet !== 'string' || !p.sheet ||
        !['append', 'update', 'delete'].includes(p.op) ||
        !['string', 'number'].includes(typeof p.recordId) || String(p.recordId) === '' ||
        !Array.isArray(p.rowValues) || p.rowValues.some(v =>
          !['string', 'number', 'boolean'].includes(typeof v) || (typeof v === 'number' && !Number.isFinite(v))) ||
        (p.op !== 'delete' && (!p.rowValues.length || String(p.rowValues[0]) !== String(p.recordId)))) {
      throw fault('SHEET_INVALID_JOB', 'Invalid normalized sheet-write job');
    }
    const sheet = `'${p.sheet.replace(/'/g, "''")}'`;
    const request = async (kind, method, args) => {
      await governor(kind);
      try { return await google[method]({ spreadsheetId: p.spreadsheetId, ...args }); }
      catch (error) { throw classify(error, kind === 'write'); }
    };
    const resolve = async (rows) => {
      rows ??= await request('read', 'getValues', { range: `${sheet}!A:A` });
      const matches = rows.flatMap((row, i) => i > 0 && String(row[0] ?? '') === String(p.recordId) ? [i + 1] : []);
      if (matches.length > 1) throw fault('SHEET_DUPLICATE_ID', 'Multiple rows have this record ID');
      return matches[0];
    };
    const mark = async status => {
      const next = { ...job.data, effect: { status, at: new Date().toISOString() } };
      await job.updateData(next);
      job.data = next;
    };
    const confirm = async () => {
      // A lost confirmation must retain started so a stalled retry reconciles.
      try { await mark('confirmed'); } catch { /* external outcome is already confirmed */ }
      return { status: 'confirmed' };
    };
    const row = await resolve();
    const resumed = ['started', 'confirmed'].includes(data.effect?.status);
    if (!row) {
      if (p.op === 'delete') return confirm();
      if (resumed) throw unknown();
    } else if (p.op !== 'delete') {
      // Read the whole row so extra nonempty cells cannot count as an exact match.
      const values = await request('read', 'getValues', { range: `${sheet}!${row}:${row}` });
      const actual = values[0] || [];
      const expected = p.rowValues.slice();
      while (actual.length && actual.at(-1) === '') actual.pop();
      while (expected.length && expected.at(-1) === '') expected.pop();
      if (JSON.stringify(actual) === JSON.stringify(expected)) return confirm();
      if (resumed) throw unknown();
      if (p.op === 'append') throw fault('SHEET_ID_CONFLICT', 'Append ID exists with different content');
    } else if (resumed) {
      // An existing ID proves clear did not take effect, but do not replay a
      // started mutation automatically; an external edit may have restored it.
      throw unknown();
    }
    // Acquire write quota BEFORE the final verification, avoiding a governor
    // wait between verification and the actual write.
    await governor('write');
    // One request supplies both final checks; legacy injected facades retain
    // the sequential path. No verification is reused across quota waits/jobs.
    const finalValues = row && typeof google.batchGetValues === 'function'
      ? await request('read', 'batchGetValues', { ranges: [`${sheet}!A:A`, `${sheet}!A${row}`] })
      : null;
    const current = await resolve(finalValues ? finalValues[0] || [] : undefined);
    if (current !== row) throw fault('SHEET_ROW_CHANGED', 'Target moved before write');
    if (row) {
      const id = finalValues ? finalValues[1] || []
        : await request('read', 'getValues', { range: `${sheet}!A${row}` });
      if (String(id[0]?.[0] ?? '') !== String(p.recordId)) {
        throw fault('SHEET_ROW_CHANGED', 'Target ID changed before write');
      }
    }
    await mark('started'); // Persist only once quota and final reads succeeded.
    try {
      if (p.op === 'append' || (p.op === 'update' && !row)) await google.append({ spreadsheetId: p.spreadsheetId,
        range: `${sheet}!A:A`, values: [p.rowValues] });
      else if (p.op === 'update') await google.update({ spreadsheetId: p.spreadsheetId,
        range: `${sheet}!A${row}:${column(p.rowValues.length)}${row}`, values: [p.rowValues] });
      else await google.clear({ spreadsheetId: p.spreadsheetId, range: `${sheet}!${row}:${row}` });
    } catch (error) {
      const classified = classify(error, true);
      // 429 is an explicit rejection: permit a later safe attempt.
      if (classified.retryable) {
        try { await mark('rejected'); } catch (cause) { throw unknown(cause); }
      }
      throw classified;
    }
    return confirm();
  };
}

async function createGoogleAdapter({ env = process.env, googleapis } = {}) {
  const { google } = googleapis || await import('googleapis');
  const auth = new google.auth.GoogleAuth({
    ...(env.GOOGLE_SERVICE_ACCOUNT_JSON
      ? { credentials: JSON.parse(env.GOOGLE_SERVICE_ACCOUNT_JSON) }
      : { keyFile: env.GOOGLE_APPLICATION_CREDENTIALS }),
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const values = google.sheets({ version: 'v4', auth }).spreadsheets.values;
  const options = () => ({ timeout: 20000, retry: false, retryConfig: { retry: 0 } });
  return {
    async getValues(args) {
      const result = await values.get({ ...args, valueRenderOption: 'UNFORMATTED_VALUE' }, options());
      return result.data.values || [];
    },
    async batchGetValues(args) {
      const result = await values.batchGet({ ...args, valueRenderOption: 'UNFORMATTED_VALUE' }, options());
      return (result.data.valueRanges || []).map(item => item.values || []);
    },
    append: ({ values: rows, ...args }) => values.append({ ...args,
      valueInputOption: 'RAW', insertDataOption: 'INSERT_ROWS', requestBody: { values: rows } }, options()),
    update: ({ values: rows, ...args }) => values.update({ ...args,
      valueInputOption: 'RAW', requestBody: { values: rows } }, options()),
    clear: args => values.clear({ ...args, requestBody: {} }, options()),
  };
}

exports.createSheetsProcessor = createSheetsProcessor;
exports.createGoogleAdapter = createGoogleAdapter;
