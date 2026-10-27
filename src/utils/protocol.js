'use strict';
const {createHash, timingSafeEqual} = require('node:crypto');
const TYPES = new Set(['sheet-write', 'email', 'weighing-email', 'qr-recovery', 'security-alert', 'sales-email-scan', 'sales-email-schedule']);
function canonical(value) {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value && typeof value === 'object') return '{' + Object.keys(value).sort().map(k => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
  return JSON.stringify(value);
}
const hash = value => createHash('sha256').update(value).digest('hex');
function validate(input, now = Date.now()) {
  if (!input || typeof input !== 'object' || !TYPES.has(input.type)) throw Object.assign(Error('INVALID_JOB_TYPE'), {status: 400});
  if (typeof input.key !== 'string' || input.key.length < 1 || input.key.length > 300) throw Object.assign(Error('INVALID_IDEMPOTENCY_KEY'), {status: 400});
  if (!input.payload || typeof input.payload !== 'object' || Array.isArray(input.payload)) throw Object.assign(Error('INVALID_PAYLOAD'), {status: 400});
  if (Buffer.byteLength(JSON.stringify(input.payload)) > 256 * 1024) throw Object.assign(Error('PAYLOAD_TOO_LARGE'), {status: 413});
  const maxAttempts = input.maxAttempts ?? 5;
  if (!Number.isSafeInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 12) throw Object.assign(Error('INVALID_ATTEMPTS'), {status: 400});
  let availableAt = now;
  if (input.availableAt != null) {
    if (input.availableAt === 'infinity') throw Object.assign(Error('USE_DEPENDS_ON_NOT_INFINITY'), {status: 400});
    availableAt = new Date(input.availableAt).getTime();
    if (!Number.isFinite(availableAt)) throw Object.assign(Error('INVALID_AVAILABLE_AT'), {status: 400});
  }
  if (input.dependsOn != null && !/^[a-f0-9]{64}$/.test(input.dependsOn)) throw Object.assign(Error('INVALID_DEPENDENCY'), {status: 400});
  const payload = input.payload;
  if (input.type === 'sales-email-scan' || input.type === 'sales-email-schedule') {
    const scan = input.type === 'sales-email-scan', fields = scan ? ['userId', 'runId', 'generation'] : ['userId', 'generation', 'scheduleId'];
    const validReference = scan ? typeof payload.runId === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(payload.runId)
      : typeof payload.scheduleId === 'string' && payload.scheduleId.length > 0 && payload.scheduleId.length <= 100;
    if (Object.keys(payload).some(key => !fields.includes(key)) || typeof payload.userId !== 'string' || !payload.userId || payload.userId.length > 200 || !Number.isSafeInteger(payload.generation) || payload.generation < 0 || !validReference) {
      throw Object.assign(Error('INVALID_SALES_EMAIL_JOB'), {status: 400});
    }
  }
  let entityKey = input.entityKey;
  if (input.type === 'sheet-write') {
    if (!['append', 'update', 'delete'].includes(payload.op) || !/^[a-zA-Z0-9_-]{10,200}$/.test(payload.spreadsheetId || '') ||
      typeof payload.sheet !== 'string' || !payload.sheet || payload.sheet.length > 100 || typeof payload.recordId !== 'string' || !payload.recordId ||
      !Array.isArray(payload.rowValues) || payload.rowValues.length < 1 || payload.rowValues.length > 200 ||
      String(payload.rowValues[0]) !== payload.recordId || payload.rowValues.some(v => !['string','number','boolean'].includes(typeof v) || (typeof v === 'number' && !Number.isFinite(v)))) {
      throw Object.assign(Error('INVALID_SHEET_WRITE'), {status: 400});
    }
    entityKey = payload.spreadsheetId + '/' + payload.sheet + '/' + payload.recordId;
  }
  if (entityKey != null && (typeof entityKey !== 'string' || entityKey.length > 500)) throw Object.assign(Error('INVALID_ENTITY_KEY'), {status: 400});
  const normalized = {type: input.type, key: input.key, payload, maxAttempts, dependsOn: input.dependsOn || null, entityKey: entityKey || null};
  // Default 'now' is excluded so a retried HTTP submission retains its identity.
  const fingerprint = hash(canonical({...normalized, availableAt: input.availableAt ?? null}));
  return {...normalized, availableAt, fingerprint, id: hash(input.key)};
}
function authorized(header, secret) {
  const supplied = Buffer.from(header || ''); const expected = Buffer.from('Bearer ' + secret);
  return supplied.length === expected.length && timingSafeEqual(supplied, expected);
}
function retryDelay(attempt, retryAfterMs = 0, random = Math.random) {
  return Math.max(retryAfterMs, Math.min(300000, 1000 * 2 ** Math.min(12, Math.max(0, attempt - 1))) * (0.5 + random() * 0.5));
}
module.exports = {TYPES, canonical, hash, validate, authorized, retryDelay};
