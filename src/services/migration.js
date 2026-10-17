'use strict';
const {createHash} = require('node:crypto');
const {canonical, hash, validate} = require('../utils/protocol');
const digest = snapshot => createHash('sha256').update(canonical(snapshot)).digest('hex');
function plan(snapshot, target) {
  if (snapshot.version !== 1 || !snapshot.namespace || !Array.isArray(snapshot.sheets) || !Array.isArray(snapshot.jobs)) throw Error('INVALID_SNAPSHOT');
  if (!target.spreadsheetId || !target.tabs) throw Error('TARGET_SHEET_MAPPING_REQUIRED');
  const blocked = [], submissions = [];
  const sourceSheets = new Map(snapshot.sheets.map(row => [row.id, row]));
  const sheetKey = id => `migration:${snapshot.namespace}:sheet:${id}`;
  const ordered = snapshot.sheets.slice().sort((a, b) => BigInt(a.entity_version) < BigInt(b.entity_version) ? -1 : BigInt(a.entity_version) > BigInt(b.entity_version) ? 1 : String(a.id).localeCompare(String(b.id)));
  for (const row of ordered) {
    if (['done', 'superseded'].includes(row.status)) continue;
    if (row.status !== 'pending' || Number(row.attempts) !== 0 || row.recovered) {
      blocked.push({id: row.id, source: 'sheet_outbox', code: 'SOURCE_OUTCOME_REVIEW_REQUIRED'}); continue;
    }
    const sheet = target.tabs[row.sheet];
    if (!sheet) {blocked.push({id: row.id, source: 'sheet_outbox', code: 'MISSING_SHEET_MAPPING'}); continue;}
    const submission = {type: 'sheet-write', key: sheetKey(row.id), maxAttempts: Number(row.max_attempts || 5),
      availableAt: row.available_at, payload: {spreadsheetId: target.spreadsheetId, sheet, recordId: row.record_id, op: row.op, rowValues: row.row_values}};
    try {validate(submission); submissions.push(submission);} catch {blocked.push({id: row.id, source: 'sheet_outbox', code: 'INVALID_SOURCE_ROW'});}
  }
  const importedSheetKeys = new Set(submissions.map(row => row.key));
  for (const row of snapshot.jobs) {
    if (row.status === 'completed') continue;
    if (row.status !== 'pending' || Number(row.attempts) !== 0) {blocked.push({id: row.id, source: 'job_queue', code: 'SOURCE_OUTCOME_REVIEW_REQUIRED'}); continue;}
    const submission = {type: row.type, key: row.idempotency_key, payload: row.payload, maxAttempts: Number(row.max_attempts || 5)};
    const dependency = row.payload?.sheetJobId;
    if (dependency) {
      const source = sourceSheets.get(dependency);
      if (importedSheetKeys.has(sheetKey(dependency))) submission.dependsOn = hash(sheetKey(dependency));
      else if (source?.status !== 'done') {blocked.push({id: row.id, source: 'job_queue', code: 'DEPENDENCY_NOT_CONFIRMED'}); continue;}
    } else if (row.available_at === 'infinity') {blocked.push({id: row.id, source: 'job_queue', code: 'UNRESOLVED_HOLD'}); continue;}
    if (row.available_at !== 'infinity') submission.availableAt = row.available_at;
    try {validate(submission); submissions.push(submission);} catch {blocked.push({id: row.id, source: 'job_queue', code: 'INVALID_SOURCE_ROW'});}
  }
  return {version: 1, snapshotDigest: digest(snapshot), namespace: snapshot.namespace, blocked, submissions,
    safeToImport: blocked.length === 0, sourceMustRemainFrozen: true};
}
module.exports = {plan, digest};
