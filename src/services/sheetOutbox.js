'use strict';
const {hash} = require('../utils/protocol');
const mapStatus = status => ({waiting:'pending',delayed:'pending',active:'processing',completed:'done',failed:'failed',unknown:'unknown'}[status] || status);
const invalid = code => Object.assign(Error(code), {status: 400});
function createSheetOutbox({redis, store, queues}) {
  const list = async (key, max = 5000) => {
    if (await redis.zcard(key) > max) throw Object.assign(Error('OUTBOX_QUERY_TOO_LARGE'), {status: 503});
    const ids = await redis.zrange(key, 0, -1);
    return (await Promise.all(ids.map(store.get))).filter(Boolean);
  };
  async function execute(op, body) {
    switch (op) {
      case 'sheetEnqueue': {
        if (body.data?.type !== 'sheet-write') throw invalid('INVALID_SHEET_JOB');
        const result = await store.commit({idempotencyKey: body.data.key, operations: [{kind:'sheet', data:body.data, logicalSheet:body.logicalSheet}]});
        return {...result.jobs[0], op:body.data.payload.op};
      }
      case 'sheetPending': {
        if (typeof body.spreadsheetId !== 'string' || typeof body.sheet !== 'string') throw invalid('INVALID_SHEET_SCOPE');
        const rows = await list(store.base + 'sheet-open:' + hash(body.spreadsheetId + '/' + body.sheet));
        return rows.filter(row => ['waiting','active','delayed','unknown'].includes(row.status)).map(row => ({record_id:row.data.payload.recordId, op:row.data.payload.op, row_values:row.data.payload.rowValues}));
      }
      case 'sheetLookup': {
        if (typeof body.recordId !== 'string') throw invalid('INVALID_RECORD_ID');
        const rows = await list(store.base + 'record-jobs:' + hash(body.recordId));
        const row = rows.filter(row => !body.spreadsheetId || row.data.payload.spreadsheetId === body.spreadsheetId).at(-1);
        return row ? {id:row.id, status:mapStatus(row.status)} : null;
      }
      case 'sheetHistory': {
        const limit = body.limit ?? 20;
        if (!Number.isInteger(limit) || limit < 1 || limit > 50) throw invalid('INVALID_LIMIT');
        const allowed = ['pending','processing','unknown','done','failed','superseded','manual'];
        if (body.status && !allowed.includes(body.status)) throw invalid('INVALID_STATUS');
        let before = '+inf';
        if (body.cursor) {
          try {const cursor = JSON.parse(Buffer.from(body.cursor, 'base64url').toString());
            if (!Number.isSafeInteger(cursor.seq) || cursor.filter !== (body.status || '')) throw Error(); before = '(' + cursor.seq;
          } catch {throw invalid('INVALID_CURSOR');}
        }
        const states = ['waiting','delayed','active','completed','failed','unknown'];
        const counts = {};
        await Promise.all(states.map(async state=>{const n=await redis.zcard(store.base+'queue-status:sheets:'+state);const label=mapStatus(state);counts[label]=(counts[label]||0)+n;}));
        const buckets = body.status ? states.filter(state=>body.status==='manual'?state==='unknown':mapStatus(state)===body.status).map(state=>store.base+'queue-status:sheets:'+state) : [store.base+'sheet-history'];
        const candidateIds = (await Promise.all(buckets.map(key=>redis.zrevrangebyscore(key,before,'-inf','LIMIT',0,limit+1)))).flat();
        const filtered = (await Promise.all(candidateIds.map(store.get))).filter(Boolean).sort((a,b)=>Number(b.seq)-Number(a.seq));
        const chosen = filtered.slice(0, limit);
        const items = await Promise.all(chosen.map(async row => ({jobId:row.id, sheet:row.data.logicalSheet || row.data.payload.sheet,
          recordId:row.data.payload.recordId, op:row.data.payload.op, status:mapStatus(row.status),
          attempts:(await queues.sheets.getJob(row.id))?.attemptsMade || 0, error:row.error || '', at:new Date(Number(row.updatedAt || row.createdAt)).toISOString()})));
        return {available:true, counts, items, nextCursor:filtered.length > limit ? Buffer.from(JSON.stringify({seq:Number(chosen.at(-1).seq),filter:body.status || ''})).toString('base64url') : null};
      }
      case 'sheetReconcile': {
        if (typeof body.reason !== 'string' || body.reason.trim().length < 8) throw invalid('REASON_REQUIRED');
        const row = await store.get(body.id);
        if (row?.data.type !== 'sheet-write' || row.status !== 'unknown') throw Object.assign(Error('OUTBOX_RECOVERY_STATE'),{status:409});
        await store.retry(body.id, true); return {ok:true,status:'pending',message:'Đã yêu cầu đối soát; chưa xác nhận ghi Google Sheets thành công.'};
      }
      default: throw invalid('UNKNOWN_SHEET_OPERATION');
    }
  }
  return {execute};
}
module.exports = {createSheetOutbox, mapStatus};
