'use strict';
// Explicit read-only export; does not import .env, migrate schema, or change jobs.
const fs = require('node:fs'); const path = require('node:path');
const {digest} = require('../src/services/migration');
async function main() {
  const output = process.argv[2], namespace = process.env.APP_NAMESPACE;
  if (!output || !namespace || !process.env.DATABASE_URL || !process.argv.includes('--source-frozen')) throw Error('Requires output path, APP_NAMESPACE, DATABASE_URL and operator --source-frozen attestation');
  const {Client} = require('../../backend/node_modules/pg');
  const url = new URL(process.env.DATABASE_URL); url.searchParams.set('sslmode', 'verify-full');
  const client = new Client({connectionString: url.href, enableChannelBinding: true, connectionTimeoutMillis: 10000, statement_timeout: 30000});
  await client.connect();
  try {
    await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const sheets = await client.query('SELECT id,sheet,record_id,op,entity_version,row_values,sheet_row,status,attempts,max_attempts,recovered,available_at::text FROM sheet_outbox WHERE namespace=$1 ORDER BY entity_version,id LIMIT 10001', [namespace]);
    const jobs = await client.query('SELECT id,type,idempotency_key,payload,status,attempts,max_attempts,available_at::text FROM job_queue WHERE namespace=$1 ORDER BY created_at,id LIMIT 10001', [namespace]);
    if (sheets.rows.length > 10000 || jobs.rows.length > 10000) throw Error('SNAPSHOT_LIMIT_EXCEEDED_REQUIRES_PAGED_EXPORT');
    const snapshot = {version: 1, namespace, exportedAt: new Date().toISOString(), sheets: sheets.rows, jobs: jobs.rows};
    await client.query('COMMIT');
    // Exclusive creation avoids accidentally overwriting an earlier receipt.
    fs.writeFileSync(path.resolve(output), JSON.stringify({digest: digest(snapshot), snapshot}, null, 2), {flag: 'wx', mode: 0o600});
    console.log(JSON.stringify({sheets: sheets.rows.length, jobs: jobs.rows.length, digest: digest(snapshot)}));
  } catch (error) {await client.query('ROLLBACK').catch(() => {}); throw error;}
  finally {await client.end();}
}
if (require.main === module) main().catch(() => {console.error('READ_ONLY_EXPORT_FAILED'); process.exitCode = 1;});
