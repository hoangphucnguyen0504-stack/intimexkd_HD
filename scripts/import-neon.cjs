'use strict';
const fs = require('node:fs');
const {plan, digest} = require('../src/services/migration');
async function main() {
  const [snapshotFile, targetFile] = process.argv.slice(2);
  if (!snapshotFile || !targetFile) throw Error('SNAPSHOT_AND_TARGET_MAPPING_REQUIRED');
  const exported = JSON.parse(fs.readFileSync(snapshotFile, 'utf8'));
  if (exported.digest !== digest(exported.snapshot)) throw Error('SNAPSHOT_DIGEST_MISMATCH');
  const result = plan(exported.snapshot, JSON.parse(fs.readFileSync(targetFile, 'utf8')));
  // Dry run is default. Output only technical IDs/counts, never row values.
  console.log(JSON.stringify({digest: result.snapshotDigest, safeToImport: result.safeToImport, jobs: result.submissions.length, blocked: result.blocked}, null, 2));
  if (!process.argv.includes('--apply')) return;
  if (!result.safeToImport || !process.argv.includes('--source-frozen')) throw Error('CUTOVER_BLOCKED');
  const url = new URL(process.env.HDOI_URL);
  if (url.protocol !== 'https:' && !['127.0.0.1', 'localhost'].includes(url.hostname)) throw Error('HTTPS_REQUIRED');
  if (!process.env.HDOI_TOKEN || process.env.HDOI_TOKEN.length < 32) throw Error('TOKEN_REQUIRED');
  for (const input of result.submissions) {
    const response = await fetch(new URL('/v1/jobs', url), {method: 'POST', redirect: 'error', signal: AbortSignal.timeout(10000),
      headers: {authorization: 'Bearer ' + process.env.HDOI_TOKEN, 'content-type': 'application/json'}, body: JSON.stringify(input)});
    if (!response.ok) throw Error('IMPORT_STOPPED_' + response.status);
    const receipt = await response.json();
    if (!receipt.accepted || receipt.id !== require('../src/utils/protocol').hash(input.key)) throw Error('INVALID_IMPORT_RECEIPT');
    console.log(JSON.stringify({imported: receipt.id, status: receipt.status}));
  }
  // Source rows are deliberately untouched. Keep both old consumers and new
  // business ingress stopped until operator verifies full mapping and journals.
}
if (require.main === module) main().catch(error => {console.error(String(error.message).slice(0, 120)); process.exitCode = 1;});
