'use strict';
const fs = require('node:fs'); const path = require('node:path'); const {spawnSync} = require('node:child_process');
const root = path.resolve(__dirname, '..'); let count = 0;
function scan(dir) {
  for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
    const file = path.join(dir, entry.name);
    if (entry.isDirectory()) scan(file);
    else if (/\.(?:js|cjs)$/.test(entry.name)) {
      const result = spawnSync(process.execPath, ['--check', file], {encoding: 'utf8', windowsHide: true});
      if (result.status !== 0) {console.error(result.stderr); process.exit(1);} count++;
    }
  }
}
for (const dir of ['src', 'tests', 'scripts']) scan(path.join(root, dir));
JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8'));
console.log(JSON.stringify({syntaxFiles: count, errors: 0}));
