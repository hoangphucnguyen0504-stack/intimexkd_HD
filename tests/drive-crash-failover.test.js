'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs/promises'),os=require('node:os'),path=require('node:path');
test('crash after create never abandons file when revoked token blocks reconciliation',async t=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'crash-failover-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));
 const id='b'.repeat(32),dir=path.join(root,'gate',id);await fs.mkdir(dir,{recursive:true});
 await fs.writeFile(path.join(dir,'image'),Buffer.from([255,216,255]));
 await fs.writeFile(path.join(dir,'manifest.json'),JSON.stringify({namespace:'gate',imageId:id,state:'UPLOADING',driveFileId:'already-created',credentialId:'old',writeStarted:true,leaseUntil:0,holdAt:1000,attempts:1,version:1,chunks:{}}));
 let rotations=0,allocations=0;
 const service=require('../src/services/images').createImages({config:{namespace:'gate',imageStorageDir:root,imageAlertAfterMs:900000},clock:()=>2000,drive:{allocate:async()=>{allocations++;},upload:async()=>{throw Object.assign(Error(),{noWriteReason:'invalid_grant'});},rotate:async()=>rotations++},logger:{error(){}}});
 await service.processUpload({namespace:'gate',imageId:id});
 const journal=JSON.parse(await fs.readFile(path.join(dir,'manifest.json')));
 assert.equal(journal.driveFileId,'already-created');assert.equal(journal.ambiguousWrite,true);assert.equal(rotations,0);assert.equal(allocations,0);
});
