'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs/promises'),os=require('node:os'),path=require('node:path');
const {driveFailure}=require('../src/services/driveFailure');
const error=reason=>({response:{status:403,data:{error:{errors:[{reason}]}}}});
test('only explicit provider failures rotate, never transient or ambiguous failures',()=>{
 for(const [reason,expected] of [['storageQuotaExceeded','storageQuotaExceeded'],['insufficientFilePermissions','permissionDenied'],['accountDisabled','accountDisabled'],['userRateLimitExceeded','userRateLimitExceeded']])assert.equal(driveFailure(error(reason)),expected);
 for(const reason of ['invalid_grant','invalid_client','unauthorized_client'])assert.equal(driveFailure({response:{status:400,data:{error:reason}}}),reason==='unauthorized_client'?'invalid_client':reason);
 for(const e of [{code:'ECONNRESET'},error('rateLimitExceeded'),{response:{status:429}},error('dailyLimitExceeded'),{response:{status:500}},{response:{status:403}},{response:{status:404}}])assert.equal(driveFailure(e),null);
 assert.equal(driveFailure({code:'DRIVE_FOLDER_INVALID',status:503}),'permissionDenied');
});
test('explicit no-write storage failure rotates and preserves bytes; ambiguous upload never changes owner',async t=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'failover-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));
 const config={namespace:'gate',imageStorageDir:root,imageAlertAfterMs:900000},id='a'.repeat(32),bytes=Buffer.from([255,216,255,1]),rotations=[];
 let now=1000,mode='storage',allocations=0;
 const drive={allocate:async()=>({fileId:'file-'+(++allocations),credentialId:'account-'+allocations}),upload:async()=>{if(mode==='storage')throw Object.assign(Error(),{noWriteReason:'storageQuotaExceeded'});if(mode==='ambiguous')throw Error('timeout');},verify:async()=>{},rotate:async(id,reason)=>rotations.push({id,reason})};
 const service=require('../src/services/images').createImages({config,drive,clock:()=>now,logger:{error(){}}});
 await service.uploadChunk('gate',id,{namespace:'gate',imageId:id,index:0,total:1,uploadKey:'test-key-123',field:'photo',slot:0,chunk:bytes.toString('base64')},0);
 await service.processUpload({namespace:'gate',imageId:id});
 assert.deepEqual(rotations,[{id:'account-1',reason:'storageQuotaExceeded'}]);assert.deepEqual((await service.content('gate',id)).bytes,bytes);
 mode='ambiguous';now+=400000;await service.processUpload({namespace:'gate',imageId:id});
 mode='storage';now+=400000;await service.processUpload({namespace:'gate',imageId:id});assert.equal(rotations.length,1);assert.equal(allocations,2);
 mode='success';now+=400000;await service.processUpload({namespace:'gate',imageId:id});assert.equal((await service.get('gate',id)).state,'COMPLETED');assert.equal(allocations,2);
});
