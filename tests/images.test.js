'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const os=require('node:os');
const path=require('node:path');
const {createImages}=require('../src/services/images');
const ID='a'.repeat(32),NS='gate';
const bytes=Buffer.concat([Buffer.from([255,216,255]),Buffer.from('image fixture')]);
const chunk=(more={})=>({namespace:NS,imageId:ID,uploadKey:'ticket-field-slot',field:'Photo',slot:0,index:0,total:1,chunk:bytes.toString('base64'),...more});
async function setup(t,options={}) {
  const dir=await fs.mkdtemp(path.join(os.tmpdir(),'hdoi-images-'));t.after(()=>fs.rm(dir,{recursive:true,force:true}));
  const config={namespace:NS,imageStorageDir:dir,imageAlertAfterMs:900000};
  const jobs=[];const queue={getJob:async()=>null,add:async(name,data,opts)=>{assert.ok(Number.isFinite(opts.delay),'queue delay must be finite');jobs.push({name,data,opts});}};
  const create=extra=>createImages({config,queues:{images:queue,imageMetadata:queue},logger:{error(){}},...options,...extra});
  return {dir,create,jobs,service:create()};
}
test('durable chunk idempotency, conflict, namespace and batch identity validation',async t=>{
  const {service,create,jobs}=await setup(t);
  const image=await service.uploadChunk(NS,ID,chunk(),0);assert.equal(image.state,'HOLD');assert.equal(image.source,'hdoi');
  assert.equal((await create().content(NS,ID)).bytes.toString('hex'),bytes.toString('hex'));
  assert.deepEqual(await service.uploadChunk(NS,ID,chunk(),0),image);
  await assert.rejects(service.uploadChunk(NS,ID,chunk({chunk:Buffer.from('bad').toString('base64')}),0),{status:409});
  await assert.rejects(service.get('other',ID),{status:403});
  await assert.rejects(service.get(NS,'../../etc'),{status:400});
  assert.deepEqual(Object.keys(jobs[0].data),['namespace','imageId']);
});
test('partial restart and rejected magic bytes never acknowledges complete image',async t=>{
  const {service,create}=await setup(t);
  assert.equal((await service.uploadChunk(NS,ID,chunk({total:2,chunk:bytes.subarray(0,3).toString('base64')}),0)).state,'RECEIVING');
  assert.equal((await create().uploadChunk(NS,ID,chunk({total:2,index:1,chunk:bytes.subarray(3).toString('base64')}),1)).state,'HOLD');
  const other='b'.repeat(32);
  await assert.rejects(service.uploadChunk(NS,other,chunk({imageId:other,chunk:Buffer.from('not image').toString('base64')}),0),{status:400});
  await assert.rejects(service.content(NS,other),{status:404});
});
test('COMPLETED committed before cleanup, data sync failure does not upload again',async t=>{
  let uploads=0,verified=0,syncs=0;
  const {service,create,dir,jobs}=await setup(t,{drive:{allocate:async()=>'drive-id',upload:async()=>uploads++,verify:async()=>verified++},sync:async()=>{syncs++;throw Error('offline');}});
  await service.uploadChunk(NS,ID,chunk(),0);await service.processUpload({namespace:NS,imageId:ID});
  const m=await service.get(NS,ID);assert.equal(m.state,'COMPLETED');assert.equal(m.fileId,'drive-id');
  assert.equal(jobs.find(job=>job.name==='imageMetadata')?.opts.delay,0,'first metadata job must be enqueued immediately');
  assert.equal((await service.content(NS,ID)).image.source,'drive');
  await assert.rejects(fs.stat(path.join(dir,NS,ID,'image')),{code:'ENOENT'});
  await create().processUpload({namespace:NS,imageId:ID});await service.processMetadata({namespace:NS,imageId:ID});
  assert.equal(uploads,1);assert.equal(verified,1);assert.equal(syncs,1);assert.equal((await service.get(NS,ID)).syncState,'RETRY');
});
test('timeout after Drive accepts reuses preallocated ID; failed media keeps temporary bytes',async t=>{
  let now=100000,allocations=0,calls=0;const ids=[];
  const {service,create}=await setup(t,{clock:()=>now,drive:{allocate:async()=>{allocations++;return 'stable-id';},upload:async({fileId})=>{ids.push(fileId);if(calls++===0)throw Error('timeout');},verify:async()=>{throw Error('403');}}});
  await service.uploadChunk(NS,ID,chunk(),0);await service.processUpload({namespace:NS,imageId:ID});
  assert.equal((await service.get(NS,ID)).state,'RETRY');assert.deepEqual((await service.content(NS,ID)).bytes,bytes);
  now+=100000;await create().processUpload({namespace:NS,imageId:ID});
  assert.equal(allocations,1);assert.deepEqual(ids,['stable-id','stable-id']);assert.notEqual((await service.get(NS,ID)).state,'COMPLETED');
  assert.deepEqual((await service.content(NS,ID)).bytes,bytes);
});
test('retry-after and sweeper do not reset backoff or duplicate live lease',async t=>{
  let now=100000;const {service,create,jobs}=await setup(t,{clock:()=>now,drive:{allocate:async()=>{throw Object.assign(Error('429'),{retryAfterMs:120000});}}});
  await service.uploadChunk(NS,ID,chunk(),0);await service.processUpload({namespace:NS,imageId:ID});
  const before=await service.get(NS,ID);assert.ok(before.nextAttemptAt>=220000);
  jobs.length=0;await create().sweep();assert.equal(jobs.length,0);assert.equal((await service.get(NS,ID)).nextAttemptAt,before.nextAttemptAt);
  now=before.nextAttemptAt+1;await service.sweep();assert.equal(jobs.length,1);
});
test('concurrent workers cannot upload same image; journal recovers enqueue loss',async t=>{
  let release,entered;const started=new Promise(r=>entered=r),block=new Promise(r=>release=r);let uploads=0;
  const {service,create,jobs}=await setup(t,{drive:{allocate:async()=>'one',upload:async()=>{uploads++;entered();await block;},verify:async()=>{}}});
  await service.uploadChunk(NS,ID,chunk(),0);jobs.length=0;await create().sweep();assert.equal(jobs.length,1);
  const pending=service.processUpload({namespace:NS,imageId:ID});await started;
  await create().processUpload({namespace:NS,imageId:ID});jobs.length=0;await service.sweep();assert.equal(jobs.length,0);release();await pending;assert.equal(uploads,1);
});
test('metadata links survive restart independently of image bytes',async t=>{
  const links=[];const sync=async()=>{};sync.link=async link=>links.push(link);
  const {service,create}=await setup(t,{drive:{allocate:async()=>'one',upload:async()=>{},verify:async()=>{}},sync});
  await service.uploadChunk(NS,ID,chunk(),0);
  await service.links(NS,ID,[{entity:'Vehicles',recordId:'r1',field:'Photo',slot:0,version:1}]);
  await service.processUpload({namespace:NS,imageId:ID});await create().processMetadata({namespace:NS,imageId:ID});
  assert.equal(links.length,1);assert.equal(links[0].recordId,'r1');assert.equal((await service.get(NS,ID)).syncState,'SYNCED');
});
test('Drive adapter checks metadata and media checksum with private server authentication',async()=>{
  const crypto=require('node:crypto'),{Readable}=require('node:stream');let bad=false;const calls=[];
  const fake={google:{auth:{OAuth2:class{setCredentials(value){assert.equal(value.refresh_token,'refresh');}}},drive:()=>({files:{
    generateIds:async()=>({data:{ids:['allocated']}}),
    get:async args=>{calls.push(args);return args.alt==='media'?{data:Readable.from(bad?Buffer.from('wrong'):bytes)}:{data:{id:'allocated',size:String(bytes.length),mimeType:'image/jpeg',md5Checksum:crypto.createHash('md5').update(bytes).digest('hex')}};}
  }})}};
  const drive=await require('../src/services/imageDrive').createImageDrive({DRIVE_CLIENT_ID:'client',DRIVE_CLIENT_SECRET:'secret',DRIVE_REFRESH_TOKEN:'refresh'},fake);
  const expected={fileId:'allocated',mime:'image/jpeg',size:bytes.length,md5:crypto.createHash('md5').update(bytes).digest('hex'),checksum:crypto.createHash('sha256').update(bytes).digest('hex')};
  assert.equal(await drive.allocate(),'allocated');await drive.verify(expected);assert.ok(calls.some(c=>c.alt==='media'));
  bad=true;await assert.rejects(drive.verify(expected),{code:'IMAGE_DRIVE_CONTENT_MISMATCH'});
});
test('HTTP routes keep metadata envelope, bounded batch and completed source redirect',async t=>{
  const express=require('express');const {service}=await setup(t);await service.uploadChunk(NS,ID,chunk(),0);
  const app=express();app.use(express.json());app.use(require('../src/routes/images')(service,{namespace:NS}));app.use((err,_req,res,_next)=>res.status(err.status||500).json({ok:false,code:err.message}));
  const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));t.after(()=>new Promise(r=>server.close(r)));const base='http://127.0.0.1:'+server.address().port;
  const response=await fetch(base+'/status',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({namespace:NS,imageIds:[ID]})});
  assert.equal((await response.json()).images[0].state,'HOLD');
  const invalid=await fetch(base+'/status',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({namespace:NS,imageIds:Array(51).fill(ID)})});assert.equal(invalid.status,400);
  assert.equal((await fetch(base+'/'+ID+'/content?namespace='+NS)).headers.get('content-type'),'image/jpeg');
});
test('business callback persists image links before successful acknowledgement',async()=>{
  const {createBackendProcessor}=require('../src/processors/backend');const link={imageId:ID,entity:'Vehicles',recordId:'one',field:'Photo',slot:0,version:1};let accepted=0;
  const response=async()=>({ok:true,json:async()=>({ok:true,result:{_imageLinks:[link]}})});
  const config={namespace:NS,backendUrl:'https://backend.invalid',backendToken:'s'.repeat(40)};
  const job={id:'job',data:{type:'qr-recovery',payload:{}},attemptsMade:0};
  await assert.rejects(createBackendProcessor(config,response,{images:{links:async()=>{throw Error('disk');}}})(job),{code:'IMAGE_LINK_COMMIT_FAILED',retryable:true});
  const result=await createBackendProcessor(config,response,{images:{links:async(namespace,id,items)=>{assert.equal(namespace,NS);assert.equal(id,ID);assert.equal(items[0].recordId,'one');accepted++;}}})(job);
  assert.equal(result.confirmed,true);assert.equal(accepted,1);
});
