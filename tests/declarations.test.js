'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {createDeclarations, ACCEPT, UPDATE} = require('../src/services/declarations');
const {hash} = require('../src/utils/protocol');
// Transport fixtures only: no Redis process, network, AOF, or restart claim.
function fixture() {
  const calls = [], rows = new Map();
  const f = {settings: ['appendonly','yes','appendfsync','always','maxmemory-policy','noeviction'], info: 'aof_enabled:1\r\naof_last_write_status:ok\r\n', ack: [1,0]};
  const redis = {
    config: async (...args) => {calls.push(['config',...args]); return f.settings;},
    info: async () => f.info,
    get: async key => rows.get(key) || null,
    call: async (...args) => {calls.push(['call',...args]); if(args[0]==='COMMAND') return f.unsupported ? [null] : [['waitaof',4]]; if(f.ack instanceof Error) throw f.ack; return f.ack;},
    eval: async (...args) => {
      calls.push(['eval',...args]);
      if(f.result) return f.result;
      if(args[0] === ACCEPT) {const duplicate=rows.has(args[2]);const value = rows.get(args[2]) || args[14]; rows.set(args[2],value); return JSON.stringify({record:JSON.parse(value),duplicate});}
      return JSON.stringify({id: hash('decl-one'),revision:2,status:'PROCESSING',job:{status:'PROCESSING'}});
    },
  };
  const options={redis, config:{prefix:'test'}, store:{base:'test:service:',record:id=>'test:service:record:'+id,get:async()=>({status:f.nativeStatus||'waiting'})}};
  return Object.assign(f, {calls, rows, options, service: createDeclarations(options)});
}
const input = () => ({key:'decl-one',identityKey:'identity-one',ticketId:'ticket-one',job:{fingerprint:'a'.repeat(64),data:{unit:'kg',quantity:12},uploadRefs:'[]'}});
test('accept persists complete payload and native dispatcher indexes before same-client AOF barrier',async()=>{
  const f=fixture(), value=await f.service.accept(input()), call=f.calls.find(c=>c[0]==='eval');
  assert.deepEqual(value,{id:hash(input().key),accepted:true,durable:true,status:'QUEUED',duplicate:false});
  assert.equal(call[2],8);assert.equal(call[6],'test:service:record:'+value.id);
  assert.deepEqual(call.slice(7,11),['test:service:sequence','test:service:dispatch','test:service:history','test:service:queue-status:background:waiting']);
  const native=JSON.parse(call[17]);assert.equal(native.id,value.id);assert.equal(native.type,'qr-recovery');assert.deepEqual(native.payload,{key:value.id,durable:true});
  assert.deepEqual(f.calls.at(-1),['call','WAITAOF',1,0,5000]);
  assert.equal(call[19],'test:service:declaration:');
  const raw=await f.service.get({id:value.id});assert.equal(raw.attempts,0);assert.equal(raw.revision,1);assert.deepEqual(raw.job,input().job);
});
test('readiness exposes only sanitized version and availability',async()=>{
  const f=fixture();assert.deepEqual(await f.service.health(),{version:1,available:true});
  for(const [i,v] of [[1,'no'],[3,'everysec'],[5,'allkeys-lru']]){const h=fixture();h.settings[i]=v;assert.deepEqual(await h.service.health(),{version:1,available:false});await assert.rejects(h.service.accept(input()),/DURABLE_DECLARATIONS_UNAVAILABLE/);assert.equal(h.calls.some(c=>c[0]==='eval'),false);}
  f.info='aof_enabled:1\naof_last_write_status:err';assert.deepEqual(await f.service.health(),{version:1,available:false});
});
test('missing CONFIG permission and missing persistence fields fail closed',async()=>{
  const f=fixture();f.settings=null;assert.deepEqual(await f.service.health(),{version:1,available:false});
  const h=fixture();h.info='';assert.equal((await h.service.health()).available,false);
});
test('lost AOF acknowledgment after commit never returns durable success or falls back',async()=>{
  for(const ack of [[0,0],null,[],['invalid',0],new Error('unknown command')]){const f=fixture();f.ack=ack;await assert.rejects(f.service.accept(input()),/DURABLE_DECLARATION_ACK_UNCONFIRMED/);assert.equal(f.rows.size,1);assert.equal(f.calls.filter(c=>c[0]==='eval').length,1);}
});
test('replay still requires its own same-client barrier',async()=>{
  const f=fixture(), first=await f.service.accept(input());const second=await f.service.accept(input());assert.deepEqual(second,{...first,duplicate:true});assert.equal(f.calls.filter(c=>c[1]==='WAITAOF').length,2);
});
test('atomic admission outcomes retain their distinct fail-closed errors',async()=>{
  for(const [result,code] of [['CONFLICT','IDEMPOTENCY_CONFLICT'],['PENDING','SUBMISSION_PENDING'],['TICKET_USED','TICKET_USED'],['CORRUPT','DURABLE_DECLARATION_STATE_INVALID']]){const f=fixture();f.result=result;await assert.rejects(f.service.accept(input()),e=>e.message===code);assert.equal(f.calls.some(c=>c[1]==='WAITAOF'),false);}
});
test('updates send exact remote owner lock and revision and retain immutable payload',async()=>{
  const f=fixture(), id=hash('decl-one');await f.service.accept(input());f.calls.length=0;await f.service.update({id,owner:'owner-one',expectedRevision:1,status:'PROCESSING',data:{unit:'changed'},job:{fingerprint:'replacement'},attempts:1});
  const c=f.calls.find(c=>c[0]==='eval');assert.equal(c[1],UPDATE);assert.equal(c[2],3);assert.equal(c[4],'test:lock:'+hash('durable-declaration/'+id));assert.equal(c[5],'test:service:declaration-identity:'+hash('identity-one'));assert.equal(c[6],'owner-one');assert.equal(c[7],'1');assert.deepEqual(JSON.parse(c[8]),{status:'PROCESSING',attempts:1});assert.equal(f.calls.at(-1)[1],'WAITAOF');
});
test('owner/revision/terminal failures never acknowledge update',async()=>{
  for(const result of ['LEASE_LOST','REVISION_CONFLICT','TERMINAL','MISSING','TRANSITION','CORRUPT']){const f=fixture();await f.service.accept(input());f.calls.length=0;f.result=result;await assert.rejects(f.service.update({id:hash('decl-one'),owner:'owner',expectedRevision:1,status:'SUCCEEDED',attempts:1}));assert.equal(f.calls.some(c=>c[1]==='WAITAOF'),false);}
});
test('malformed requests make no Redis writes',async()=>{
  const f=fixture();await assert.rejects(f.service.accept({...input(),job:{}}));await assert.rejects(f.service.update({id:'bad',owner:'owner',expectedRevision:1,status:'PROCESSING',attempts:1}));for(const attempts of [-1,6,NaN,1.5])await assert.rejects(f.service.update({key:'decl-one',owner:'owner',expectedRevision:1,status:'PROCESSING',attempts}));await assert.rejects(f.service.execute({op:'unexpected'}));assert.equal(f.calls.length,0);
});
test('Lua includes atomic ticket ceiling, immutable replay, ownership, revision and no TTL',()=>{
  assert.match(ACCEPT,/SCARD.*>=5/);assert.match(ACCEPT,/r.fingerprint~=ARGV\[1\]/);assert.match(ACCEPT,/SET',KEYS\[1\],old/);assert.match(ACCEPT,/ARGV\[9\]\.\.identity/);assert.match(UPDATE,/GET',KEYS\[2\].*ARGV\[1\]/);assert.match(UPDATE,/r.revision~=tonumber/);assert.doesNotMatch(ACCEPT+UPDATE,/EXPIRE|PEXPIRE|SETEX/);assert.doesNotMatch(UPDATE,/r\.job\[/);assert.match(UPDATE,/GET',KEYS\[3\]\)==r.id/);
});
test('native exhausted or unknown delivery is visible without replacing immutable payload or successful raw state',async()=>{
  for(const state of ['failed','unknown']){const f=fixture(),ack=await f.service.accept(input());f.nativeStatus=state;const raw=await f.service.get({id:ack.id});assert.equal(raw.status,'FAILED');assert.equal(raw.manualReconciliation,true);assert.deepEqual(raw.job,input().job);const key='test:service:declaration:'+ack.id,stored=JSON.parse(f.rows.get(key));assert.equal(stored.status,'QUEUED');stored.status='SUCCEEDED';stored.result={id:'saved'};f.rows.set(key,JSON.stringify(stored));assert.equal((await f.service.get({id:ack.id})).status,'SUCCEEDED');}
});
test('object reconstruction and barrier-loss replay retain full raw job (fixture, not Redis restart)',async()=>{
  const f=fixture();f.ack=[0,0];await assert.rejects(f.service.accept(input()));const rebuilt=createDeclarations(f.options);f.ack=[1,0];const ack=await rebuilt.accept(input());assert.equal(ack.duplicate,true);assert.deepEqual((await rebuilt.get({id:ack.id})).job,input().job);assert.equal(f.rows.size,1);
});
test('unsupported WAITAOF prevents readiness and admission before the Lua commit',async()=>{
  const f=fixture();f.unsupported=true;assert.deepEqual(await f.service.health(),{version:1,available:false});await assert.rejects(f.service.accept(input()),/DURABLE_DECLARATIONS_UNAVAILABLE/);assert.equal(f.calls.some(c=>c[0]==='eval'),false);
});
test('successful raw declaration reports native Sheets synchronization without changing its status or payload',async()=>{
  const f=fixture(),ack=await f.service.accept(input()),key='test:service:declaration:'+ack.id;
  const first=hash('sheet-first'),second=hash('sheet-second');
  const row=JSON.parse(f.rows.get(key));row.status='SUCCEEDED';row.job.scope='fixture-sheet';row.result={id:'saved',_sheetJobs:[first,second]};f.rows.set(key,JSON.stringify(row));
  const native=new Map([[first,{id:first,status:'completed',data:{type:'sheet-write',payload:{spreadsheetId:'fixture-sheet'}}}],[second,{id:second,status:'waiting',data:{type:'sheet-write',payload:{spreadsheetId:'fixture-sheet'}}}]]);
  f.options.store.get=async id=>native.get(id);
  let value=await f.service.get({id:ack.id});assert.equal(value.status,'SUCCEEDED');assert.deepEqual(value.synchronization,{status:'PENDING'});assert.deepEqual(value.result,row.result);
  native.get(second).status='completed';value=await f.service.get({id:ack.id});assert.deepEqual(value.synchronization,{status:'SUCCEEDED'});
  for(const state of ['failed','unknown']){native.get(second).status=state;value=await f.service.get({id:ack.id});assert.equal(value.status,'SUCCEEDED');assert.deepEqual(value.synchronization,{status:'FAILED',manualReconciliation:true});}
  assert.equal(f.rows.get(key),JSON.stringify(row));
});
test('sync fails closed for missing intents, malformed IDs, wrong types and target mismatch; empty evidence stays pending',async()=>{
  const f=fixture(),ack=await f.service.accept(input()),key='test:service:declaration:'+ack.id,id=hash('sheet-intent');
  const row=JSON.parse(f.rows.get(key));row.status='SUCCEEDED';row.job.scope='fixture-sheet';
  const cases=[{ids:[id],native:null},{ids:['bad'],native:null},{ids:'not-an-array',native:null},
    {ids:[id],native:{id,status:'completed',data:{type:'email',payload:{spreadsheetId:'fixture-sheet'}}}},
    {ids:[id],native:{id,status:'completed',data:{type:'sheet-write',payload:{spreadsheetId:'other-sheet'}}}}];
  for(const entry of cases){row.result={id:'saved',_sheetJobs:entry.ids};f.rows.set(key,JSON.stringify(row));f.options.store.get=async()=>entry.native;const result=await f.service.get({id:ack.id});assert.equal(result.status,'SUCCEEDED');assert.equal(result.synchronization.status,'FAILED');assert.equal(result.synchronization.manualReconciliation,true);}
  for(const ids of [undefined,[]]){row.result={id:'saved',...(ids?{_sheetJobs:ids}:{})};f.rows.set(key,JSON.stringify(row));assert.deepEqual((await f.service.get({id:ack.id})).synchronization,{status:'PENDING'});}
});
test('native dispatcher reconstructs the persisted intent and raw hydration after object reconstruction',async()=>{
  const f=fixture(),ack=await f.service.accept(input()),call=f.calls.find(c=>c[0]==='eval'),native=JSON.parse(call[17]);
  const pending=new Set([ack.id]),deliveries=[];
  const redis={hgetall:async()=>({data:JSON.stringify(native),queue:'background',status:'waiting'}),
    zrange:async key=>key.endsWith(':dispatch')?[...pending]:[],zrem:async(_key,id)=>pending.delete(id),
    eval:async function ack(_script,_n,_key,id){return this.zrem(_key,id);}};
  const store=require('../src/services/store').createStore(redis,{background:{add:async(...args)=>deliveries.push(args)}},{prefix:'test'});
  assert.equal(await store.dispatch(),1);assert.equal(deliveries[0][0],'qr-recovery');assert.deepEqual(deliveries[0][1].payload,{key:ack.id,durable:true});assert.equal(deliveries[0][2].jobId,ack.id);assert.equal(pending.size,0);
  const rebuilt=createDeclarations(f.options);assert.deepEqual((await rebuilt.get({id:deliveries[0][1].payload.key})).job,input().job);
});
test('legacy readiness remains HTTP 200 on healthy Redis when durable capability is unavailable',async()=>{
  const f=fixture();f.unsupported=true;let pings=0;f.options.redis.ping=async()=>{pings++;return 'PONG';};
  const app=require('../src/app').createApp({...f.options,queues:{},config:{prefix:'test',namespace:'fixture',token:'fixture-only-token'.repeat(3)}});
  const layer=app._router.stack.find(layer=>layer.route?.path==='/health/ready');
  const response={statusCode:200,status(code){this.statusCode=code;return this;},json(body){this.body=body;return this;}};
  await layer.route.stack[0].handle({},response,error=>{throw error;});
  assert.equal(pings,1);assert.equal(response.statusCode,200);assert.deepEqual(response.body,{ok:true,durableDeclarations:{version:1,available:false}});
  await assert.rejects(f.service.accept(input()),/DURABLE_DECLARATIONS_UNAVAILABLE/);assert.equal(f.calls.some(c=>c[0]==='eval'),false);
});
test('declarationReleaseIdentity releases missing or terminal reservations safely',async()=>{
  const f=fixture(), id=hash('decl-one'), identityKey='identity-release-test';
  const redisKey='test:service:declaration-identity:'+hash(identityKey);
  const declKey='test:service:declaration:'+id;
  f.options.redis.del=async key=>{f.rows.delete(key);return 1;};
  let res=await f.service.execute({op:'declarationReleaseIdentity',identityKey});
  assert.deepEqual(res,{released:false,found:null});
  f.rows.set(redisKey,id);
  res=await f.service.execute({op:'declarationReleaseIdentity',identityKey});
  assert.deepEqual(res,{released:true,id,status:'MISSING'});
  assert.equal(f.rows.has(redisKey),false);
  f.rows.set(redisKey,id);f.rows.set(declKey,JSON.stringify({id,status:'FAILED'}));
  res=await f.service.execute({op:'declarationReleaseIdentity',identityKey});
  assert.deepEqual(res,{released:true,id,status:'FAILED'});
  assert.equal(f.rows.has(redisKey),false);
  f.rows.set(redisKey,id);f.rows.set(declKey,JSON.stringify({id,status:'PROCESSING'}));
  res=await f.service.execute({op:'declarationReleaseIdentity',identityKey});
  assert.deepEqual(res,{released:false,id,status:'PROCESSING'});
  assert.equal(f.rows.has(redisKey),true);
  res=await f.service.execute({op:'declarationReleaseIdentity',identityKey,force:true});
  assert.deepEqual(res,{released:true,id,status:'PROCESSING'});
  assert.equal(f.rows.has(redisKey),false);
});
