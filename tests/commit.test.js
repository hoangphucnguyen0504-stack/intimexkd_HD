'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const {createStore}=require('../src/services/store');
const {hash}=require('../src/utils/protocol');
const sheet={type:'sheet-write',key:'write-one',payload:{spreadsheetId:'1234567890',sheet:'Vehicles',recordId:'V1',op:'append',rowValues:['V1',4000]}};
test('invalid later operation rejects a whole commit before any Redis write',async()=>{
 let writes=0;
 const store=createStore({eval:async()=>{writes++;},hgetall:async()=>({})},{},{prefix:'test'});
 await assert.rejects(store.commit({idempotencyKey:'request-one',operations:[{kind:'technical',op:'set',key:'admin-session/test',value:true,ttl:60},{kind:'job',data:{type:'unknown'}}]}));
 assert.equal(writes,0);
 await assert.rejects(store.commit({idempotencyKey:'request-two',operations:[{kind:'job',data:{type:'email',key:'mail-one',payload:{key:'mail-one'},dependsOn:'a'.repeat(64)}}]}),/DEPENDENCY_NOT_FOUND/);
 assert.equal(writes,0);
});
test('one Redis commit carries ordered Sheets/email intents, TTL state and owner checks',async()=>{
 let recorded;
 const redis={eval:async(...args)=>{recorded=args;return args.at(-1);},hgetall:async()=>({})};
 const store=createStore(redis,{},{prefix:'test'});
 const result=await store.commit({idempotencyKey:'request-group',locks:[{name:'vehicle/V1',owner:'owner1'}],operations:[
  {kind:'sheet',data:sheet,logicalSheet:'VehicleTrips'},
  {kind:'job',data:{type:'email',key:'mail-one',payload:{key:'mail-one'},dependsOn:hash(sheet.key)}},
  {kind:'technical',op:'set',key:'qr-uses/one',value:1,ttl:60}
 ]});
 assert.equal(result.jobs.length,2);assert.equal(result.accepted,true);
 assert.equal(recorded[1],4);
 const actions=JSON.parse(recorded[7]);
 assert.equal(actions.filter(a=>a[0]==='JOB').length,2);
 assert.equal(JSON.parse(actions[1][3]).dependsOn,result.jobs[0].id);
 assert.equal(JSON.parse(actions[0][3]).payload.rowValues[1],4000);
 assert.deepEqual(JSON.parse(recorded[9]),[['test:lock:'+hash('vehicle/V1'),'owner1']]);
});
test('lost ownership and conflicting receipts fail closed',async()=>{
 for(const [result,code]of[['LEASE_LOST','HDOI_LEASE_LOST'],['CONFLICT','IDEMPOTENCY_CONFLICT']]){
  const store=createStore({eval:async()=>result},{},{prefix:'test'});
  await assert.rejects(store.commit({idempotencyKey:'request-one',operations:[]}),error=>error.message===code&&error.status===409);
 }
});
test('identical intents inside one commit coalesce while changed payload conflicts',async()=>{
 let calls=0;const store=createStore({eval:async(...args)=>{calls++;return args.at(-1);}},{},{prefix:'test'});
 const operation={kind:'sheet',data:sheet};
 const result=await store.commit({idempotencyKey:'coalesced',operations:[operation,operation]});
 assert.equal(result.jobs.length,1);assert.equal(calls,1);
 await assert.rejects(store.commit({idempotencyKey:'conflicting',operations:[operation,{...operation,data:{...sheet,payload:{...sheet.payload,rowValues:['V1',5000]}}}]}),/IDEMPOTENCY_CONFLICT/);
 assert.equal(calls,1);
});
