'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const {createCoordination}=require('../src/services/coordination');
const {createGovernor}=require('../src/services/governor');
const {createBackendProcessor}=require('../src/processors/backend');
const {processor}=require('../src/workers');

test('shared admission model reserves foreground headroom without increasing total rolling budget',async()=>{
  // Evaluator model only: argument-policy test, not a real Redis Lua execution.
  const windows=new Map(),calls=[];
  const redis={eval:async(script,n,bucket,_cooldown,limit,id,now,cap)=>{
    calls.push({script,n,bucket,limit,cap});
    const rows=(windows.get(bucket)||[]).filter(row=>row.at>now-60000);
    windows.set(bucket,rows);
    if(rows.length>=cap)return Math.max(1,Math.ceil((rows[0].at+60000-now)/1000));
    rows.push({id,at:now});return 0;
  }};
  const config={prefix:'test',reads:5,writes:5,foregroundReserve:2};
  const a=createCoordination(redis,config),b=createCoordination(redis,config);
  for(const kind of ['read','write']){
    for(let i=0;i<3;i++)assert.equal((await a.execute({op:'quotaReserve',kind,limit:5,priority:'background'})).accepted,true);
    assert.equal((await b.execute({op:'quotaReserve',kind,limit:5,priority:'background'})).accepted,false);
    for(let i=0;i<2;i++)assert.equal((await b.execute({op:'quotaReserve',kind,limit:5,priority:'interactive'})).accepted,true);
    assert.equal((await a.execute({op:'quotaReserve',kind,limit:5})).accepted,false);
    assert.equal(windows.get('test:quota:'+kind).length,5);
  }
  assert.equal(calls[0].cap,3);assert.equal(calls[0].limit,5);
  assert.match(calls[0].script,/ARGV\[4\]/);
  await assert.rejects(a.execute({op:'quotaReserve',kind:'read',limit:5,priority:'bypass'}),{status:400});
});
test('native worker requests background admission and retains local delay signal',async()=>{
  const calls=[];
  const reserve=createGovernor({eval:async(...args)=>{calls.push(args);return 7;}},{prefix:'test',reads:40,writes:40,foregroundReserve:8});
  await assert.rejects(reserve('read'),{code:'SHEETS_LOCAL_RATE_LIMIT',retryAfterMs:7000});
  assert.equal(calls[0].at(-1),32);
});
const config={backendUrl:'https://fixture.invalid',backendToken:'fixture-only-'.repeat(4)};
const job=(type='qr-recovery',durable=true)=>({id:'fixture-job',data:{type,payload:{key:'fixture',durable}},attemptsMade:4,opts:{attempts:5},updateData:async()=>{}});
test('only confirmed durable QR admission deferrals bypass the business retry counter',async()=>{
  const run=createBackendProcessor(config,async()=>Response.json({ok:false,code:'QR_JOB_QUOTA_RETRY',retryable:true},{headers:{'retry-after':'12'}}));
  await assert.rejects(run(job()),{code:'SHEETS_LOCAL_RATE_LIMIT',retryAfterMs:12000});
  await assert.rejects(run(job('qr-recovery',false)),{code:'BACKEND_JOB_FAILED'});
  await assert.rejects(run(job('email')),{code:'BACKEND_JOB_FAILED'});
  const actualRejection=createBackendProcessor(config,async()=>Response.json({ok:false,code:'QR_JOB_RETRY',retryable:true},{headers:{'retry-after':'60'}}));
  await assert.rejects(actualRejection(job()),{code:'BACKEND_JOB_FAILED',retryAfterMs:60000});
});
test('quota waiting delays a job without consuming attempts or modifying its immutable payload',async()=>{
  const j=job(),original=structuredClone(j.data),states=[];let delayed=0;
  j.timestamp=Date.now();j.moveToDelayed=async at=>{delayed=at;};
  const run=processor({store:{ready:async()=>true,status:async(_data,state)=>states.push(state)},execute:async()=>{throw Object.assign(Error('quota'),{code:'SHEETS_LOCAL_RATE_LIMIT',retryAfterMs:1000});}});
  await assert.rejects(run(j,'owner'),error=>error.constructor.name==='DelayedError');
  assert.deepEqual(states,['active','delayed']);assert.ok(delayed>=Date.now()+900);
  assert.equal(j.attemptsMade,4);assert.deepEqual(j.data,original);
});
test('a day-long admission outage becomes visible terminal failure while payload remains intact',async()=>{
  const j=job(),original=structuredClone(j.data),states=[];
  j.timestamp=Date.now()-86400001;j.moveToDelayed=async()=>assert.fail('must not wait indefinitely');
  const run=processor({store:{ready:async()=>true,status:async(_data,state,code)=>states.push({state,code})},execute:async()=>{throw Object.assign(Error('quota'),{code:'SHEETS_LOCAL_RATE_LIMIT'});}});
  await assert.rejects(run(j,'owner'),/SHEETS_ADMISSION_DEADLINE/);
  assert.deepEqual(states.at(-1),{state:'failed',code:'SHEETS_ADMISSION_DEADLINE'});assert.deepEqual(j.data,original);
});
