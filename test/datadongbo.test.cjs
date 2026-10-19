const {test}=require('node:test'),assert=require('node:assert/strict');
const {createDataDongBoProcessor}=require('../src/processors/datadongbo');
const env={DATA_DONG_BO_URL:'https://fixture.invalid',DATA_DONG_BO_TOKEN:'fixture-token-'.repeat(4)};
const job=()=>({data:{type:'sheet-write',key:'fixed-job-key',payload:{storageProvider:'datadongbo',entity:'VehicleTrips',sheet:'VehicleTrips',spreadsheetId:'fixture-sheet-1234567890',headers:['ID'],recordId:'trip-1',op:'append',rowValues:['trip-1']}},updateData:async()=>{}});
test('HDoi confirms MySQL delivery and never calls Google',async()=>{
 const calls=[],execute=createDataDongBoProcessor({env,fetch:async(url,options)=>{calls.push({url,options});return new Response(JSON.stringify({ok:true,durable:true}));}});
 const j=job(),one=await execute(j);await execute(j);assert.equal(one.target,'datadongbo');assert.equal(one.sheetsStatus,'pending');assert.equal(calls.length,2);
 assert.equal(JSON.parse(calls[0].options.body).requestId,JSON.parse(calls[1].options.body).requestId);assert(calls.every(c=>c.url.startsWith(env.DATA_DONG_BO_URL)));
});
test('HDoi recovers lost ACK by durable receipt lookup',async()=>{
 const calls=[],execute=createDataDongBoProcessor({env,fetch:async(url)=>{calls.push(url);if(url.endsWith('/write'))throw Error('offline');return new Response(JSON.stringify({ok:true,durable:true}));}});
 assert.equal((await execute(job())).status,'confirmed');assert.equal(calls.length,2);assert(calls[1].includes('/v1/requests/'));
});
test('HDoi holds unbound old jobs instead of writing either database',async()=>{
 let calls=0;const execute=createDataDongBoProcessor({env,fetch:async()=>{calls++;}}),j=job();delete j.data.payload.storageProvider;
 await assert.rejects(()=>execute(j),e=>e.code==='STORAGE_PROVIDER_MISMATCH'&&e.unknown);assert.equal(calls,0);
});
test('HDoi cannot confirm a response without durable database acknowledgement',async()=>{
 const execute=createDataDongBoProcessor({env,fetch:async()=>new Response(JSON.stringify({ok:true}))});await assert.rejects(()=>execute(job()),{code:'DATA_DONG_BO_NOT_DURABLE'});
});
