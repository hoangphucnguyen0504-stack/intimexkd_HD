'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs/promises'),os=require('node:os'),path=require('node:path');
const {createCredentialStore,userQuota}=require('../src/services/driveCredentials');
test('encrypted durable acquisition single flight, restart, stale rotation and archived ownership',async()=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'drive-credentials-'));let calls=0;
 const env={DRIVE_CREDENTIAL_ENCRYPTION_KEY:'a'.repeat(64),DATA_DONG_BO_URL:'https://broker.invalid',DATA_DONG_BO_TOKEN:'test-token'};
 const fetchImpl=async()=>{calls++;return {ok:true,json:async()=>({ok:true,credential:{id:'account-'+calls,label:'Account',clientId:'client',clientSecret:'secret',refreshToken:'refresh-sensitive',folderId:'folder',revision:1},version:1,issuedAt:'2026-01-01T00:00:00Z'})};};
 const make=()=>createCredentialStore({env,root,fetchImpl});
 try{
  const [a,b]=await Promise.all([make().acquire(),make().acquire()]);assert.equal(a.id,b.id);assert.equal(calls,1);
  assert.equal((await make().acquire()).id,a.id);assert.equal(calls,1);
  const next=await make().acquire({currentId:a.id,reason:'userRateLimitExceeded'});assert.equal(calls,2);
  assert.equal((await make().acquire({currentId:a.id,reason:'userRateLimitExceeded'})).id,next.id);assert.equal(calls,2);
  await make().owner('hdoi','file_123456789',a.id);assert.equal(await make().owner('hdoi','file_123456789'),a.id);assert.equal((await make().get('hdoi',a.id)).refreshToken,'refresh-sensitive');
  await assert.rejects(make().owner('hdoi','file_123456789',next.id),/DRIVE_OWNER_CONFLICT/);
  assert.equal((await fs.readFile(path.join(root,'hdoi.json'),'utf8')).includes('refresh-sensitive'),false);
 }finally{await fs.rm(root,{recursive:true,force:true});}
});
test('only explicit user quota is eligible; generic 429, project, network and permissions never rotate',()=>{
 const error=(status,reasons)=>({response:{status,data:{error:{errors:reasons.map(reason=>({reason}))}}}});
 assert.equal(userQuota(error(403,['userRateLimitExceeded'])),true);
 for(const e of [error(429,['userRateLimitExceeded']),error(403,['rateLimitExceeded']),error(403,['userRateLimitExceeded','rateLimitExceeded']),error(403,['insufficientPermissions']),{code:'ECONNRESET'},error(401,[])])assert.equal(userQuota(e),false);
});
test('ambiguous broker response keeps stable durable requestId through restart',async()=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'drive-retry-')),requests=[];
 const env={DRIVE_CREDENTIAL_ENCRYPTION_KEY:'b'.repeat(64),DATA_DONG_BO_URL:'https://broker.invalid',DATA_DONG_BO_TOKEN:'test'};
 const fetchImpl=async(_url,options)=>{requests.push(JSON.parse(options.body));throw Error('network');};
 try{for(let i=0;i<2;i++)await assert.rejects(createCredentialStore({root,env,fetchImpl}).acquire(),/DRIVE_REGISTRY_UNAVAILABLE/);assert.equal(requests[0].requestId,requests[1].requestId);}finally{await fs.rm(root,{recursive:true,force:true});}
});
test('weak keys rejected and Cham archive cannot borrow HDoi environment',async()=>{
 assert.throws(()=>createCredentialStore({env:{DRIVE_CREDENTIAL_ENCRYPTION_KEY:'x'}}),/KEY_INVALID/);
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'drive-isolation-'));
 try{const store=createCredentialStore({root,env:{DRIVE_CREDENTIAL_ENCRYPTION_KEY:'c'.repeat(64),DRIVE_CLIENT_ID:'hdoi-client'}});await assert.rejects(store.get('chamcong','environment'),{status:404});assert.equal((await store.get('hdoi','environment')).clientId,'hdoi-client');}finally{await fs.rm(root,{recursive:true,force:true});}
});
test('reserved IDs rejected before archive access, fallback persistence or broker assignment',async()=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'drive-reserved-'));const env={DRIVE_CREDENTIAL_ENCRYPTION_KEY:'e'.repeat(64),DATA_DONG_BO_TOKEN:'token',DATA_DONG_BO_URL:'https://broker.invalid'};
 try{
  for(const id of ['__proto__','constructor','prototype','toString','hasOwnProperty','__defineGetter__']){
   const store=createCredentialStore({root,env,fetchImpl:async()=>new Response(JSON.stringify({ok:true,credential:{id,label:'Account',clientId:'client',clientSecret:'secret',refreshToken:'refresh',folderId:'folder',revision:1},issuedAt:'2026-01-01T00:00:00Z',version:1}))});
   await assert.rejects(store.get('hdoi',id),/ID_INVALID/);
   await assert.rejects(store.acquire({currentId:id,reason:'userRateLimitExceeded'}),/ID_INVALID/);
   await assert.rejects(store.acquire({fallbackCredential:{id,clientId:'c',clientSecret:'s',refreshToken:'r',folderId:'f'}}),/FALLBACK_INVALID/);
   await assert.rejects(store.acquire(),/RESPONSE_INVALID/);
  }
  await assert.rejects(createCredentialStore({root,env}).get('hdoi','constructor'),/ID_INVALID/);
 }finally{await fs.rm(root,{recursive:true,force:true});}
});
test('broker transport rejects insecure hosts, oversized replies and changed archived identity',async()=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'drive-transport-'));const env={DRIVE_CREDENTIAL_ENCRYPTION_KEY:'d'.repeat(64),DATA_DONG_BO_TOKEN:'token',DATA_DONG_BO_URL:'http://remote.invalid'};let calls=0;
 try{
  await assert.rejects(createCredentialStore({root,env,fetchImpl:async()=>{calls++;}}).acquire(),/URL_INVALID/);assert.equal(calls,0);
  env.DATA_DONG_BO_URL='https://broker.invalid';
  await assert.rejects(createCredentialStore({root,env,fetchImpl:async(_u,o)=>{assert.equal(o.redirect,'error');return new Response('x'.repeat(32769));}}).acquire(),/RESPONSE_INVALID/);
  const reply=token=>new Response(JSON.stringify({ok:true,credential:{id:'same-account',label:'Account',clientId:'client',clientSecret:'secret',refreshToken:token,folderId:'folder',revision:1},version:1,issuedAt:'2026-01-01T00:00:00Z'}));
  await createCredentialStore({root,env,fetchImpl:async()=>reply('original')}).acquire();
  await assert.rejects(createCredentialStore({root,env,fetchImpl:async()=>reply('changed')}).acquire({currentId:'same-account',reason:'userRateLimitExceeded'}),/IDENTITY_CHANGED/);
  assert.equal((await createCredentialStore({root,env}).get('hdoi','same-account')).refreshToken,'original');
 }finally{await fs.rm(root,{recursive:true,force:true});}
});
