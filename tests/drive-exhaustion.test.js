'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs/promises'),path=require('node:path'),os=require('node:os');
const {createCredentialStore}=require('../src/services/driveCredentials');
test('exhaustion blocks cached active token and resumes with same pending request after adding backup',async t=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'exhausted-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));
 const env={DRIVE_CREDENTIAL_ENCRYPTION_KEY:'a'.repeat(64),DATA_DONG_BO_URL:'https://broker.invalid',DATA_DONG_BO_TOKEN:'test'},requests=[];
 let mode='old';
 const make=()=>createCredentialStore({root,env,fetchImpl:async(_url,options)=>{
  requests.push(JSON.parse(options.body));
  return mode==='exhausted'?{ok:false,json:async()=>({code:'DRIVE_CREDENTIALS_EXHAUSTED'})}:{ok:true,json:async()=>({ok:true,credential:{id:mode,label:mode,clientId:'client',clientSecret:'secret',refreshToken:'refresh-'+mode,folderId:'folder',revision:1},issuedAt:'2026-01-01T00:00:00Z',version:1})};
 }});
 await make().acquire();mode='exhausted';
 await assert.rejects(make().acquire({currentId:'old',reason:'invalid_grant'}),/DRIVE_CREDENTIALS_EXHAUSTED/);
 await assert.rejects(make().acquire(),/DRIVE_CREDENTIALS_EXHAUSTED/);
 mode='new';assert.equal((await make().acquire()).id,'new');
 assert.equal(requests[1].requestId,requests[2].requestId);assert.equal(requests[2].requestId,requests[3].requestId);
 assert.equal((await make().get('hdoi','old')).refreshToken,'refresh-old');
});
