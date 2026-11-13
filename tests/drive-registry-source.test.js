'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs/promises'),os=require('node:os'),path=require('node:path');
const {createCredentialStore}=require('../src/services/driveCredentials');
const {createImageDrive}=require('../src/services/imageDrive');
test('HDoi migrates cached environment token to registry and keeps archived configuration',async t=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'hdoi-registry-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));
 const env={DRIVE_CREDENTIAL_ENCRYPTION_KEY:'a'.repeat(64),DRIVE_CLIENT_ID:'client',DRIVE_CLIENT_SECRET:'secret',DRIVE_REFRESH_TOKEN:'expired',DRIVE_FOLDER_ID:'folder'};
 await createCredentialStore({root,env}).acquire();
 Object.assign(env,{DATA_DONG_BO_URL:'https://broker.invalid',DATA_DONG_BO_TOKEN:'test'});
 let calls=0;
 const store=createCredentialStore({root,env,fetchImpl:async()=>{calls++;return {ok:true,json:async()=>({ok:true,credential:{id:'registry-token',label:'Token',refreshToken:'fresh',revision:1},version:1,issuedAt:'2026-01-01T00:00:00Z'})};}});
 const c=await store.acquire();assert.equal(c.refreshToken,'fresh');assert.equal(c.clientId,'client');
 assert.equal((await store.get('hdoi','environment')).refreshToken,'expired');
 assert.equal((await store.acquire()).id,'registry-token');assert.equal(calls,1);
});
test('allocation switches registry token on invalid_grant before any file is created',async()=>{
 let current={id:'old',clientId:'client',clientSecret:'secret',refreshToken:'expired'},rotations=[];
 const credentials={acquire:async request=>{if(request.reason){rotations.push(request);current={...current,id:'next',refreshToken:'fresh'};}return current;},userQuota:()=>false};
 class OAuth2 {setCredentials(c){this.token=c.refresh_token;}}
 const google={auth:{OAuth2},drive:({auth})=>({files:{generateIds:async()=>{if(auth.token==='expired')throw {response:{status:400,data:{error:'invalid_grant'}}};return {data:{ids:['file-id']}};}}})};
 const drive=await createImageDrive({DRIVE_CREDENTIAL_ENCRYPTION_KEY:'a'.repeat(64),DATA_DONG_BO_URL:'https://broker.invalid'},{google},credentials);
 assert.deepEqual(await drive.allocate(),{fileId:'file-id',credentialId:'next'});
 assert.equal(rotations[0].reason,'invalid_grant');assert.equal(rotations[0].currentId,'old');
});
