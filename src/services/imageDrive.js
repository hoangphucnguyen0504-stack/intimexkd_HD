'use strict';
const {Readable}=require('node:stream');
const crypto=require('node:crypto');
async function createImageDrive(env=process.env,googleapis=require('googleapis')) {
  const {google}=googleapis;
  let auth;
  const clientId=env.DRIVE_CLIENT_ID||env.GOOGLE_OAUTH_CLIENT_ID,clientSecret=env.DRIVE_CLIENT_SECRET||env.GOOGLE_OAUTH_CLIENT_SECRET,refreshToken=env.DRIVE_REFRESH_TOKEN||env.GOOGLE_OAUTH_REFRESH_TOKEN;
  if(clientId&&clientSecret&&refreshToken){
    auth=new google.auth.OAuth2(clientId,clientSecret);
    auth.setCredentials({refresh_token:refreshToken});
  } else auth=new google.auth.GoogleAuth({...(env.GOOGLE_SERVICE_ACCOUNT_JSON?{credentials:JSON.parse(env.GOOGLE_SERVICE_ACCOUNT_JSON)}:{keyFile:env.GOOGLE_APPLICATION_CREDENTIALS}),scopes:['https://www.googleapis.com/auth/drive']});
  const drive=google.drive({version:'v3',auth});
  const options=()=>({timeout:60000,retry:false,retryConfig:{retry:0}});
  const info=async id=>(await drive.files.get({fileId:id,fields:'id,size,mimeType,md5Checksum,trashed',supportsAllDrives:true},options())).data;
  return {
    async allocate(){return (await drive.files.generateIds({count:1,space:'drive'},options())).data.ids[0];},
    async upload({fileId,bytes,mime,namespace,imageId}) {
      let existing;
      try {existing=await info(fileId);}catch(e){if(Number(e.response?.status||e.code)!==404)throw e;}
      if(existing)return; // Retry after ambiguous success verifies the exact preallocated ID below.
      if(!env.DRIVE_FOLDER_ID)throw Object.assign(Error('IMAGE_DRIVE_FOLDER_REQUIRED'),{code:'IMAGE_DRIVE_FOLDER_REQUIRED'});
      try {
        await drive.files.create({requestBody:{id:fileId,name:namespace+'-'+imageId,parents:[env.DRIVE_FOLDER_ID]},media:{mimeType:mime,body:Readable.from(bytes)},supportsAllDrives:true,fields:'id'},options());
      }catch(e){if(Number(e.response?.status||e.code)!==409)throw e;}
    },
    async verify({fileId,mime,size,md5,checksum}) {
      const m=await info(fileId);
      if(m.id!==fileId||m.trashed||Number(m.size)!==size||m.mimeType!==mime||m.md5Checksum!==md5)throw Object.assign(Error('IMAGE_DRIVE_METADATA_MISMATCH'),{code:'IMAGE_DRIVE_METADATA_MISMATCH'});
      const result=await drive.files.get({fileId,alt:'media',supportsAllDrives:true},{...options(),responseType:'stream'});
      const digest=crypto.createHash('sha256');let received=0;
      for await(const chunk of result.data){received+=chunk.length;if(received>size){result.data.destroy();throw Object.assign(Error('IMAGE_DRIVE_CONTENT_MISMATCH'),{code:'IMAGE_DRIVE_CONTENT_MISMATCH'});}digest.update(chunk);}
      if(received!==size||digest.digest('hex')!==checksum)throw Object.assign(Error('IMAGE_DRIVE_CONTENT_MISMATCH'),{code:'IMAGE_DRIVE_CONTENT_MISMATCH'});
    }
  };
}
function createImageSync(env=process.env) {
  const send=async(endpoint,payload)=>{
    if(!env.DATA_DONG_BO_URL||!env.DATA_DONG_BO_TOKEN)throw Error('IMAGE_METADATA_NOT_CONFIGURED');
    const response=await fetch(env.DATA_DONG_BO_URL.replace(/\/$/,'')+'/v1/cham-cong/images/'+endpoint,{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer '+env.DATA_DONG_BO_TOKEN},body:JSON.stringify(payload),signal:AbortSignal.timeout(20000)});
    if(!response.ok)throw Object.assign(Error('IMAGE_METADATA_SYNC_FAILED'),{retryAfterMs:Number(response.headers.get('retry-after'))*1000});
  };
  const sync=({fileId,...rest})=>send('upsert',{...rest,driveFileId:fileId});
  sync.link=payload=>send('link',payload);
  return sync;
}
module.exports={createImageDrive,createImageSync};
