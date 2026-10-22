'use strict';
const {createHash}=require('node:crypto');
const fault=(code,extra={})=>Object.assign(Error(code),{code,...extra});
function createDataDongBoProcessor({env=process.env,fetch:fetchImpl=globalThis.fetch}={}){
  let url;try{url=new URL(env.DATA_DONG_BO_URL);if(url.pathname!=='/'||url.username||url.password||url.search||url.hash||
    (url.protocol!=='https:'&&!(url.protocol==='http:'&&['localhost','127.0.0.1','[::1]'].includes(url.hostname))))throw Error();}
  catch{throw fault('DATA_DONG_BO_CONFIG');}
  if(!env.DATA_DONG_BO_TOKEN||env.DATA_DONG_BO_TOKEN.length<32||env.DATA_DONG_BO_TOKEN.trim()!==env.DATA_DONG_BO_TOKEN)throw fault('DATA_DONG_BO_CONFIG');
  async function call(path,body){
    let response;try{response=await fetchImpl(url.origin+path,{method:body?'POST':'GET',redirect:'error',signal:AbortSignal.timeout(15000),
      headers:{Authorization:'Bearer '+env.DATA_DONG_BO_TOKEN,'X-DataDongBo-Client':'hdoi-worker',...(body?{'Content-Type':'application/json'}:{})},body:body?JSON.stringify(body):undefined});}
    catch{throw fault('DATA_DONG_BO_NETWORK',{retryable:true});}
    let result;try{const parts=[];let bytes=0;for await(const chunk of response.body){bytes+=chunk.length;if(bytes>262144)throw Error();parts.push(Buffer.from(chunk));}result=JSON.parse(Buffer.concat(parts).toString());}catch{throw fault('DATA_DONG_BO_RESPONSE',{retryable:true});}
    if(!response.ok||result.ok!==true)throw fault('DATA_DONG_BO_'+(/^[A-Z][A-Z0-9_]{0,59}$/.test(result.code||'')?result.code:'HTTP_'+response.status),
      {status:response.status,retryable:response.status>=500||response.status===429,retryAfterMs:Math.max(0,Number(response.headers.get('retry-after'))||0)*1000});
    return result;
  }
  const run=async job=>{
    const data=job.data,p=data?.payload;
    if(data?.type!=='sheet-write'||p?.storageProvider!=='datadongbo')throw fault('STORAGE_PROVIDER_MISMATCH',{unknown:true});
    if(typeof data.key!=='string'||!data.key||!p.entity||!Array.isArray(p.headers)||p.headers[0]!=='ID'||new Set(p.headers).size!==p.headers.length||!Array.isArray(p.rowValues)||p.rowValues.length>p.headers.length||!['append','update','delete'].includes(p.op)||!p.recordId||(p.op!=='delete'&&String(p.rowValues[0])!==String(p.recordId)))throw fault('DATA_DONG_BO_INVALID_JOB');
    const requestId='hdoi-'+createHash('sha256').update(data.key).digest('hex');
    const body={requestId,spreadsheetId:p.spreadsheetId,entity:p.entity,headers:p.headers,changes:[{op:p.op,recordId:String(p.recordId),rowValues:p.rowValues}],guards:[]};
    let receipt;
    try{receipt=await call('/v1/cham-cong/write',body);}
    catch(error){
      if(error.retryable){try{receipt=await call('/v1/requests/'+requestId);}catch{}}
      if(!receipt)throw error;
    }
    if(receipt.durable!==true)throw fault('DATA_DONG_BO_NOT_DURABLE',{retryable:true});
    const next={...job.data,effect:{status:'confirmed',target:'datadongbo',requestId,at:new Date().toISOString()}};
    await job.updateData(next);job.data=next;
    return {status:'confirmed',target:'datadongbo',sheetsStatus:'pending'};
  };
  // Sheet jobs no longer run for ordinary writes, so the worker announces itself on a timer.
  run.ping=()=>call('/v1/cham-cong/ping');
  return run;
}
module.exports={createDataDongBoProcessor};
