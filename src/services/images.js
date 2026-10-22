'use strict';
const fs = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const fault = (code, status = 503) => Object.assign(Error(code), {code, status});
const hash = (bytes, algorithm = 'sha256') => crypto.createHash(algorithm).update(bytes).digest('hex');
const MAX = 5 * 1024 * 1024;
async function atomic(file, bytes) {
  const tmp = file + '.' + crypto.randomUUID() + '.tmp';
  const handle = await fs.open(tmp, 'wx', 0o600);
  try { await handle.writeFile(bytes); await handle.sync(); } finally { await handle.close(); }
  await fs.rename(tmp, file);
  // Directory fsync commits rename on Linux; Windows does not support this operation.
  try { const dir = await fs.open(path.dirname(file), 'r'); try { await dir.sync(); } finally { await dir.close(); } }
  catch (error) { if (process.platform !== 'win32') throw error; }
}
function imageMime(bytes) {
  if (bytes.length >= 3 && bytes.subarray(0, 3).equals(Buffer.from([255,216,255]))) return 'image/jpeg';
  if (bytes.length >= 8 && bytes.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10]))) return 'image/png';
  if (bytes.length >= 12 && bytes.toString('ascii',0,4)==='RIFF' && bytes.toString('ascii',8,12)==='WEBP') return 'image/webp';
  throw fault('INVALID_IMAGE_CONTENT', 400);
}
function publicImage(m) {
  return {namespace:m.namespace,imageId:m.imageId,state:m.state,source:m.state==='COMPLETED'?'drive':'hdoi',
    fileId:m.state==='COMPLETED'?m.driveFileId:undefined,mime:m.mime,size:m.size,checksum:m.checksum,
    version:m.version,verifiedAt:m.verifiedAt,received:Object.keys(m.chunks || {}).length,total:m.total,
    attempts:m.attempts || 0,nextAttemptAt:m.nextAttemptAt,lastError:m.lastError,syncState:m.syncState};
}
function createImages({config, queues = {}, clock = Date.now, drive, sync, logger = console}) {
  const root = path.resolve(config.imageStorageDir || '/data/images');
  const validate = (namespace,id) => {
    if(namespace!==config.namespace) throw fault('NAMESPACE_MISMATCH',403);
    if(typeof id!=='string'||!/^[a-f0-9]{24,64}$/.test(id)) throw fault('INVALID_IMAGE_ID',400);
    return path.join(root,namespace,id);
  };
  const read = async dir => { try { return JSON.parse(await fs.readFile(path.join(dir,'manifest.json'),'utf8')); } catch(e) { if(e.code==='ENOENT') return null; throw e; } };
  const write = (dir,m) => atomic(path.join(dir,'manifest.json'),JSON.stringify(m));
  async function locked(namespace,id,fn) {
    const dir=validate(namespace,id); await fs.mkdir(dir,{recursive:true,mode:0o700});
    const lock=path.join(dir,'journal.lock'); const until=Date.now()+10000;
    let handle;
    while(!handle) {
      try { handle=await fs.open(lock,'wx',0o600); }
      catch(e) {
        if(e.code!=='EEXIST') throw e;
        const stat=await fs.stat(lock).catch(()=>null);
        if(stat && Date.now()-stat.mtimeMs>120000) { await fs.unlink(lock).catch(()=>{}); continue; }
        if(Date.now()>until) throw fault('IMAGE_BUSY',409);
        await new Promise(r=>setTimeout(r,25));
      }
    }
    try { return await fn(dir,await read(dir)); } finally { await handle.close(); await fs.unlink(lock).catch(()=>{}); }
  }
  async function enqueue(m,kind='images') {
    const queue=queues[kind]; if(!queue) return;
    const id=m.imageId+'-'+(kind==='imageMetadata'?m.version+'-'+(m.syncRevision||0):m.attempts||0);
    const job=await queue.getJob(id);
    if(job && ['active','waiting','delayed','prioritized','waiting-children'].includes(await job.getState())) return;
    if(job) await job.remove();
    const nextAttemptAt=(kind==='imageMetadata'?m.syncNextAttemptAt:m.nextAttemptAt)||0;
    await queue.add(kind,{namespace:m.namespace,imageId:m.imageId},{jobId:id,delay:Math.max(0,nextAttemptAt-clock()),attempts:1,removeOnComplete:1000,removeOnFail:1000});
  }
  async function uploadChunk(namespace,id,input,index) {
    validate(namespace,id);
    if(input.namespace!==namespace||input.imageId!==id||input.index!==index || !Number.isInteger(index)||index<0 || !Number.isInteger(input.total)||input.total<1||input.total>128||index>=input.total || typeof input.uploadKey!=='string'||input.uploadKey.length<8||input.uploadKey.length>256 || typeof input.field!=='string'||!input.field||input.field.length>80 || !Number.isInteger(input.slot)||input.slot<0||input.slot>100 || typeof input.chunk!=='string'||input.chunk.length>1048576|| !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(input.chunk)) throw fault('INVALID_IMAGE_CHUNK',400);
    const bytes=Buffer.from(input.chunk,'base64'); if(!bytes.length) throw fault('INVALID_IMAGE_CHUNK',400);
    const result=await locked(namespace,id,async(dir,existing)=>{
      const m=existing||{namespace,imageId:id,uploadKey:input.uploadKey,field:input.field,slot:input.slot,total:input.total,state:'RECEIVING',version:1,createdAt:clock(),updatedAt:clock(),chunks:{},attempts:0};
      if(m.uploadKey!==input.uploadKey||m.field!==input.field||m.slot!==input.slot||m.total!==input.total) throw fault('IMAGE_CONTENT_CONFLICT',409);
      const digest=hash(bytes);
      if(m.chunks[index] && m.chunks[index].checksum!==digest) throw fault('IMAGE_CONTENT_CONFLICT',409);
      if(m.chunks[index]) return m;
      if(m.state!=='RECEIVING') throw fault('IMAGE_CONTENT_CONFLICT',409);
      if(Object.values(m.chunks).reduce((n,c)=>n+c.size,0)+bytes.length>MAX) throw fault('IMAGE_TOO_LARGE',413);
      await atomic(path.join(dir,'chunk-'+index),bytes);
      m.chunks[index]={checksum:digest,size:bytes.length}; m.updatedAt=clock();
      if(Object.keys(m.chunks).length===m.total) {
        const chunks=[]; for(let i=0;i<m.total;i++) chunks.push(await fs.readFile(path.join(dir,'chunk-'+i)));
        const image=Buffer.concat(chunks); m.mime=imageMime(image);m.size=image.length;m.checksum=hash(image);m.md5=hash(image,'md5');
        await atomic(path.join(dir,'image'),image);m.state='HOLD';m.holdAt=clock();m.enqueueIntent=true;m.version++;
      }
      await write(dir,m); return m;
    });
    if(result.enqueueIntent) await enqueue(result).catch(()=>{});
    return publicImage(result);
  }
  async function get(namespace,id) { const m=await read(validate(namespace,id));if(!m)throw fault('IMAGE_NOT_FOUND',404);return publicImage(m); }
  async function content(namespace,id) {
    const dir=validate(namespace,id),m=await read(dir);if(!m||m.state==='RECEIVING')throw fault('IMAGE_NOT_READY',404);
    if(m.state==='COMPLETED')return {image:publicImage(m)};
    try { return {bytes:await fs.readFile(path.join(dir,'image')),mime:m.mime}; }
    catch(e) { if(e.code!=='ENOENT')throw e;const current=await read(dir);if(current?.state==='COMPLETED')return {image:publicImage(current)};throw fault('IMAGE_NOT_AVAILABLE'); }
  }
  const retryDelay = (attempt,error) => {
    const header=error?.response?.headers?.get?.('retry-after')??error?.response?.headers?.['retry-after'];
    const retry=Number(header),after=header==null?0:Number.isFinite(retry)?retry*1000:Date.parse(header)-clock();
    return Math.max(Number(error.retryAfterMs)||0,after||0,Math.min(300000,1000*2**Math.min(attempt,8))+Math.floor(Math.random()*500));
  };
  async function processUpload({namespace,imageId}) {
    const owner=crypto.randomUUID();
    const claimed=await locked(namespace,imageId,async(dir,m)=>{
      if(!m||m.state==='RECEIVING'||m.state==='COMPLETED'||m.state==='FAILED'||m.nextAttemptAt>clock()||m.leaseUntil>clock())return null;
      m.state='UPLOADING';m.leaseOwner=owner;m.leaseUntil=clock()+180000;m.attempts++;m.updatedAt=clock();await write(dir,m);return m;
    });
    if(!claimed)return;
    let lost=false;
    const renew=setInterval(()=>locked(namespace,imageId,async(dir,m)=>{if(m?.leaseOwner!==owner){lost=true;return;}m.leaseUntil=clock()+180000;await write(dir,m);}).catch(()=>{lost=true;}),30000);renew.unref?.();
    try {
      if(!drive)throw fault('IMAGE_DRIVE_NOT_CONFIGURED');
      let fileId=claimed.driveFileId;
      if(!fileId) {
        fileId=await drive.allocate();
        await locked(namespace,imageId,async(dir,m)=>{if(m.leaseOwner!==owner)throw fault('IMAGE_LEASE_LOST');m.driveFileId=fileId;await write(dir,m);});
      }
      const bytes=await fs.readFile(path.join(validate(namespace,imageId),'image'));
      await drive.upload({fileId,bytes,mime:claimed.mime,namespace,imageId});
      await drive.verify({fileId,mime:claimed.mime,size:claimed.size,md5:claimed.md5,checksum:claimed.checksum});
      if(lost)throw fault('IMAGE_LEASE_LOST');
      const completed=await locked(namespace,imageId,async(dir,m)=>{
        if(m.leaseOwner!==owner)throw fault('IMAGE_LEASE_LOST');
        m.state='COMPLETED';m.verifiedAt=new Date(clock()).toISOString();m.updatedAt=clock();m.version++;m.enqueueIntent=false;m.syncState='PENDING';m.syncIntent=true;m.leaseUntil=0;delete m.leaseOwner;
        await write(dir,m);return m;
      });
      await cleanup(completed);await enqueue(completed,'imageMetadata').catch(()=>{});
    } catch(error) {
      await locked(namespace,imageId,async(dir,m)=>{
        if(m?.leaseOwner!==owner||m.state==='COMPLETED')return;
        m.lastError=/^[A-Z][A-Z0-9_]{1,80}$/.test(error.code||'')?error.code:'IMAGE_DRIVE_FAILED';
        m.state=m.attempts>=10||clock()-m.holdAt>config.imageAlertAfterMs?'FAILED':'RETRY';
        m.nextAttemptAt=clock()+retryDelay(m.attempts,error);m.leaseUntil=0;delete m.leaseOwner;m.updatedAt=clock();await write(dir,m);
        if(m.state==='FAILED')logger.error(JSON.stringify({event:'image-upload-failed',imageId,code:m.lastError}));
      });
    } finally {clearInterval(renew);}
  }
  async function cleanup(m) {
    if(m.state!=='COMPLETED')return;
    const dir=validate(m.namespace,m.imageId);
    for(const file of ['image',...Object.keys(m.chunks||{}).map(i=>'chunk-'+i)]) await fs.unlink(path.join(dir,file)).catch(e=>{if(e.code!=='ENOENT')logger.error(JSON.stringify({event:'image-cleanup-pending',imageId:m.imageId}));});
  }
  async function processMetadata({namespace,imageId}) {
    // Idempotent receiver accepts repeated versions; image transfer never depends on metadata availability.
    const dir=validate(namespace,imageId),m=await read(dir);if(!m||m.state!=='COMPLETED'||!m.syncIntent||m.syncNextAttemptAt>clock())return;
    try {
      if(!sync)throw fault('IMAGE_METADATA_NOT_CONFIGURED');
      await sync({namespace,imageId,version:m.version,state:m.state,fileId:m.driveFileId,mime:m.mime,size:m.size,checksum:m.checksum,verifiedAt:m.verifiedAt,updatedAt:new Date(m.updatedAt).toISOString()});
      for(const link of m.links||[]) await sync.link({namespace,imageId,...link});
      await locked(namespace,imageId,async(d,current)=>{if(current.version!==m.version||current.syncRevision!==m.syncRevision)return;current.syncIntent=false;current.syncState='SYNCED';await write(d,current);});
    } catch(error) {await locked(namespace,imageId,async(d,current)=>{current.syncAttempts=(current.syncAttempts||0)+1;current.syncNextAttemptAt=clock()+retryDelay(current.syncAttempts,error);current.syncState='RETRY';await write(d,current);});}
  }
  async function links(namespace,id,items) {
    if(!Array.isArray(items)||items.length>50||items.some(i=>!i||typeof i.entity!=='string'||!i.entity||i.entity.length>100||typeof i.recordId!=='string'||!i.recordId||i.recordId.length>200||typeof i.field!=='string'||!i.field||i.field.length>100||!Number.isInteger(i.slot)||i.slot<0||i.slot>99||!Number.isSafeInteger(i.version)||i.version<1))throw fault('INVALID_IMAGE_LINKS',400);
    const result=await locked(namespace,id,async(dir,m)=>{
      if(!m||m.state==='RECEIVING')throw fault('IMAGE_NOT_READY',409);
      const existing=new Map((m.links||[]).map(l=>[JSON.stringify([l.entity,l.recordId,l.field,l.slot]),l]));
      for(const i of items){const key=JSON.stringify([i.entity,i.recordId,i.field,i.slot]);if((existing.get(key)?.version||0)<i.version)existing.set(key,{entity:i.entity,recordId:i.recordId,field:i.field,slot:i.slot,version:i.version,updatedAt:typeof i.updatedAt==='string'&&Number.isFinite(Date.parse(i.updatedAt))?new Date(i.updatedAt).toISOString():new Date(clock()).toISOString()});}
      m.links=Array.from(existing.values());m.syncIntent=true;m.syncState='PENDING';m.syncRevision=(m.syncRevision||0)+1;await write(dir,m);return m;
    });
    if(result.state==='COMPLETED')await enqueue(result,'imageMetadata').catch(()=>{});return publicImage(result);
  }
  async function retry(namespace,id) {
    const m=await locked(namespace,id,async(dir,m)=>{if(!m)throw fault('IMAGE_NOT_FOUND',404);if(m.state==='FAILED'){m.state='RETRY';m.attempts=0;m.holdAt=clock();m.nextAttemptAt=clock();await write(dir,m);}return m;});
    if(m.state==='COMPLETED')await enqueue(m,'imageMetadata');else if(m.state!=='RECEIVING')await enqueue(m);return publicImage(m);
  }
  async function sweep() {
    const ns=path.join(root,config.namespace);await fs.mkdir(ns,{recursive:true,mode:0o700});
    for(const id of await fs.readdir(ns)) {
      if(!/^[a-f0-9]{24,64}$/.test(id))continue;
      try {
        let m=await read(validate(config.namespace,id));if(!m)continue;
        if(m.state==='COMPLETED'){await cleanup(m);if(m.syncIntent&&!(m.syncNextAttemptAt>clock()))await enqueue(m,'imageMetadata');continue;}
        if(m.state==='RECEIVING'){
          if(clock()-m.updatedAt>86400000)await locked(config.namespace,id,async(dir,current)=>{
            if(current.state!=='RECEIVING'||clock()-current.updatedAt<=86400000)return;
            // No complete business reference has been acknowledged for these partial uploads.
            for(const i of Object.keys(current.chunks||{}))await fs.unlink(path.join(dir,'chunk-'+i)).catch(e=>{if(e.code!=='ENOENT')throw e;});
            current.chunks={};current.updatedAt=clock();await write(dir,current);
          });
          continue;
        }
        if(m.state==='FAILED'||m.leaseUntil>clock()||m.nextAttemptAt>clock())continue;
        if(m.state==='UPLOADING') m=await locked(config.namespace,id,async(dir,current)=>{if(current.leaseUntil>clock())return current;current.state='RETRY';current.leaseUntil=0;delete current.leaseOwner;await write(dir,current);return current;});
        await enqueue(m);
      }catch(error){logger.error(JSON.stringify({event:'image-recovery-pending',imageId:id,code:error.code==='ENOSPC'?'ENOSPC':'IMAGE_RECOVERY_FAILED'}));}
    }
  }
  return {uploadChunk,get,content,retry,links,sweep,processUpload,processMetadata,root};
}
module.exports={createImages,publicImage,imageMime,atomic};
