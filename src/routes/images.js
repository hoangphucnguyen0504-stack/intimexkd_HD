'use strict';
const express=require('express');
module.exports=function imageRoutes(images,config){
  const router=express.Router();
  const route=fn=>(req,res,next)=>Promise.resolve(fn(req,res)).catch(next);
  const namespace=req=>req.body?.namespace||req.query.namespace;
  router.get('/capabilities',route(async(_req,res)=>{
    await require('node:fs/promises').mkdir(images.root,{recursive:true,mode:0o700});
    await require('node:fs/promises').access(images.root,require('node:fs').constants.W_OK);
    res.json({ok:true,images:{version:1,durable:true,acceptingUploads:config.imagesEnabled===true,maxBytes:5*1024*1024,maxBatch:50,namespace:config.namespace}});
  }));
  router.put('/:imageId/chunks/:index',route(async(req,res)=>{
    if(config.imagesEnabled!==true)throw Object.assign(Error('IMAGE_UPLOAD_DISABLED'),{status:503});
    res.json({ok:true,image:await images.uploadChunk(namespace(req),req.params.imageId,req.body,Number(req.params.index))});
  }));
  router.post('/status',route(async(req,res)=>{
    if(req.body.namespace!==config.namespace)throw Object.assign(Error('NAMESPACE_MISMATCH'),{status:403});
    if(!Array.isArray(req.body.imageIds)||req.body.imageIds.length>50)throw Object.assign(Error('INVALID_IMAGE_IDS'),{status:400});
    const items=await Promise.all(req.body.imageIds.map(id=>images.get(namespace(req),id).catch(e=>{if(e.status===404)return {imageId:id,state:'NOT_FOUND'};throw e;})));
    res.json({ok:true,images:items});
  }));
  router.get('/:imageId/content',route(async(req,res)=>{
    const result=await images.content(namespace(req),req.params.imageId);
    res.set('Cache-Control','private, no-store');
    if(result.image)return res.json({ok:true,image:result.image});
    res.type(result.mime).send(result.bytes);
  }));
  router.get('/:imageId',route(async(req,res)=>res.json({ok:true,image:await images.get(namespace(req),req.params.imageId)})));
  router.post('/:imageId/retry',route(async(req,res)=>res.json({ok:true,image:await images.retry(namespace(req),req.params.imageId)})));
  router.post('/:imageId/links',route(async(req,res)=>res.json({ok:true,image:await images.links(namespace(req),req.params.imageId,req.body.links)})));
  return router;
};
