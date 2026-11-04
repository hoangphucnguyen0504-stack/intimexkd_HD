'use strict';
const Redis = require('ioredis');
const {Queue} = require('bullmq');
function connect(config, worker = false) {
  const connection = new Redis(config.redisUrl, {maxRetriesPerRequest: worker ? null : 1,
    enableOfflineQueue: worker, connectTimeout: 5000, ...(worker ? {} : {commandTimeout: 8000}),
    retryStrategy: times => Math.min(5000, times * 250)});
  connection.on('error', error => {
    const allowed = ['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EAI_AGAIN', 'ENOTFOUND', 'EPIPE'];
    console.error(JSON.stringify({event: 'redis-unavailable', code: allowed.includes(error?.code) ? error.code : 'REDIS_ERROR'}));
  });
  return connection;
}
function queues(config, connection) {
  const options = {connection, prefix: config.prefix, defaultJobOptions: {
    removeOnComplete: false, removeOnFail: false, attempts: 5,
    backoff: {type: 'hdoi', delay: 1000}
  }};
  return {sheets: new Queue('sheets', options), background: new Queue('background', options),
    ...(config.imagesEnabled ? {images:new Queue('images',options),imageMetadata:new Queue('imageMetadata',options)} : {})};
}
module.exports = {connect, queues};
