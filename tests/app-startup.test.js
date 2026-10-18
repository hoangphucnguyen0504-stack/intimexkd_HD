'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const {EventEmitter}=require('node:events');
const {createApp,waitForRedisReady,logStartupFailure}=require('../src/app');
function connection(status='connecting') {return Object.assign(new EventEmitter(),{status});}
function cleaned(redis) {for(const event of ['ready','error','end'])assert.equal(redis.listenerCount(event),0,event);}
test('app exports factory and already-ready Redis needs no listeners',async()=>{
  assert.equal(typeof createApp,'function');
  const redis=connection('ready');await waitForRedisReady(redis,100);cleaned(redis);
});
test('transient Redis errors allow reconnect and clean listeners after ready',async()=>{
  const redis=connection(),waiting=waitForRedisReady(redis,1000);
  redis.emit('error',Object.assign(Error('temporary'),{code:'ECONNREFUSED'}));
  redis.status='reconnecting';redis.emit('error',Error('retry'));
  redis.status='ready';redis.emit('ready');await waiting;cleaned(redis);
});
test('Redis readiness timeout rejects and cleans listeners',{timeout:2000},async()=>{
  const redis=connection();await assert.rejects(waitForRedisReady(redis,15),{code:'REDIS_READY_TIMEOUT'});cleaned(redis);
});
test('closed Redis connection fails immediately rather than awaiting timeout',async()=>{
  const redis=connection(),waiting=waitForRedisReady(redis,1000);
  const rejection=assert.rejects(waiting,{code:'REDIS_CONNECTION_CLOSED'});
  redis.status='end';redis.emit('end');await rejection;cleaned(redis);
  const ended=connection('end');await assert.rejects(waitForRedisReady(ended,1000),{code:'REDIS_CONNECTION_CLOSED'});cleaned(ended);
});
test('startup errors log allowlisted codes without connection secrets',()=>{
  const original=console.error,logs=[];console.error=value=>logs.push(JSON.parse(value));
  try {
    logStartupFailure(Object.assign(Error('redis://user:secret@host'),{code:'REDIS_READY_TIMEOUT'}));
    logStartupFailure(Object.assign(Error('private-key-secret'),{code:'PRIVATE_SECRET'}));
    logStartupFailure(null);
  } finally {console.error=original;}
  assert.deepEqual(logs.map(row=>row.code),['REDIS_READY_TIMEOUT','STARTUP_FAILED','STARTUP_FAILED']);
  assert.ok(logs.every(row=>row.event==='queue-api-start-failed'));
  assert.ok(!JSON.stringify(logs).includes('secret'));
});
