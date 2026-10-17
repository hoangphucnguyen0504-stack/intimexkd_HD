'use strict';
const express = require('express');
const {createCoordination} = require('../services/coordination');
// Mount at /v1/coordination AFTER the existing API authentication middleware.
function createRouter({redis, config}) {
  const router = express.Router();
  const service = createCoordination(redis, config);
  router.use(express.json({limit: '300kb', strict: true}));
  router.post('/', (req, res, next) => {
    Promise.resolve().then(() => service.execute(req.body))
      .then(value => res.json({ok: true, value})).catch(next);
  });
  return router;
}
module.exports = {createRouter};
