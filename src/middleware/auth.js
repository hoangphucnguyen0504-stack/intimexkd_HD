'use strict';
const {authorized} = require('../utils/protocol');
module.exports = token => (req, res, next) => {
  if (!authorized(req.headers.authorization, token)) return res.status(401).json({ok: false, code: 'UNAUTHORIZED'});
  next();
};
