
const express = require('/usr/local/lib/node_modules/express');
const admin = express.Router();

admin.use('/recce/admin/$', (req,res)=> {
    res.status(501).send('page not implemented yet');
});

module.exports = admin;
