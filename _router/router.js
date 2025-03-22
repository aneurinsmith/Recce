
const express = require('/usr/local/lib/node_modules/express');
const router = express.Router();

router.use('/recce@:lat,:lng', (req, res)=> {
    res.status(501).send('page not implemented yet');
});

router.use('/recce/api/:lat,:lng/:venue?', (req, res)=> {
    res.status(501).send('api not implemented yet');
});

module.exports = router;
