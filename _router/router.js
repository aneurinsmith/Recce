
const VenueController = require('../controller/venue_controller');
const express = require('/usr/local/lib/node_modules/express');
const router = express.Router();

router.get('/recce/api/:lat,:lng/:venue?', VenueController.get);
router.get('/recce@:lat,:lng/:venue?', VenueController.index);
router.get('/recce', VenueController.index);

module.exports = router;
