
const VenueController = require('../controller/venue_controller');
const express = require('/usr/local/lib/node_modules/express');
const router = express.Router();

router.get('/recce/api/:lat,:lng/:venue?', VenueController.get);
router.get('/recce@:lat,:lng/:venue', VenueController.result);
router.get('/recce@:lat,:lng', VenueController.search);
router.get('/recce', VenueController.search);

module.exports = router;
