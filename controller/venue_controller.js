
module.exports = 
{
    index: (req, res) => {
        res.render('pages/index', {title: "recce."});
    },

    get: async (req, res) => {
        const Venue = require('../model/venue');
        const venue = new Venue();

        const lat = req.params.lat;
        const lng = req.params.lng;
        const dest = req.params.venue;
        const day = req.query.day;
        const time = req.query.time;
        const atco = req.query.atco;

        console.log({lat, lng, dest, day, time, atco});

        if ((lat >= -90 && lat <= 90 && lng >= -180 && lng <= 180)) {
            res.json(await venue.find({lat, lng, dest, day, time, atco}));
        } else {
            res.status(404).sendFile('/usr/share/nginx/html/404.html');
        }
    }
}
