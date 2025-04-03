
module.exports = 
{
    search: (req, res) => {
        res.render('pages/search', {
            title: "recce.",
            styles: [
                "https://cdn.jsdelivr.net/npm/font-awesome@4.7.0/css/font-awesome.min.css",
                "https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css",
                "/recce/css/search.css"
            ],
            scripts: [
                "https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.min.js"
            ]
        });
    },

    result: (req, res) => {
        res.render('pages/result', {
            title: "recce.",
            styles: [
                "https://cdn.jsdelivr.net/npm/font-awesome@4.7.0/css/font-awesome.min.css",
                "https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css",
                "https://unpkg.com/maplibre-gl@^5.3.0/dist/maplibre-gl.css"
            ],
            scripts: [
                "https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.min.js",
                "https://unpkg.com/maplibre-gl@^5.3.0/dist/maplibre-gl.js"
            ]
        });
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
