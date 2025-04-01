
const Database = require('../config/neo4j')

class Stop {
    constructor() {
        this.db = new Database();
    }
    
    async find(lat, lng) {
        return this.db.execute(`
            
            WITH // Initial search params
                {location: point({latitude: $lat, longitude: $lng})} AS curr

            MATCH // Match first stops
                (fs:Stop)
            WHERE (NOT (fs)-[:CHILD_OF]->(:Stop))
              AND point.distance(fs.location, curr.location) < 1800

            RETURN fs.stop_id

        `, {lat, lng})
    }
}

module.exports = Stop;
