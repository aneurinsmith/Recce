
const neo4j = require('/usr/local/lib/node_modules/neo4j-driver');

class Database {

    USER = process.env.NEO4J_USER;
    PASS = process.env.NEO4J_PASS;
    URI = "bolt+s://neo4j.aneur.info";

    constructor() {
        this.instance = neo4j.driver(this.URI, 
            neo4j.auth.basic(this.USER, this.PASS),
            {disableLosslessIntegers: true}
        );
    }

    async execute(query, params = {}) {
        const {records, summary, keys} = await this.instance.executeQuery(query, params, {database: 'recce'});

        return records.map(record => {
            return record.keys.reduce((acc, key, index) => {
                if (record.keys.length > 1) {
                    acc[key] = record._fields[index];
                } else {
                    acc = record._fields[index];
                }
                return acc;
            }, {})
        })
    }
}

module.exports = Database;
