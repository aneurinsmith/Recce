
const neode = require('/usr/local/lib/node_modules/neode')

class Database {

    username = process.env.neo4j_user;
    password = process.env.neo4j_pass;
    ip = "neo4j.aneur.info";
    port = process.env.neo4j_port;

    constructor() {
        this.instance = new neode(
            `bolt+s//${this.ip}:${this.port}`,
            this.username, this.password
        );
    }

    get_instance() {
        return this.instance;
    }
}

module.exports = Database;