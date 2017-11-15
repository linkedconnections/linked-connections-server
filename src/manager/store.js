const level = require('level');

class Store {
    constructor(name) {
        this.name = name;
        this._store = level(name);
    }

    async get(key) {
        try {
            return JSON.parse(await this._store.get(key));
        } catch(err) {
            throw err;
        }
    }

    put(key, value) {
        return this._store.put(key, value, {valueEncoding: 'json'});
    }
}

module.exports = Store;