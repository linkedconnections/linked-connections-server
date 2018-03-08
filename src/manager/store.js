const level = require('level');

class Store {
    constructor(name) {
        this.name = name;
        this._store = level(name);
    }

    async get(key) {
        try {
            return JSON.parse(await this._store.get(key));
        } catch (err) {
            throw err;
        }
    }

    put(key, value) {
        return this._store.put(key, value, { valueEncoding: 'json' });
    }

    async close() {
        return new Promise(async (resolve, reject) => {
            try {
                await this._store.close();
                if (this._store.isClosed()) {
                    resolve();
                } else {
                    reject(new Error(name + 'LevelDB store not properly closed'));
                }
            } catch (err) {
                reject(err);
            }
        });
    }
}

module.exports = Store;