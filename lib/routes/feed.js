const util = require('util');
const fs = require('fs');
const utils = require('../utils/utils');

const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);
const writeFile = util.promisify(fs.writeFile);

class Feed{
    constructor() {
        this._storage = utils.datasetsConfig.storage;
        this._datasets = utils.datasetsConfig.datasets;
        this._serverConfig = utils.serverConfig;
    }

    async getFeed(req, res) {

    }

    createFeed(company) {

    }

    getDataset(name) {
        for (let i in this.datasets) {
            if (this.datasets[i].companyName === name) {
                return this.datasets[i];
            }
        }
    }

    get storage(){
        return this._storage;
    }

    get datasets(){
        return this._datasets
    }
}