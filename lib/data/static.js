const fs = require('fs');
const util = require('util');
const utils = require('../utils/utils');

const readdir = util.promisify(fs.readdir);

class StaticData {
    constructor(storage, datasets) {
        this._storage = `${storage}/linked_pages/`;
        this._datasets = datasets;
        this._staticFragments = {};
    }

    async updateStaticFragments() {
        // Iterate over all companies
        for (let i in this.datasets) {
            let companyName = this.datasets[i].companyName;

            // If there is at least one static version available proceed to scan it
            if (fs.existsSync(this.storage + companyName)) {
                // Object for keeping fragment scan of every static version
                if (!this.staticFragments[companyName]) {
                    this.staticFragments[companyName] = {};
                }

                let versions = await readdir(this.storage + companyName);
                for (let y in versions) {
                    if (!this.staticFragments[companyName][versions[y]]) {
                        let dir = await readdir(this.storage + companyName + '/' + versions[y]);
                        let arr = [];
                        // Keep each fragment in milliseconds since epoch in the scan
                        for (let z in dir) {
                            // Check that this version is not being currently converted
                            if (dir[z].indexOf('.gz') >= 0) {
                                let fd = new Date(dir[z].substring(0, dir[z].indexOf('.jsonld')));
                                arr.push(fd.getTime());
                            } else if(dir[z].endsWith('.jsonld')){
                                // Version is being processed now, discard it to avoid incomplete scans 
                                arr = [];
                                break;
                            }
                        }

                        if (arr.length > 0) {
                            this.staticFragments[companyName][versions[y]] = arr;
                        }
                    } else {
                        continue;
                    }
                }
            }
        }
    }

    get storage() {
        return this._storage;
    }

    get datasets() {
        return this._datasets;
    }

    get staticFragments() {
        return this._staticFragments;
    }
}

module.exports = StaticData;