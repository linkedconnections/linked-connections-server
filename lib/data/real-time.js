const fs = require('fs');
const util = require('util');
const utils = require('../utils/utils');

const readdir = util.promisify(fs.readdir);


class RealTimeData {
    /**
     * construct a RealTimeData object that keeps an index of all real-time files
     * @param storage
     * @param datasets
     */
    constructor(storage, datasets) {
        this._storage = `${storage}/feed_history/`;
        this._datasets = datasets;
        this._RTFragments = {};
    }

    /**
     * read all of the files in 'feed_history' and index the filenames into this.RTFragments
     */
    async initRTFragments() {
        // Iterate over all companies
        for (let i in this.datasets) {
            let companyName = this.datasets[i].companyName;
            // Object for keeping fragment scan
            if (!this.RTFragments[companyName]) {
                this.RTFragments[companyName] = {};
                let dir = await readdir(this.storage + companyName);
                let arr = [];
                // Keep each fragment in milliseconds since epoch in the scan
                for (let fragment of dir) {
                    // check if file is 'latest.json'
                    if (!(fragment === 'latest.json')) {
                        // convert filename to epoch time
                        let fragment_date = new Date(fragment.substring(0, fragment.indexOf('.json')));
                        arr.push(fragment_date.getTime());
                    }
                }
                if (arr.length > 0) {
                    //sort the array
                    arr.sort();
                    this.RTFragments[companyName] = arr;
                }
            }
        }
    }

    /**
     * add newly created files to the 'RTFragments' index. To make sure that the array of fragments is still sorted
     * after insertion, you should only insert new fragments.
     * @param companyName name of agency where we inserted the new RT fragment
     * @param fragment file name of the new fragment (ISO 8601 creation date with '.json' extension)
     */
    insertRTFragments(companyName, fragment) {
        let fragment_date = new Date(fragment.substring(0, fragment.indexOf('.json')));
        this.RTFragments[companyName].push(fragment_date.getTime());
    }

    get storage() {
        return this._storage;
    }

    get datasets() {
        return this._datasets;
    }

    get RTFragments() {
        return this._RTFragments;
    }
}

module.exports = RealTimeData;