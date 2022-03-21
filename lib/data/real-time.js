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
            }

            // iterate over all update directories in the current company directory
            let company_dir = await readdir(this.storage + companyName);
            for (let update of company_dir) {
                if ((update !== 'latest.json') && !this.RTFragments[companyName][update]) {
                    // if 'update' is a directory, read all its file names and save these fragment indexes in epoch time
                    let update_dir = await readdir(`${this.storage}${companyName}/${update}`);
                    let arr = [];

                    for (let file of update_dir) {
                        // each file will be a fragment
                        const fragment_date = new Date(file.substring(0, file.indexOf('.json.gz')));
                        arr.push(fragment_date.getTime());
                    }

                    // store the array of epoch indexes in RTFragments
                    if (arr.length > 0) {
                        arr.sort();
                        this.RTFragments[companyName][update] = arr;
                    }
                }

            }
        }
    }

    /**
     * add newly created files to the 'RTFragments' index. This method is called after every RT update (see 'connections-feed.js').
     * @param companyName name of agency where we want to insert the new RT fragments
     * @param update directory name of the new update (ISO 8601 creation date of update). This directory contains 
     *               all of the update files sorted by departureTime (ISO 8601 creation date of update).
     */
    async insertRTFragments(companyName, update) {
        let arr = [];
        let update_dir = await readdir(`${this.storage}${companyName}/${update}`);
        for (let file of update_dir) {
            const fragment_date = new Date(file.substring(0, file.indexOf('.json')));
            arr.push(fragment_date.getTime());
        }
        if (arr.length > 0) {
            this.RTFragments[companyName][update] = arr;
        }
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