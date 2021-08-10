const fs = require('fs');
const util = require('util');
const watch = require('node-watch');
const utils = require('../utils/utils');
const AVLTree = require('../utils/avl-tree');
const Logger = require('../utils/logger');

const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);
const logger = Logger.getLogger(utils.serverConfig.logLevel || 'info');

class ConnectionsFeed {
    constructor(options) {
        this._agency = options['agency'];
        this._staticData = options['staticData'];
        this._RTData = options['RTData'];
        this._avlTree = new AVLTree();
        this._delayIndex = {};
        this._min = null;
        this._max = null;
        this._watcher = null;
        this._recreateJob = null;
        this._latestVersion = null;
        this._numberOfUpdates = 10;
    }

    async init() {
        try {
            const start = new Date();

            // Make sure we start fresh
            this.clear();

            // get an object with 'numberOfUpdates' of the most recent updates.
            let updates = this.getCurrentUpdates(this.numberOfUpdates);

            // load the updates drom disk and insert them into AVL tree
            const path = `${utils.datasetsConfig['storage']}/feed_history/${this.agency}`;
            await Promise.all(Object.keys(updates).map(async update => {
                // parse and store all fragments in the update
                const data = {};
                await Promise.all(updates[update].map(async fragment => {
                    const fragment_date = new Date(fragment);
                    data[fragment_date.getTime()] = JSON.parse(await readFile(`${path}/${update}/${fragment_date.toISOString()}.json`));
                }));

                // insert update into AVL tree
                const update_date = new Date(update);
                this.avlTree.insert(update_date.getTime(), data);
            }));

            logger.info(`[feed] Created feed AVL Tree for ${this.agency} (took ${(new Date().getTime() - start.getTime())} ms)`);
            logger.debug(`[feed] Number of RT updates inserted in ${this.agency} AVL Tree: ${Object.keys(updates).length}`);

            // Check for real-time updates by watching the latest.json file for real-time updates
            const latest = `${utils.datasetsConfig['storage']}/real_time/${this.agency}/latest.json`;
            if (!fs.existsSync(latest)) {
                await writeFile(latest, 'await for rt update', 'utf8');
            }

            // add watcher that checks for updates in latest.json
            this.watcher = watch(latest, (event, filename) => {
                if (filename && event === 'update') {
                    this.handleUpdate();
                }
            });

        } catch (err) {
            logger.error('[feed] ' + err);
            console.error('[feed]', err);
        }
    }

    /**
     * get the most recent RT update with its fragments
     * @returns object with creation date of update as key and an array of data fragments in epoch time as value
     */
    async getNewestUpdate() {
        let update_dirs = await readdir(`${utils.datasetsConfig['storage']}/feed_history/${this.agency}`);
        update_dirs.sort();

        // remove 'latest.json' if it exists
        const latestIndex = update_dirs.indexOf("latest.json");
        if (latestIndex > -1) {
            update_dirs.splice(latestIndex, 1);
        }

        const created = update_dirs[0];

        // transform all fragments that are part of the update to Dates and store them
        const fragment_names = await readdir(`${utils.datasetsConfig['storage']}/feed_history/${this.agency}/${update_dir}`)
        let fragments = []
        fragment_names.forEach(f => {
            const f_date = new Date(f.substring(0, f.indexOf(".json")));
            fragments.push(f_date.getTime());
        })

        return {created: fragments};
    }

    /**
     * get a certain amount of RT updates (with their fragments) by newest creation date
     * @param quantity amount of updates to return
     * @returns object with latest updates and their respective fragments
     */
    getCurrentUpdates(quantity) {
        let updates = {};
        let update_dirs = Object.keys(this.RTData.RTFragments[this.agency]);
        update_dirs.sort();

        // remove 'latest.json' if it exists
        const latestIndex = update_dirs.indexOf("latest.json");
        if (latestIndex > -1) {
            update_dirs.splice(latestIndex, 1);
        }

        // get the necessary quantity of updates and find their respective fragments
        update_dirs.slice(0, quantity);
        update_dirs.forEach(update => updates[update] = this.RTData.RTFragments[this.agency][update]);

        return updates;
    }

    async handleUpdate() {
        try {
            let start = new Date();

            // check if AVL tree is 'full'
            if (this.avlTree.size >= this.numberOfUpdates) {
                // if tree is full, delete oldest update
                this.avlTree.remove(this.avlTree.min(), this.avlTree.minNode())
            }

            const latest_update = await this.getNewestUpdate();
            const update = Object.keys(latest_update)[0];

            // parse and store all fragments in the update
            const data = {};
            await Promise.all(updates[update].map(async fragment => {
                let fragment_date = new Date(fragment);
                data[fragment_date.getTime()] = JSON.parse(await readFile(`${path}/${update}/${fragment_date.toISOString()}.json`));
            }));

            // insert update into AVL tree
            const update_date = new Date(update);
            this.avlTree.insert(update_date.getTime(), data);
            await this.RTData.insertRTFragments(this.agency, update);

            logger.debug(`[feed] -----------Update ${this.agency} AVL Tree-------------`);
            logger.debug(`[feed] Load live data from disk took ${(new Date().getTime() - start.getTime())} ms`);
        } catch (err) {
            logger.error('[feed] ' + err);
            console.error('[feed]', err);
        }
    }

    clear() {
        this.avlTree.clear();
        this.delayIndex = {};
        this.min = null;
        this.max = null;
        this.recreateJob = null;
        this.watcher = null;
    }

    get agency() {
        return this._agency;
    }

    get staticData() {
        return this._staticData;
    }

    get avlTree() {
        return this._avlTree;
    }

    get delayIndex() {
        return this._delayIndex;
    }

    set delayIndex(index) {
        this._delayIndex = index;
    }

    get min() {
        return this._min;
    }

    set min(min) {
        this._min = min;
    }

    get max() {
        return this._max;
    }

    set max(max) {
        this._max = max;
    }

    get watcher() {
        return this._watcher;
    }

    set watcher(watcher) {
        this._watcher = watcher;
    }

    get recreateJob() {
        return this._recreateJob;
    }

    set recreateJob(job) {
        this._recreateJob = job;
    }

    get latestVersion() {
        return this._latestVersion;
    }

    set latestVersion(newVersion) {
        this._latestVersion = newVersion;
    }

    get numberOfUpdates() {
        return this._numberOfUpdates;
    }

    set numberOfUpdates(numberOfUpdates) {
        this._numberOfUpdates = numberOfUpdates;
    }

    get RTData() {
        return this._RTData
    }
}

module.exports = ConnectionsFeed;