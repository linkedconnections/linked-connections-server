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

    /**
     * initialise the connections feed by creating an AVL tree with real-time data and setting
     * up a watcher that updates the tree when new updates arrive
     *
     * @returns {Promise<void>}
     */
    async init() {
        try {
            const start = new Date();

            // Make sure we start fresh
            this.clear();

            // get <numberOfUpdates> of the most recent updates.
            let updates = this.getCurrentUpdates(this.numberOfUpdates);

            // load the updates from disk and insert them into AVL tree
            const path = `${utils.datasetsConfig['storage']}/feed_history/${this.agency}`;
            await Promise.all(Object.keys(updates).map(async update => {
                // parse and store all fragments in the update
                const data = {};
                await Promise.all(updates[update].map(async fragment => {
                    const fragment_date = new Date(fragment);
                    data[fragment_date.getTime()] = JSON.parse(await utils.readAndGunzip(`${path}/${update}/${fragment_date.toISOString()}.json.gz`));
                }));

                // insert update into AVL tree
                const update_date = new Date(update);
                this.avlTree.insert(update_date.getTime(), data);
            }));

            logger.info(` [feed] Created feed AVL Tree for ${this.agency} (took ${(new Date().getTime() - start.getTime())} ms)`);
            logger.debug(`[feed] Number of RT updates inserted in ${this.agency} AVL Tree: ${Object.keys(updates).length}`);

            // Check for real-time updates by watching the latest.json file
            const latest = `${utils.datasetsConfig['storage']}/feed_history/${this.agency}/latest.json`;
            if (!fs.existsSync(latest)) {
                await writeFile(latest, 'await for rt update', 'utf8');
            }
            // add watcher that checks for updates to latest.json
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
     * get the most recent real-time update folder with its fragments
     *
     * @returns {Promise<{}>} object with creation date of update as key and an array of data
     *                        fragments in epoch time as value
     */
    async getNewestUpdate() {
        // get all updates
        let update_dirs = await readdir(`${utils.datasetsConfig['storage']}/feed_history/${this.agency}`);
        update_dirs.sort().reverse();

        // remove 'latest.json' if it exists
        const latestIndex = update_dirs.indexOf("latest.json");
        if (latestIndex > -1) {
            update_dirs.splice(latestIndex, 1);
        }

        // transform all fragments that are part of the update to dates in epoch time and store them
        const fragment_names = await readdir(`${utils.datasetsConfig['storage']}/feed_history/${this.agency}/${update_dirs[0]}`)
        let fragments = []
        fragment_names.forEach(f => {
            const f_date = new Date(f.substring(0, f.indexOf(".json.gz")));
            fragments.push(f_date.getTime());
        });

        let result = {};
        result[update_dirs[0]] = fragments

        return result;
    }

    /**
     * get a certain amount of the latest real-time update folders (and their fragments)
     *
     * @param quantity amount of updates to return
     * @returns {{}} object containing the latest updates and their respective fragments
     */
    getCurrentUpdates(quantity) {
        let updates = {};
        let update_dirs = Object.keys(this.RTData.RTFragments[this.agency]);
        update_dirs.sort().reverse();

        // remove 'latest.json' if it exists
        const latestIndex = update_dirs.indexOf("latest.json");
        if (latestIndex > -1) {
            update_dirs.splice(latestIndex, 1);
        }

        // get the necessary quantity of updates and find their respective fragments
        update_dirs = update_dirs.slice(0, quantity);
        update_dirs.forEach(update => updates[update] = this.RTData.RTFragments[this.agency][update]);

        return updates;
    }

    /**
     * handle update of real-time data by updating the AVL tree accordingly
     * @returns {Promise<void>}
     */
    async handleUpdate() {
        try {
            let start = new Date();

            // if tree is full, delete oldest update
            if (this.avlTree.size >= this.numberOfUpdates) {
                this.avlTree.remove(this.avlTree.min(), this.avlTree.minNode())
            }

            const latest_update = await this.getNewestUpdate();
            const update = Object.keys(latest_update)[0];
            const path = `${utils.datasetsConfig['storage']}/feed_history/${this.agency}`;

            // parse and store all fragments in the update
            const data = {};
            await Promise.all(latest_update[update].map(async fragment => {
                let fragment_date = new Date(fragment);
                data[fragment_date.getTime()] = JSON.parse(await utils.readAndGunzip(`${path}/${update}/${fragment_date.toISOString()}.json.gz`));
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