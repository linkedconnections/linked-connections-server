const fs = require('fs');
const util = require('util');
const watch = require('node-watch');
const utils = require('../utils/utils');
const AVLTree = require('../utils/avl-tree');
const Logger = require('../utils/logger');
// const RealTimeData = require('../data/real-time')
// const StaticData = require('../data/static');

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
        this._numberOfFragments = 100;
    }

    async init() {
        try {
            const start = new Date();

            // Make sure we start fresh
            this.clear();

            // Create AVL Tree which will consist the 100 most recent fragments

            // get list of current fragments in feed_history
            let fragments = await this.getSortedFragments();

            // if necessary, cut the list down to 100 fragments
            fragments.splice(this._numberOfFragments);

            // load the ordered fragments and insert them into AVL tree
            const path = `${utils.datasetsConfig['storage']}/feed_history/${this.agency}`;
            await Promise.all(fragments.map(async f => {
                const fragment_date = new Date(f.substring(0, f.indexOf('.json')));
                this.avlTree.insert(fragment_date.getTime(), JSON.parse(await readFile(`${path}/${fragment_date.toISOString()}.json`)));
            }));

            logger.info(`Created feed AVL Tree for ${this.agency} (took ${(new Date().getTime() - start.getTime())} ms)`);
            logger.debug(`Number of RT fragments inserted in ${this.agency} AVL Tree: ${fragments.length}`);

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
            logger.error(err);
            console.error(err);
        }
    }

    async getSortedFragments() {
        // get list of current fragments in feed_history
        let fragments = await readdir(`${utils.datasetsConfig['storage']}/feed_history/${this.agency}`);

        // remove 'latest.json' if it exists
        const latestIndex = fragments.indexOf("latest.json");
        if (latestIndex > -1) {
            fragments.splice(latestIndex, 1);
        }

        // sort the list from newest to oldest date
        fragments.sort();

        return fragments;
    }

    async handleUpdate() {
        try {
            let t0 = new Date();
            // check if AVL tree is 'full'
            if (this.avlTree.size >= this.numberOfFragments) {
                // if tree is full, delete oldest fragment
                this.avlTree.remove(this.avlTree.min(), this.avlTree.minNode())
            }
            const fragments = await this.getSortedFragments();
            // if tree is not full, just insert
            const latestFragment = fragments[fragments.length - 1];
            const newFragmentData = JSON.parse(await readFile(`${utils.datasetsConfig['storage']}/feed_history/${this.agency}/${latestFragment}`));
            const newFragmentDate = new Date();
            this.avlTree.insert(newFragmentDate.getTime(), newFragmentData);
            this.RTData.insertRTFragments(this.agency, latestFragment);

            logger.debug(`-----------Update ${this.agency} AVL Tree-------------`);
            logger.debug(`Load live data from disk took ${(new Date().getTime() - t0.getTime())} ms`);
        } catch (err) {
            logger.error(err);
            console.error(err);
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

    get numberOfFragments() {
        return this._numberOfFragments;
    }

    set numberOfFragments(numberOfFragments) {
        this._numberOfFragments = numberOfFragments;
    }

    get RTData() {
        return this._RTData
    }
}

module.exports = ConnectionsFeed;