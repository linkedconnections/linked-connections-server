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
                const conns = JSON.parse(await readFile(`${path}/${fragment_date.toISOString()}.json`));
                console.log("inserting into AVL tree:", fragment_date.getTime());
                this.avlTree.insert(fragment_date.getTime(), conns);
            }));

            // if necessary, insert more static files to fill the AVL tree
            let staticFragmentCount = 0;
            if (fragments.length < this._numberOfFragments) {
                const amount_of_static_data = this._numberOfFragments - fragments.length;

                // get static data from now until 'amount_of_static_data' fragments in the future
                const now = new Date();
                const versions = utils.sortVersions(now, Object.keys(this.staticData.staticFragments[this.agency]));
                const [staticVersion, fr, index] = utils.findResource(this.agency, now.getTime(), versions, this.staticData.staticFragments);
                const staticFragments = this.staticData['staticFragments'][this.agency][staticVersion].slice(index, index + amount_of_static_data);
                this.min = staticFragments[0];
                this.max = staticFragments[staticFragments.length - 1];
                const path = `${utils.datasetsConfig['storage']}/linked_pages/${this.agency}/${staticVersion}`;

                // insert the static data into the AVL tree
                await Promise.all(staticFragments.map(async f => {
                    console.log("f:", f);
                    const fragment_date = new Date(f);
                    console.log("fragment_date:", fragment_date);
                    const conns = JSON.parse('['+await utils.readAndGunzip(`${path}/${fragment_date.toISOString()}.jsonld.gz`)+']');
                    console.log("inserting into AVL tree (static):", fragment_date.getTime());
                    this.avlTree.insert(fragment_date.getTime(), conns)
                    staticFragmentCount += 1;
                }));
            }

            logger.info(`Created AVL Tree for ${this.agency} (took ${(new Date().getTime() - start.getTime())} ms)`);
            logger.debug(`Number of Connections inserted in ${this.agency} AVL Tree: ${fragments.length + staticFragmentCount} total (${fragments.length} RT, ${staticFragmentCount} static)`);

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
        fragments.sort((a, b) => {
            return new Date(b) - new Date(a);
        });

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
            const newFragment = JSON.parse(await readFile(`${utils.datasetsConfig['storage']}/feed_history/${this.agency}/${fragments[0]}`));
            const newFragmentDate = new Date(newFragment.substring(0, newFragment.indexOf('.json')));
            this.avlTree.insert(newFragmentDate.getTime(), newFragment);

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
}

module.exports = ConnectionsFeed;