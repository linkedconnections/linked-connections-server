import fs from 'fs';
import util from 'util';
import watch from 'node-watch';
import { CronJob } from 'cron';
import { Utils } from '../utils/utils.js';
import { getLogger } from '../utils/logger.js';
import { AVLTree } from '../utils/avl-tree.js';

const utils = new Utils();
const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);
const logger = getLogger(utils.serverConfig.logLevel || 'info');

export class Connections {
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
    }

    async init() {
        try {
            let t0 = new Date();
            // Make sure we start fresh
            this.clear();
            // Create AVL Tree from now until 100 fragments in the future
            const now = new Date();
            const versions = utils.sortVersions(now, Object.keys(this.staticData.staticFragments[this.agency]));
            const [staticVersion, fr, index] = utils.findResource(this.agency, now.getTime(), versions, this.staticData.staticFragments);
            const fragments = this.staticData['staticFragments'][this.agency][staticVersion].slice(index, index + 100);
            this.min = fragments[0];
            this.max = fragments[fragments.length - 1];
            const path = `${utils.datasetsConfig['storage']}/linked_pages/${this.agency}/${staticVersion}`;
            let connCount = 0;

            await Promise.all(fragments.map(async f => {
                const fragment = new Date(f).toISOString();
                const conns = (await utils.readAndGunzip(`${path}/${fragment}.jsonld.gz`)).split(',\n').map(JSON.parse);
                for (const cx of conns) {
                    this.avlTree.insert(new Date(cx['departureTime']).getTime(), cx);
                }
                connCount += conns.length;
            }));
            logger.info(`Created AVL Tree for ${this.agency} (took ${(new Date().getTime() - t0.getTime())} ms)`);
            logger.debug(`Number of Connections inserted in ${this.agency} AVL Tree: ${connCount}`);
            logger.debug(`Time window of the ${this.agency} tree: ${(this.max - this.min) / (1000 * 60)} minutes`);

            // Register the creation version for this connections source
            this.latestVersion = new Date();

            // Check for real-time updates by watching the latest.json file of real-time updates
            if (utils.getCompanyDatasetConfig(this.agency)['realTimeData']) {
                const latest = `${utils.datasetsConfig['storage']}/real_time/${this.agency}/latest.json`;
                if (!fs.existsSync(latest)) {
                    await writeFile(latest, 'await for rt update', 'utf8');
                } else {
                    // There is already an existing update
                    await this.handleUpdate();
                }

                this.watcher = watch(latest, (event, filename) => {
                    if (filename && event ==='update') {
                        this.handleUpdate();
                    }
                });
            }

            // Setup a cron job to recreate the tree every 10 minutes and shift its time window
            const recreateTree = new CronJob(
                '0 */10 * * * *', // TODO: make this configurable
                () => {
                    this.recreateJob.stop();
                    if (this.watcher) this.watcher.close();
                    logger.info(`Recreating AVL tree for ${this.agency}...`);
                    this.init();
                },
                null,
                true
            );

            this.recreateJob = recreateTree;
        } catch (err) {
            logger.error(err);
            console.error(err);
        }
    }

    async handleUpdate() {
        try {
            let t0 = new Date();
            const newConns = JSON.parse(await readFile(`${utils.datasetsConfig['storage']}/real_time/${this.agency}/latest.json`));
            logger.debug(`-----------Update ${this.agency} AVL Tree-------------`);
            logger.debug(`Load live data from disk took ${(new Date().getTime() - t0.getTime())} ms`);
            t0 = new Date();

            await Promise.all(newConns.map(async conn => {
                // Current connection departure time
                const curr = new Date(conn['departureTime']).getTime();

                // Only consider connections in the range of the tree
                if (curr >= this.min && curr < this.max) {
                    // Get original departure time
                    let dep = curr - (conn['departureDelay'] * 1000);

                    // Check if the connection had a delay
                    if (this.delayIndex[conn['@id']]) {
                        dep += (this.delayIndex[conn['@id']] * 1000);
                    }

                    // Check if the connection's departure time changed and update its position in the tree
                    if (curr !== dep) {
                        this.avlTree.remove(dep, '@id', conn['@id']);
                        this.avlTree.insert(curr, conn);
                    } else {
                        // Departure time didn't change but arrival time might have or the connection could've been cancelled
                        let node = this.avlTree.find(dep, '@id', conn['@id']);
                        if (node) {
                            // Connection existed in the tree, update its data
                            node.data = conn
                        } else {
                            // New Connection, add it to the tree
                            this.avlTree.insert(curr, conn);
                        }
                    }
                    // Register the last known departure delay for this connection so we can find it again
                    this.delayIndex[conn['@id']] = conn['departureDelay'];
                }
            }));

            this.latestVersion = new Date();
            logger.debug(`Reorganize tree nodes took ${(new Date().getTime() - t0.getTime())} ms`);
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
}