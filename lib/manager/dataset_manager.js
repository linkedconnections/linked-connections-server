const util = require('util');
const fs = require('fs');
const del = require('del');
const child_process = require('child_process');
const cron = require('cron');
const Logger = require('../utils/logger');
const utils = require('../utils/utils');
const { Connections } = require('gtfs2lc');
const JsonLParser = require('stream-json/jsonl/Parser');
const pageWriterStream = require('./pageWriterStream.js');
const { GtfsIndex, Gtfsrt2LC } = require('gtfsrt2lc');
const Catalog = require('../routes/catalog');
const Stops = require('../routes/stops');
const Routes = require('../routes/routes');

const writeFile = util.promisify(fs.writeFile);
const readdir = util.promisify(fs.readdir);
const exec = util.promisify(child_process.exec);
var logger = null;

class DatasetManager {
    constructor() {
        this._config = utils.datasetsConfig;
        this._datasets = this._config.datasets;
        this._storage = this._config.storage;
        this._jobs = [];
        this._indexes = [];
        logger = Logger.getLogger(utils.serverConfig.logLevel || 'info');
    }

    initDirs() {
        if (this.storage.endsWith('/')) {
            this.storage = this.storage.substring(0, this.storage.length - 1);
        }

        if (!fs.existsSync(this.storage)) {
            fs.mkdirSync(this.storage);
        }

        if (!fs.existsSync(this.storage + '/tmp')) {
            fs.mkdirSync(this.storage + '/tmp');
        }

        if (!fs.existsSync(this.storage + '/datasets')) {
            fs.mkdirSync(this.storage + '/datasets');
        }

        if (!fs.existsSync(this.storage + '/stops')) {
            fs.mkdirSync(this.storage + '/stops');
        }

        if (!fs.existsSync(this.storage + '/routes')) {
            fs.mkdirSync(this.storage + '/routes');
        }

        if (!fs.existsSync(this.storage + '/catalog')) {
            fs.mkdirSync(this.storage + '/catalog');
        }

        if (!fs.existsSync(this.storage + '/feed_history')) {
            fs.mkdirSync(this.storage + '/feed_history');
        }

        if (!fs.existsSync(this.storage + '/linked_connections')) {
            fs.mkdirSync(this.storage + '/linked_connections');
        }

        if (!fs.existsSync(this.storage + '/linked_pages')) {
            fs.mkdirSync(this.storage + '/linked_pages');
        }

        if (!fs.existsSync(this.storage + '/real_time')) {
            fs.mkdirSync(this.storage + '/real_time');
        }
    }

    async manage() {
        // Create required folders
        this.initDirs();
        // Verify that there are no incomplete processes
        await this.cleanUpIncompletes();
        this._datasets.forEach(async (dataset, index) => {
            try {
                // Create necessary dirs
                this.initCompanyDirs(dataset.companyName);
                // Schedule GTFS feed processing job
                this.launchStaticJob(index, dataset);

                const datasets_dir = this.storage + '/datasets/' + dataset.companyName;
                // Delete any temp folders (this means something went wrong last time)
                await del([datasets_dir + '/*_tmp', datasets_dir + '/.tmp', datasets_dir + '/.indexes'], { force: true });

                // Handle real-time data if available
                if (dataset.realTimeData) {
                    // Get GTFS indexes
                    let loaded = await this.loadGTFSIdentifiers(index, dataset, this.storage + '/real_time/' + dataset.companyName + '/.indexes');
                    // If identifiers were loaded schedule GTFS-RT processing job
                    if (loaded) {
                        this.launchRTJob(index, dataset);
                        // Schedule compression or RT files
                        this.rtCompressionJob(dataset);
                    } else {
                        logger.warn('There are no GTFS datasets present for ' + dataset.companyName +
                            '. Make sure to provide one before handling real-time data.');
                    }
                }

                // Download and process GTFS feed on server launch if required
                if (dataset.downloadOnLaunch) {
                    this.processStaticGTFS(index, dataset);
                }
            } catch (err) {
                logger.error(err);
            }
        });
    }

    initCompanyDirs(name) {
        if (!fs.existsSync(this.storage + '/datasets/' + name)) {
            fs.mkdirSync(this.storage + '/datasets/' + name);
        }

        if (!fs.existsSync(this.storage + '/stops/' + name)) {
            fs.mkdirSync(this.storage + '/stops/' + name);
        }

        if (!fs.existsSync(this.storage + '/routes/' + name)) {
            fs.mkdirSync(this.storage + '/routes/' + name);
        }

        if (!fs.existsSync(this.storage + '/catalog/' + name)) {
            fs.mkdirSync(this.storage + '/catalog/' + name);
        }

        if (!fs.existsSync(this.storage + '/feed_history/' + name)) {
            fs.mkdirSync(this.storage + '/feed_history/' + name);
        }

        if (!fs.existsSync(this.storage + '/linked_connections/' + name)) {
            fs.mkdirSync(this.storage + '/linked_connections/' + name);
        }

        if (!fs.existsSync(this.storage + '/linked_pages/' + name)) {
            fs.mkdirSync(this.storage + '/linked_pages/' + name);
        }

        if (!fs.existsSync(this.storage + '/real_time/' + name)) {
            fs.mkdirSync(this.storage + '/real_time/' + name);
        }
    }

    launchStaticJob(index, dataset) {
        let static_job = new cron.CronJob({
            cronTime: dataset.updatePeriod,
            onTick: () => {
                this.processStaticGTFS(index, dataset);
            },
            start: true
        });

        if (!this.jobs[index]) this.jobs[index] = {};
        this.jobs[index]['static_job'] = static_job;
        logger.info('GTFS job for ' + dataset.companyName + ' scheduled correctly');
    }

    async processStaticGTFS(index, dataset) {
        const t0 = new Date().getTime();
        const companyName = dataset.companyName;
        logger.info('Running cron job to update ' + companyName + ' GTFS feed');

        try {
            // Download GTFS feed
            const file_name = await this.getDataset(dataset);

            if (file_name != null) {
                // Create .lock file to prevent incomplete transformations
                const lockPath = `${this.storage}/datasets/${companyName}/${file_name}.lock`;
                await writeFile(lockPath, `${file_name} GTFS feed being transformed to Linked Connections`);

                // Reload GTFS identifiers and static indexes for RT processing, using new GTFS feed files
                if (dataset.realTimeData) {
                    logger.info(`Updating GTFS identifiers for ${companyName}...`);
                    // First pause RT job if is already running
                    if (this.jobs[index]['rt_job']) {
                        this.jobs[index]['rt_job'].stop();
                    }

                    this.loadGTFSIdentifiers(index, dataset, `${this.storage}/datasets/${companyName}/.indexes`).then(() => {
                        // Start RT job again or create new one if does not exist
                        if (this.jobs[index]['rt_job']) {
                            this.jobs[index]['rt_job'].start();
                        } else {
                            this.launchRTJob(index, dataset);
                        }
                    });
                }

                const path = `${this.storage}/datasets/${companyName}/${file_name}.zip`;
                // Unzip it
                const uncompressed_feed = utils.readAndUnzip(path);
                logger.info(companyName + ' Dataset uncompressed');

                // Get base URIs for conversion to Linked Connections
                const baseURIs = this.getBaseURIs(dataset);
                // Convert to Linked Connections
                const converter = new Connections({
                    format: 'jsonld',
                    store: 'LevelStore',
                    baseUris: baseURIs,
                    compressed: true,
                    fresh: true
                });
                // Path where connections will be created
                const connsPath = `${this.storage}/linked_connections/${companyName}`;
                logger.info(`Creating ${companyName} Linked Connections...`);

                converter.resultStream(uncompressed_feed, connsPath, async rawConns => {

                    logger.info(`Sorting and fragmenting ${companyName} Linked Connections...`);
                    // Create folder for Linked Connection fragments
                    fs.mkdirSync(`${this.storage}/linked_pages/${companyName}/${file_name}`);

                    // Proceed to sort and fragment the Linked Connections graph
                    const sorted = await this.sortLCByDepartureTime(rawConns);

                    sorted.pipe(JsonLParser.parser())
                        .pipe(new pageWriterStream(`${this.storage}/linked_pages/${companyName}/${file_name}`,
                            dataset.fragmentSize || 300))
                        .on('finish', async () => {
                            const t1 = (new Date().getTime() - t0) / 1000;
                            logger.info(`Dataset conversion for ${companyName} completed successfully (took ${t1} seconds)`);

                            // Update Catalog, Stops and Routes
                            const [stops, routes] = await Promise.all([
                                new Stops(uncompressed_feed).createStopList(companyName),
                                new Routes(uncompressed_feed).createRouteList(companyName),
                            ]);
                            await Promise.all([
                                writeFile(`${this.storage}/stops/${companyName}/stops.json`, JSON.stringify(stops), 'utf8'),
                                writeFile(`${this.storage}/routes/${companyName}/routes.json`, JSON.stringify(routes), 'utf8'),
                            ]);

                            const catalog = await new Catalog().createCatalog(companyName);
                            await writeFile(`${this.storage}/catalog/${companyName}/catalog.json`, JSON.stringify(catalog), 'utf8'),

                                logger.info('DCAT catalog updated correctly');
                            logger.info(`Stop dataset for ${companyName} updated`);
                            logger.info(`Route dataset for ${companyName} updated`);

                            // Clean up
                            await del([
                                lockPath,
                                uncompressed_feed,
                            ], { force: true });
                        });
                });
            } else {
                logger.warn(companyName + " dataset was already downloaded");
            }
        } catch (err) {
            logger.error(err);
        }
    }

    async loadGTFSIdentifiers(index, dataset, tmp) {
        try {
            const datasets_dir = this.storage + '/datasets/' + dataset.companyName;

            let lastGtfs = await utils.getLatestGtfsSource(datasets_dir);

            if (lastGtfs !== null) {
                const rtData = dataset['realTimeData'];
                let indexer = new GtfsIndex({
                    path: lastGtfs,
                    auxPath: tmp,
                    headers: rtData['headers']
                });

                logger.info(`Updating ${dataset.companyName} indexes for live data processing`);
                let indexes = await indexer.getIndexes({
                    store: rtData['indexStore'],
                    deduce: rtData['deduce']
                });
                // Keep references to the static indexes for this company
                if (!this.indexes[index]) this.indexes[index] = [];
                this.indexes[index] = indexes;
                return true;
            }
        } catch (err) {
            logger.error(err);
            return false;
        }

        return false;
    }

    launchRTJob(index, dataset) {
        let companyName = dataset.companyName;
        let rt_path = this.storage + '/real_time/' + companyName;
        if (!fs.existsSync(rt_path)) {
            fs.mkdirSync(rt_path);
        }

        // Object to keep in memory the .remove files and avoid reading them from disk every time.
        let removeCache = {};

        let rt_job = new cron.CronJob({
            cronTime: dataset.realTimeData.updatePeriod,
            onTick: async () => {
                await this.processLiveUpdate(index, dataset, rt_path, removeCache);
            },
            start: true
        });

        // Store Job reference for management
        if (!this.jobs[index]) this.jobs[index] = {};
        this.jobs[index]['rt_job'] = rt_job;
        logger.info('GTFS-RT job for ' + companyName + ' scheduled correctly');
    }

    processLiveUpdate(index, dataset, rt_path, removeCache) {
        return new Promise(async (resolve, reject) => {
            try {
                // Timestamp that indicates when the data was obtained
                let timestamp = new Date();
                // Array to store all the incoming raw connection updates
                let rawData = [];
                // Object to group the updates by fragment 
                let rtDataObject = {};
                // Object to keep track of the connections that are moved to different fragments due to delays
                let removeList = {};
                // Group all connection updates into fragment based arrays according to predefined fragment time span
                let fragTimeSpan = dataset.realTimeData.fragmentTimeSpan || 600;
                // Proceed to parse GTFS-RT
                let parser = new Gtfsrt2LC({
                    path: dataset.realTimeData.downloadUrl,
                    uris: this.getBaseURIs(dataset),
                    headers: dataset.realTimeData.headers
                });
                // Set static indexes
                parser.setIndexes(this.indexes[index]);
                // Remove old remove records that won't be used anymore
                removeCache = this.cleanRemoveCache(removeCache, timestamp);
                // Use JSON-LD as output format
                let rtlc = await parser.parse({ format: 'jsonld', objectMode: true });

                rtlc.on('data', data => {
                    // Ignore @context
                    if (!data['@context']) {
                        rawData.push(data);
                    }
                });

                rtlc.on('end', async () => {
                    try {
                        // write data to buffer (a buffer is just a json file named after the current time, and will
                        // contain the newest update)
                        const now = new Date();

                        // sort the raw data objects by departureTime, so that we can easily paginate on this later
                        rawData.sort((a, b) => (a.departureTime > b.departureTime) ? 1 : ((b.departureTime > a.departureTime) ? -1 : 0));

                        // split rawData into chunks of 100 objects
                        const chunks = Array.from(
                            new Array(Math.ceil(rawData.length / 100)),
                            (_, i) => rawData.slice(i * 100, i * 100 + 100)
                        );

                        // create a new folder where all current updates will be stored
                        fs.mkdirSync(`${this.storage}/feed_history/${dataset['companyName']}/${now.toISOString()}`);

                        // write all of these chunks to separate compressed files, then write rawData to both
                        // latest.json files
                        await Promise.all(chunks.map(async chunk => {
                            const departure_time = new Date(chunk[0].departureTime).toISOString();
                            const path = `${this.storage}/feed_history/${dataset['companyName']}/${now.toISOString()}/${departure_time}.json`;
                            await utils.writeAndGzip(path, JSON.stringify(chunk));
                        }))
                            .then(await Promise.all([
                                await writeFile(`${this.storage}/feed_history/${dataset['companyName']}/latest.json`, JSON.stringify(rawData), 'utf8'),
                                await writeFile(`${this.storage}/real_time/${dataset['companyName']}/latest.json`, JSON.stringify(rawData), 'utf8')
                            ]));

                        // Update rt fragments
                        await Promise.all(rawData.map(async jodata => {
                            // Determine current fragment that the connection belongs to, due to delays
                            let ndt = new Date(jodata.departureTime);
                            let newFragment = new Date(ndt.getTime() - (ndt.getTime() % (fragTimeSpan * 1000)));
                            // Determine connection's original fragment
                            let odt = new Date(ndt.getTime() - (jodata['departureDelay'] * 1000));
                            let origFragment = new Date(odt.getTime() - (odt.getTime() % (fragTimeSpan * 1000)));

                            // Check if there are previous remove reports for the original fragment
                            if (fs.existsSync(rt_path + '/' + origFragment.toISOString() + '.remove')) {
                                let origRemove = null;
                                let remRecord = null;

                                // Check if the .remove file has been previously read from the disk
                                if (removeCache[origFragment.toISOString()]) {
                                    origRemove = removeCache[origFragment.toISOString()];
                                } else {
                                    // Since this loop is async, we need to read this file synchronously 
                                    // to get it into the cache as soon as possible and not create a lot of promises
                                    // for reading the same file over and over.
                                    let removeFile = fs.readFileSync(rt_path + '/' + origFragment.toISOString() + '.remove', 'utf8').split('\n');
                                    removeFile.pop();
                                    origRemove = new Map();

                                    for (let k in removeFile) {
                                        let rec = removeFile[k].split(',');
                                        origRemove.set(rec[0], rec);
                                    }
                                    // Keep in memory in case it needs to be reused
                                    removeCache[origFragment.toISOString()] = origRemove;
                                }
                                // Look for the last record of removal for this Connection, if any.
                                if (origRemove.has(jodata['@id'])) {
                                    remRecord = origRemove.get(jodata['@id']);
                                }

                                // Previous record found
                                if (remRecord != null) {
                                    // Get the previous fragments where the connection has been
                                    remRecord = remRecord.slice(2);
                                    // Register the removal record on other fragments where the connection has been
                                    for (let j in remRecord) {
                                        // Skip the current fragment because it is where the connection is now
                                        if (remRecord[j] != newFragment.toISOString()) {
                                            if (!removeList[remRecord[j]]) {
                                                removeList[remRecord[j]] = [{ '@id': jodata['@id'] }];
                                            } else {
                                                removeList[remRecord[j]].push({ '@id': jodata['@id'] });
                                            }
                                            // Update the removeCache to avoid reloading the file afterwards
                                            if (removeCache[remRecord[j]]) {
                                                removeCache[remRecord[j]].set(jodata['@id'], [jodata['@id'], timestamp.toISOString()]);
                                            }
                                        }
                                    }

                                    // Add new fragment to the connection record if it is being written in a new fragment
                                    if (origFragment.getTime() != newFragment.getTime()) {
                                        if (remRecord.indexOf(newFragment.toISOString()) < 0) {
                                            remRecord.push(newFragment.toISOString());
                                        }
                                    }
                                } else {
                                    // There was no previous record, create it if it is being written in a new fragment
                                    if (origFragment.getTime() != newFragment.getTime()) {
                                        remRecord = [newFragment.toISOString()];
                                    }
                                }

                                // Register the removal record on the original fragment while keeping record of all the fragments
                                // the connection has been.
                                if (origFragment.getTime() != newFragment.getTime()) {
                                    if (!removeList[origFragment.toISOString()]) {
                                        removeList[origFragment.toISOString()] = [{
                                            '@id': jodata['@id'],
                                            'track': remRecord
                                        }];
                                    } else {
                                        removeList[origFragment.toISOString()].push({
                                            '@id': jodata['@id'],
                                            'track': remRecord
                                        });
                                    }

                                    // Update the removeCache to avoid reloading the file afterwards
                                    let newRec = [jodata['@id'], timestamp.toISOString()];
                                    if (remRecord != null) {
                                        newRec = newRec.concat(remRecord);
                                    }

                                    removeCache[origFragment.toISOString()].set(jodata['@id'], newRec);
                                }
                            } else {
                                // There is no .remove file for the original file yet. Just register the remove record on the original 
                                // fragment and keep track of the new fragment the connection is moving to.
                                if (origFragment.getTime() != newFragment.getTime()) {
                                    if (!removeList[origFragment.toISOString()]) {
                                        removeList[origFragment.toISOString()] = [{
                                            '@id': jodata['@id'],
                                            'track': [newFragment.toISOString()]
                                        }];
                                    } else {
                                        removeList[origFragment.toISOString()].push({
                                            '@id': jodata['@id'],
                                            'track': [newFragment.toISOString()]
                                        });
                                    }
                                }
                            }

                            // Add timestamp to RT data for versioning
                            jodata['mementoVersion'] = timestamp.toISOString();
                            if (!rtDataObject[newFragment.toISOString()]) rtDataObject[newFragment.toISOString()] = [];
                            rtDataObject[newFragment.toISOString()].push(JSON.stringify(jodata));
                        }));
                        // Write new data into fragment files and removeList into files
                        await Promise.all([
                            this.updateRTData(rtDataObject, rt_path),
                            this.storeRemoveList(removeList, rt_path, timestamp.toISOString())
                        ])

                        let t1 = new Date().getTime();
                        let tf = t1 - timestamp.getTime();
                        logger.info(dataset.companyName + ' GTFS-RT feed updated for version ' + timestamp.toISOString() + ' (took ' + tf + ' ms)');
                        resolve();
                    } catch (err) {
                        logger.error(err);
                        reject(err);
                    }
                });
            } catch (err) {
                logger.error('Error getting GTFS-RT feed for ' + dataset.companyName + ': ' + err);
                reject(err);
            }
        });
    }

    rtCompressionJob(dataset) {
        let companyName = dataset.companyName;

        new cron.CronJob({
            cronTime: dataset.realTimeData.compressionPeriod,
            onTick: async () => {
                try {
                    let path = this.storage + '/real_time/' + companyName + '/';
                    let now = new Date();

                    let fgmts = await readdir(path);

                    fgmts.forEach(fg => {
                        if (fg.indexOf('.gz') < 0) {
                            // Get last modification time of the file
                            let stats = fs.statSync(path + '/' + fg);
                            let lastModified = new Date(util.inspect(stats.mtime));

                            // If the file hasn't been modified in the last 4 hours, compress it
                            if (now.getTime() - lastModified.getTime() >= 14400000) {
                                child_process.spawn('gzip', [fg], { cwd: path, detached: true });
                            }
                        }
                    });

                } catch (err) {
                    logger.error('Error compressing RT files for ' + companyName + ': ' + err);
                }
            },
            start: true
        });
    }

    getDataset(dataset) {
        const path = `${this.storage}/datasets/${dataset.companyName}`;
        if (dataset.downloadUrl.startsWith('http')) {
            return utils.downloadGTFSToDisk(dataset.downloadUrl, [], path);
        } else {
            return utils.copyFileFromDisk(dataset.downloadUrl, path);
        }
    }

    getBaseURIs(dataset) {
        if (dataset.baseURIs && dataset.baseURIs !== '') {
            return dataset.baseURIs;
        } else {
            return {
                'stop': 'http://example.org/stops/{stop_id}',
                'route': 'http://example.org/routes/{routes.route_id}',
                'trip': 'http://example.org/trips/{trips.trip_id}',
                'connection': 'http://example.org/connections/{connection.departureTime(yyyyMMdd)}{connection.departureStop}{trips.trip_id}'
            };
        }
    }

    async sortLCByDepartureTime(unsorted) {
        try {
            // Amount of memory that sort can use
            const mem = this._config.sortMemory || '2G';
            // Find where is departureTime located inside the Linked Connection string
            const conn = (await exec(`zcat ${unsorted} | head -2 | tail -1`))['stdout'];
            const k = conn.split('"').indexOf('departureTime') + 2;

            // Sort using UNIX sort command
            const zcat = child_process.spawn('zcat', [`${unsorted}`]);
            const sort = child_process.spawn('sort', [
                '-S', mem,
                '-T', `${this.storage}/tmp/`,
                '-t', '\"',
                '-k', k,
                '--compress-program=gzip'
            ]);

            zcat.stdout.pipe(sort.stdin);

            // Return readable stream of sorted connections
            return sort.stdout;
        } catch (err) {
            throw err;
        }
    }

    cleanRemoveCache(removeCache, timestamp) {
        // Set time limit to 3 hours before the current moment
        let timeLimit = new Date(timestamp.getTime() - (3 * 3600 * 1000));
        let keys = Object.keys(removeCache);
        for (let i in keys) {
            let k = new Date(keys[i]);
            if (k < timeLimit) {
                delete removeCache[k];
            }
        }

        return removeCache;
    }

    storeRemoveList(removeList, path, memento) {
        Object.entries(removeList).forEach(async ([key, value]) => {
            let file_path = path + '/' + key + '.remove';

            let data = '';
            for (let r in value) {
                let rec = value[r];
                data = data.concat(rec['@id'] + ',' + memento);
                if (rec['track'] && rec['track'] != null) {
                    data = data.concat(',' + rec['track'].join(',') + '\n');
                } else {
                    data = data.concat('\n');
                }
            }

            // Register every connection that is not in its original fragment due to delays
            if (fs.existsSync(file_path)) {
                fs.appendFile(file_path, '\n' + data.trim(), 'utf8', err => {
                    if (err) throw err;
                });
            } else {
                fs.appendFile(file_path, data.trim(), 'utf8', err => {
                    if (err) throw err;
                });
            }
        });
    }

    updateRTData(data, path) {
        return new Promise((resolve, reject) => {
            try {
                // Array to store promises for writing fragment files
                let written = [];

                // Update RT fragment files with new data (asynchronously)
                Object.entries(data).forEach(async ([key, value]) => {
                    let updData = value.join('\n');
                    let file_path = path + '/' + key + '.jsonld';

                    if (!fs.existsSync(file_path)) {
                        fs.appendFile(file_path, updData, 'utf8', err => {
                            if (err) throw err;
                        });
                    } else {
                        fs.appendFile(file_path, '\n' + updData, 'utf8', err => {
                            if (err) throw err;
                        });
                    }
                });

                // All RT fragment files updated. RT update completed
                Promise.all(written).then(() => {
                    resolve();
                });
            } catch (err) {
                reject(err);
            }
        });
    }

    cleanUpIncompletes() {
        return Promise.all(this._datasets.map(async dataset => {
            if (fs.existsSync(this.storage + '/datasets/' + dataset.companyName)) {
                let files = await readdir(this.storage + '/datasets/' + dataset.companyName);
                let incomplete = null;

                for (let i in files) {
                    if (files[i].endsWith('.lock')) {
                        incomplete = files[i].substring(0, files[i].indexOf('.lock'));
                        break;
                    }
                }

                if (incomplete !== null) {
                    logger.warn('Incomplete ' + dataset.companyName + ' GTFS feed found (' + incomplete + ')');
                    await del([
                        this.storage + '/datasets/' + dataset.companyName + '/baseUris.json',
                        this.storage + '/datasets/' + dataset.companyName + '/' + incomplete + '_tmp',
                        this.storage + '/datasets/' + dataset.companyName + '/' + incomplete + '.zip',
                        this.storage + '/datasets/' + dataset.companyName + '/' + incomplete + '.lock',
                        this.storage + '/linked_connections/' + dataset.companyName + '/raw*',
                        this.storage + '/linked_connections/' + dataset.companyName + '/*_tmp*',
                        this.storage + '/linked_connections/' + dataset.companyName + '/*.json',
                        this.storage + '/linked_connections/' + dataset.companyName + '/*.db',
                        this.storage + '/linked_connections/' + dataset.companyName + '/*.txt',
                        this.storage + '/linked_pages/' + dataset.companyName + '/' + incomplete
                    ], { force: true });
                    logger.info('Incomplete ' + dataset.companyName + ' GTFS feed from ' + incomplete + ' cleaned correctly');
                }
            }
        }));
    }

    get storage() {
        return this._storage;
    }

    get jobs() {
        return this._jobs;
    }

    get indexes() {
        return this._indexes;
    }
}

module.exports = DatasetManager;