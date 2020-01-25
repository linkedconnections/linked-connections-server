const util = require('util');
const fs = require('fs');
const gfs = require('graceful-fs');
const del = require('del');
const child_process = require('child_process');
const cron = require('cron');
const url = require('url');
const firstline = require('firstline');
const http = require('follow-redirects').http;
const https = require('follow-redirects').https;
const Logger = require('../utils/logger');
const utils = require('../utils/utils');
const { Connections, Connections2JSONLD } = require('gtfs2lc');
const jsonldstream = require('jsonld-stream');
const pageWriterStream = require('./pageWriterStream.js');
const { GtfsIndex, Gtfsrt2LC } = require('gtfsrt2lc');
const Catalog = require('../routes/catalog');
const Stops = require('../routes/stops');

const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(gfs.readFile);
const readdir = util.promisify(fs.readdir);
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
                // Download and process GTFS feed on server launch if required
                if (dataset.downloadOnLaunch) {
                    this.processStaticGTFS(index, dataset);
                }

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
        let t0 = new Date().getTime();
        let companyName = dataset.companyName;
        logger.info('Running cron job to update ' + companyName + ' GTFS feed');
        try {
            // Download GTFS feed
            let file_name = await this.downloadDataset(dataset);
            if (file_name != null) {
                // Create .lock file to prevent incomplete transformations
                writeFile(this.storage + '/datasets/' + companyName + '/' + file_name + '.lock', file_name
                    + ' GTFS feed being transformed to Linked Connections');

                // Reload GTFS identifiers and static indexes for RT processing, using new GTFS feed files
                if (dataset.realTimeData) {
                    logger.info('Updating GTFS identifiers for ' + companyName + '...');
                    // First pause RT job if is already running
                    if (this.jobs[index]['rt_job']) {
                        this.jobs[index]['rt_job'].stop();
                    }

                    this.loadGTFSIdentifiers(index, dataset, this.storage + '/datasets/' + companyName + '/.indexes').then(() => {
                        // Start RT job again or create new one if does not exist
                        if (this.jobs[index]['rt_job']) {
                            this.jobs[index]['rt_job'].start();
                        } else {
                            this.launchRTJob(index, dataset);
                        }
                    });
                }

                let path = this.storage + '/datasets/' + companyName + '/' + file_name + '.zip';
                // Unzip it
                let uncompressed_feed = await utils.readAndUnzip(path);
                logger.info(companyName + ' Dataset uncompressed');
                // Get base URIs for conversion to Linked Connections
                let baseURIs = this.getBaseURIs(dataset);
                // Organize GTFS data according to the required order by gtfs2lc tool 
                await this.preSortGTFS(uncompressed_feed);
                // Convert to Linked Connections
                let converter = new Connections({ store: 'LevelStore' });
                // Stream into a file for sorting
                let unsorted_path = this.storage + '/linked_connections/' + companyName + '/' + file_name + '_tmp.jsonld';
                let fileWriter = fs.createWriteStream(unsorted_path, 'utf8');

                converter.resultStream(uncompressed_feed, (resultStream, stopsdb) => {
                    logger.info('Creating ' + companyName + ' Linked Connections...');

                    resultStream.pipe(new Connections2JSONLD(baseURIs, stopsdb))
                        .pipe(new jsonldstream.Serializer())
                        .pipe(fileWriter)
                        .on('finish', async () => {
                            // Delete uncompressed GTFS data
                            del([this.storage + '/datasets/' + companyName + '/' + file_name + '_tmp'], { force: true });

                            // Sort Linked Connections by departure time
                            logger.info("Sorting " + companyName + " Linked Connections by departure time...");
                            let sorted_path = this.storage + '/linked_connections/' + companyName + '/' + file_name + '.jsonld';
                            await this.sortLCByDepartureTime(unsorted_path, sorted_path);

                            // Delete unsorted Linked Connections graph
                            del([unsorted_path], { force: true });

                            logger.info('Fragmenting ' + companyName + ' Linked Connections...');
                            // Create folder for Linked Connection fragments
                            fs.mkdirSync(this.storage + '/linked_pages/' + companyName + '/' + file_name)
                            // Proceed to fragment the Linked Connections graph
                            let reader = fs.createReadStream(sorted_path, 'utf8');
                            reader.pipe(new jsonldstream.Deserializer())
                                .pipe(new pageWriterStream(this.storage + '/linked_pages/' + companyName + '/'
                                    + file_name, dataset.fragmentSize || 50000))
                                .on('finish', async () => {
                                    logger.info('Compressing ' + companyName + ' Linked Connections fragments...');
                                    child_process.spawn('gzip', [file_name + '.jsonld'], {
                                        cwd: this.storage + '/linked_connections/' + companyName,
                                        stdio: 'ignore'
                                    });
                                    let comp = child_process.spawn('find', ['.', '-type', 'f', '-exec', 'gzip', '{}', '+'], {
                                        cwd: this.storage + '/linked_pages/' + companyName + '/' + file_name,
                                        stdio: 'ignore'
                                    });
                                    comp.on('close', async code => {
                                        let t1 = (new Date().getTime() - t0) / 1000;
                                        logger.info('Dataset conversion for ' + companyName + ' completed successfuly (took ' + t1 + ' seconds)');
                                        // GTFS feed completed successfully, proceed to delete .lock file
                                        del([this.storage + '/datasets/' + companyName + '/' + file_name + '.lock'], { force: true });
                                        // Update Catalog file
                                        let catalog = await new Catalog().createCatalog();
                                        await writeFile(this.storage + '/datasets/catalog.json', JSON.stringify(catalog), 'utf8')
                                        logger.info('DCAT catalog updated correctly');
                                        // Update Stops file
                                        await new Stops().createStopList(companyName);
                                        logger.info('Stops dataset for ' + companyName + ' updated');
                                    });
                                });
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
            let datasets_dir = this.storage + '/datasets/' + dataset.companyName;
            // Delete any _tmp folders (this means something went wrong last time)
            await del([datasets_dir + '/*_tmp', datasets_dir + '/.tmp', tmp], { force: true });

            let lastGtfs = await utils.getLatestGtfsSource(datasets_dir);
            
            if (lastGtfs !== null) {
                let indexer = new GtfsIndex(lastGtfs, tmp);
                let indexes = await indexer.getIndexes({}, dataset['realTimeData']['indexStore']);
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
                let parser = new Gtfsrt2LC(dataset.realTimeData.downloadUrl, this.getBaseURIs(dataset));
                // Set static indexes
                parser.setIndexes(this.indexes[index]);
                // Remove old remove records that won't be used anymore
                removeCache = this.cleanRemoveCache(removeCache, timestamp);
                // Use JSON-LD as output format
                let rtlc = await parser.parse('jsonld', true);

                rtlc.on('data', data => {
                    // Ignore @context
                    if (!data['@context']) {
                        rawData.push(data);
                    }
                });

                rtlc.on('end', async () => {
                    try {
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
                                    let removeFile = (await readFile(rt_path + '/' + origFragment.toISOString() + '.remove', 'utf8')).split('\n');
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
                                        removeList[origFragment.toISOString()] = [{ '@id': jodata['@id'], 'track': remRecord }];
                                    } else {
                                        removeList[origFragment.toISOString()].push({ '@id': jodata['@id'], 'track': remRecord });
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
                                        removeList[origFragment.toISOString()] = [{ '@id': jodata['@id'], 'track': [newFragment.toISOString()] }];
                                    } else {
                                        removeList[origFragment.toISOString()].push({ '@id': jodata['@id'], 'track': [newFragment.toISOString()] });
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

    downloadDataset(dataset) {
        if (dataset.downloadUrl.startsWith('http')) {
            const durl = url.parse(dataset.downloadUrl);
            if (durl.protocol == 'https:') {
                const options = {
                    hostname: durl.hostname,
                    port: 443,
                    path: durl.path,
                    method: 'GET'
                };

                return this.download_https(dataset, options);
            } else {
                return this.download_http(dataset, durl.href);
            }
        } else {
            return this.copyFileFromDisk(dataset);
        }
    }

    download_https(dataset, options) {
        return new Promise((resolve, reject) => {
            const req = https.request(options, res => {
                try {
                    let file_name = new Date(res.headers['last-modified']).toISOString();
                    let path = this.storage + '/datasets/' + dataset.companyName + '/' + file_name + '.zip';

                    if (!fs.existsSync(path)) {
                        let wf = fs.createWriteStream(path, { encoding: 'base64' });

                        res.on('data', d => {
                            wf.write(d);
                        }).on('end', () => {
                            wf.end();
                            wf.on('finish', () => {
                                resolve(file_name);
                            });
                        });
                    } else {
                        resolve(null);
                    }
                } catch (err) {
                    reject(err);
                }
            });

            req.on('error', err => {
                reject(err);
            });
            req.end();
        });
    }

    download_http(dataset, url) {
        return new Promise((resolve, reject) => {
            const req = http.get(url, res => {
                try {
                    let file_name = new Date(res.headers['last-modified']).toISOString();
                    let path = this.storage + '/datasets/' + dataset.companyName + '/' + file_name + '.zip';

                    if (!fs.existsSync(path)) {
                        let wf = fs.createWriteStream(path, { encoding: 'base64' });

                        res.on('data', d => {
                            wf.write(d);
                        }).on('end', () => {
                            wf.end();
                            wf.on('finish', () => {
                                resolve(file_name);
                            });
                        });
                    } else {
                        resolve(null);
                    }
                } catch (err) {
                    reject(err);
                }
            });

            req.on('error', err => {
                reject(err);
            });
            req.end();
        });
    }

    copyFileFromDisk(dataset) {
        return new Promise((resolve, reject) => {
            if (fs.existsSync(dataset.downloadUrl) && dataset.downloadUrl.endsWith('.zip')) {
                let stat = fs.statSync(dataset.downloadUrl);
                let name = new Date(util.inspect(stat.mtime)).toISOString();
                let copy = fs.createReadStream(dataset.downloadUrl)
                    .pipe(fs.createWriteStream(this.storage + '/datasets/' + dataset.companyName + '/' + name + '.zip'));
                copy.on('finish', () => {
                    resolve(name);
                });
            } else {
                reject(new Error('Invalid GTFS file'));
            }
        });
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

    preSortGTFS(path) {
        return new Promise((resolve, reject) => {
            const child = child_process.spawn('./node_modules/gtfs2lc/bin/gtfs2lc-sort.sh', [path]);

            child.on('close', (code, signal) => {
                if (signal === 'SIGTERM') {
                    reject(new Error('Process gtfs2lc-sort exit with code: ' + code));
                } else {
                    resolve();
                }
            });
        });
    }

    sortLCByDepartureTime(unsorted, sorted) {
        return new Promise(async (resolve, reject) => {
            // Find where is departureTime located inside the Linked Connection string
            let conn = await firstline(unsorted);
            let k = conn.split('"').indexOf('departureTime') + 2;
            // Proceed to sort using sort command
            child_process.exec('sort -T ' + this.storage + '/tmp/ -t \\" -k ' + k + ' ' + unsorted + ' > ' + sorted,
                (err, stdout, stderr) => {
                    if (err) {
                        logger.error(err);
                        reject();
                    }
                    resolve();
                });
        });
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
                    await del([this.storage + '/datasets/' + dataset.companyName + '/baseUris.json',
                    this.storage + '/datasets/' + dataset.companyName + '/' + incomplete + '_tmp',
                    this.storage + '/datasets/' + dataset.companyName + '/' + incomplete + '.zip',
                    this.storage + '/datasets/' + dataset.companyName + '/' + incomplete + '.lock',
                    this.storage + '/linked_connections/' + dataset.companyName + '/*_tmp*',
                    this.storage + '/linked_pages/' + dataset.companyName + '/' + incomplete], { force: true });
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