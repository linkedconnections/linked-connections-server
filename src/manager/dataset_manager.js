const util = require('util');
const fs = require('fs');
const del = require('del');
const child_process = require('child_process');
const cron = require('cron');
const url = require('url');
const http = require('follow-redirects').http;
const https = require('follow-redirects').https;
const zlib = require('zlib');
const csv = require('fast-csv');
const through2 = require('through2');
const Logger = require('../utils/logger');
const utils = require('../utils/utils');
const Store = require('./store');
const paginator = require('../paginator/paginator');
const Gtfsrt2lc = require('./gtfsrt2lc');

const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);
const gzip = util.promisify(zlib.gzip);
const exec = util.promisify(child_process.exec);
var logger = null;

class DatasetManager {
    constructor() {
        this._config = utils.datasetsConfig;
        this._datasets = this._config.datasets;
        this._storage = this._config.storage;
        this._jobs = [];
        this._stores = [];
        logger = Logger.getLogger(utils.serverConfig.logLevel || 'info');
        this.initDirs();
    }

    initDirs() {
        if (this.storage.endsWith('/')) {
            this.storage = this.storage.substring(0, this.storage.length - 1);
        }

        if (!fs.existsSync(this.storage + '/tmp')) {
            fs.mkdirSync(this.storage + '/tmp');
        }

        if (!fs.existsSync(this.storage + '/datasets')) {
            fs.mkdirSync(this.storage + '/datasets');
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
        // Verify that there are no incomplete processes
        await this.cleanUpIncompletes();
        // Update static fragments structure in memory
        await utils.updateStaticFragments();

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
                    // Load GTFS identifiers
                    let loaded = await this.loadGTFSIdentifiers(index, dataset);
                    // If identifiers were loaded schedule GTFS-RT processing job
                    if (loaded) this.launchRTJob(index, dataset);
                    // Schedule compression or RT files
                    this.rtCompressionJob(dataset);
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

        if (!fs.existsSync(this.storage + '/linked_connections/' + name)) {
            fs.mkdirSync(this.storage + '/linked_connections/' + name);
        }

        if (!fs.existsSync(this.storage + '/linked_pages/' + name)) {
            fs.mkdirSync(this.storage + '/linked_pages/' + name);
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

                let path = this.storage + '/datasets/' + companyName + '/' + file_name + '.zip';
                // Unzip it
                await utils.readAndUnzip(path);
                logger.info(companyName + ' Dataset extracted');
                // Set base URIs for conversion to Linked Connections
                await this.setBaseUris(dataset);
                // Convert to Linked Connections
                await this.convertGTFS2LC(companyName, file_name);
                logger.info('Fragmenting ' + companyName + ' Linked Connections...');
                // Fragment dataset into linked data documents
                await paginator.paginateDataset(this.storage + '/linked_connections/' + companyName + '/' + file_name + '.jsonld',
                    this.storage + '/linked_pages/' + companyName + '/' + file_name, companyName, dataset.fragmentSize || 300000);
                logger.info('Compressing ' + companyName + ' Linked Connections fragments...')
                // Compress all linked data documents
                child_process.spawn('gzip', [file_name + '.jsonld'], { cwd: this.storage + '/linked_connections/' + companyName, detached: true });
                await exec('find . -type f -exec gzip {} +', { cwd: this.storage + '/linked_pages/' + companyName + '/' + file_name });
                let t1 = (new Date().getTime() - t0) / 1000;
                logger.info('Dataset conversion for ' + companyName + ' completed successfuly (took ' + t1 + ' seconds)');
                // GTFS feed completed successfuly, proceed to delete .lock file
                del([this.storage + '/datasets/' + companyName + '/' + file_name + '.lock'], {force: true});

                // Reload GTFS identifiers and static indexes for RT processing, using new GTFS feed files
                if (dataset.realTimeData) {
                    logger.info('Updating GTFS identifiers for ' + companyName + '...');
                    // First pause RT job if is already running
                    if (this.jobs[index]['rt_job']) {
                        this.jobs[index]['rt_job'].stop();
                    }
                    // Update static fragments structure in memory
                    await utils.updateStaticFragments();
                    // Reload levelDB of GTFS identifiers
                    await this.loadGTFSIdentifiers(index, dataset);
                    // Start RT job again or create new one if does not exist
                    if (this.jobs[index]['rt_job']) {
                        this.jobs[index]['rt_job'].start();
                    } else {
                        this.launchRTJob(index, dataset);
                    }
                }
            } else {
                logger.warn(companyName + " dataset was already downloaded");
            }
        } catch (err) {
            logger.error(err);
        }
    }

    loadGTFSIdentifiers(index, dataset) {
        return new Promise(async (resolve, reject) => {
            try {
                let datasets_dir = this.storage + '/datasets/' + dataset.companyName;

                // Close and delete previous existing GTFS identifier stores
                if (this.stores[index]) {
                    if (this.stores[index]['trips']) {
                        await this.stores[index]['trips'].close();
                    }

                    if (this.stores[index]['routes']) {
                        await this.stores[index]['routes'].close();
                    }
                }

                if (fs.existsSync(datasets_dir + '/.routes') || fs.existsSync(datasets_dir + '/.trips')) {
                    this.stores[index] = {};
                    await del([datasets_dir + '/.routes', datasets_dir + '/.trips'], {force: true});
                }

                // Delete any _tmp folders (this means something went wrong last time)
                await del([datasets_dir + '/*_tmp'], {force: true});

                // Get the last obtained GTFS feed 
                let gtfs_files = await readdir(datasets_dir);
                if (gtfs_files.length > 0) {
                    let last_dataset = gtfs_files[gtfs_files.length - 1];
                    let unziped_gtfs = await utils.readAndUnzip(datasets_dir + '/' + last_dataset);
                    // Store identifiers in LevelDBs hidden files
                    let routesdb = new Store(datasets_dir + '/.routes');
                    let tripsdb = new Store(datasets_dir + '/.trips');

                    // Parse the GTFS files using fast-csv lib
                    let routes = fs.createReadStream(unziped_gtfs + '/routes.txt', { encoding: 'utf8', objectMode: true })
                        .pipe(csv({ objectMode: true, headers: true }))
                        .on('error', err => {
                            reject(err);
                        });
                    let trips = fs.createReadStream(unziped_gtfs + '/trips.txt', { encoding: 'utf8', objectMode: true })
                        .pipe(csv({ objectMode: true, headers: true }))
                        .on('error', err => {
                            reject(err);
                        });

                    // Use through2 transform stream to store every id in the LevelDB
                    routes.pipe(through2.obj(async (route, enc, done) => {
                        if (route['route_id']) {
                            await routesdb.put(route['route_id'], route);
                        }
                        done();
                    }))
                        .on('error', e => {
                            reject(e);
                        })
                        .on('finish', () => {
                            // Store Routes LevelDB reference object to be used in RT updates
                            if (!this.stores[index]) this.stores[index] = {};
                            this.stores[index]['routes'] = routesdb;
                            finish();
                        });

                    trips.pipe(through2.obj(async (trip, enc, done) => {
                        if (trip['trip_id']) {
                            await tripsdb.put(trip['trip_id'], trip);
                        }
                        done();
                    }))
                        .on('error', e => {
                            reject(e);
                        })
                        .on('finish', () => {
                            // Store Trips LevelDB reference object to be used in RT updates
                            if (!this.stores[index]) this.stores[index] = {};
                            this.stores[index]['trips'] = tripsdb;
                            finish();
                        });

                    let count = 0;
                    let self = this;
                    // Function to sync streams
                    let finish = () => {
                        count++;
                        if (count === 2) {
                            // Delete temporal dir with unziped GTFS files
                            del([unziped_gtfs], {force: true});
                            logger.info('GTFS identifiers updated for ' + dataset.companyName);
                            resolve(true);
                        }
                    };
                } else {
                    // There are no GTFS feeds
                    logger.warn('There are no ' + dataset.companyName + ' GTFS feeds present, therefore is not possible to'
                        + ' start the GTFS-RT job. Make sure to obtain a static GTFS feed first');
                    resolve(false);
                }
            } catch (err) {
                reject(err);
            }
        });
    }

    launchRTJob(index, dataset) {
        let companyName = dataset.companyName;
        let rt_path = this.storage + '/real_time/' + companyName;
        if (!fs.existsSync(rt_path)) {
            fs.mkdirSync(rt_path);
        }

        let rt_job = new cron.CronJob({
            cronTime: dataset.realTimeData.updatePeriod,
            onTick: async () => {
                try {
                    // Get RT data dump and convert it to Linked Connections
                    let gtfsrtParser = new Gtfsrt2lc(dataset, this.stores[index]);
                    let rtcs = await gtfsrtParser.processFeed();
                    // Timestamp that indicates when the data was obtained
                    let timestamp = new Date();
                    // Object to group the updates by fragment 
                    let rtDataObject = {};
                    // Object to keep track of the connections that are moved to different fragments due to delays
                    let removeList = {};

                    // Group all connection updates into fragment based arrays according to predefined fragment time span
                    let fragTimeSpan = dataset.realTimeData.fragmentTimeSpan || 600;
                    for (let x in rtcs) {
                        // Determine current fragment that the connection belongs to, due to delays
                        let jodata = rtcs[x];
                        let ndt = new Date(jodata.departureTime);
                        let newFragment = new Date(ndt.getTime() - (ndt.getTime() % (fragTimeSpan * 1000))).toISOString();
                        // Determine connection's original fragment
                        let odt = new Date(ndt.getTime() - (jodata['departureDelay'] * 1000));
                        let fragment = new Date(odt.getTime() - (odt.getTime() % (fragTimeSpan * 1000))).toISOString();

                        // Check if connection should be presented in a different fragment due to delays and register it
                        if (newFragment != fragment) {
                            if (removeList[fragment]) {
                                removeList[fragment].push(jodata['@id']);
                            } else {
                                removeList[fragment] = [jodata['@id']];
                            }
                        }

                        // Add timestamp to RT data for versioning
                        jodata['mementoVersion'] = timestamp.toISOString();

                        if (!rtDataObject[newFragment]) rtDataObject[newFragment] = [];
                        rtDataObject[newFragment].push(JSON.stringify(jodata));
                    }

                    // Write new data into fragment files
                    await this.updateRTData(rtDataObject, rt_path);
                    // Write removeList into file
                    this.storeRemoveList(removeList, rt_path, timestamp.toISOString());

                    let t1 = new Date().getTime();
                    let tf = t1 - timestamp.getTime();
                    logger.info(companyName + ' GTFS-RT feed updated for version ' + timestamp.toISOString() + ' (took ' + tf + ' ms)');
                } catch (err) {
                    logger.error('Error getting GTFS-RT feed for ' + companyName + ': ' + err);
                }
            },
            start: true
        });

        // Store Job reference for management
        if (!this.jobs[index]) this.jobs[index] = {};
        this.jobs[index]['rt_job'] = rt_job;
        logger.info('GTFS-RT job for ' + companyName + ' scheduled correctly');
    }

    rtCompressionJob(dataset) {
        let companyName = dataset.companyName;

        let rt_compression_job = new cron.CronJob({
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
    }

    download_https(dataset, options) {
        return new Promise((resolve, reject) => {
            const req = https.request(options, res => {
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
                req.on('error', err => {
                    reject(err);
                });
            });
        });
    }

    async setBaseUris(dataset) {
        let uri = dataset.baseURIs;
        let config = {};

        if (typeof uri == 'undefined' || uri == '') {
            config = {
                'stops': 'http://example.org/stops/',
                'connections': 'http://example.org/connections/',
                'trips': 'http://example.org/trips/',
                'routes': 'http://example.org/routes/'
            }
        } else {
            config = {
                'stops': uri.stops,
                'connections': uri.connections,
                'trips': uri.trips,
                'routes': uri.routes
            }
        }

        await writeFile(this.storage + '/datasets/' + dataset.companyName + '/baseUris.json', JSON.stringify(config));
        return Promise.resolve();
    }

    convertGTFS2LC(companyName, file_name) {
        return new Promise((resolve, reject) => {
            const child = child_process.spawn('./gtfs2lc.sh', [companyName, file_name, this.storage], { cwd: './src/manager', detached: true });
            let error = '';

            child.stdout.on('data', data => {
                logger.info(data.toString().replace(/[\r\n]/g, ''));
            });

            //TODO: Fix stderr log messages in gtfs2lc to handle errors

            /*child.stderr.on('data', err => {
                error = err;
                logger.info('stderr: ' + err);
                process.kill(-child.pid);
            });*/

            child.on('close', (code, signal) => {
                if (signal === 'SIGTERM') {
                    reject(new Error(error));
                } else {
                    resolve();
                }
            });
        });
    }

    storeRemoveList(removeList, path, memento) {
        Object.entries(removeList).forEach(async ([key, value]) => {
            let file_path = path + '/' + key + '.remove';
            let data = value.join(',' + memento + '\n').concat(',' + memento);

            // Register every connection is not in its original fragment due to delays
            if (fs.existsSync(file_path)) {
                fs.appendFile(file_path, '\n' + data, 'utf8', err => {
                    if (err) throw err;
                });
            } else {
                fs.appendFile(file_path, data, 'utf8', err => {
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
                    this.storage + '/linked_pages/' + dataset.companyName + '/' + incomplete], {force: true});
                logger.info('Incomplete ' + dataset.companyName + ' GTFS feed from ' + incomplete + ' cleaned correctly');
            }
        }));
    }

    get storage() {
        return this._storage;
    }

    get jobs() {
        return this._jobs;
    }

    get stores() {
        return this._stores;
    }
}

module.exports = DatasetManager;