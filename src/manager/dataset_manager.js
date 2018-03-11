const util = require('util');
const fs = require('fs');
const child_process = require('child_process');
const cron = require('cron');
const url = require('url');
const http = require('follow-redirects').http;
const https = require('follow-redirects').https;
const zlib = require('zlib');
const csv = require('fast-csv');
const through2 = require('through2');
const logger = require('../utils/logger');
const utils = require('../utils/utils');
const Store = require('./store');
const paginator = require('../paginator/paginator');
const Gtfsrt2lc = require('./gtfsrt2lc');

const writeFile = util.promisify(fs.writeFile);
const readdir = util.promisify(fs.readdir);
const gzip = util.promisify(zlib.gzip);
const execFile = util.promisify(child_process.execFile);
const exec = util.promisify(child_process.exec);

class DatasetManager {
    constructor() {
        this._config = utils.datasetsConfig;
        this._datasets = this._config.datasets;
        this._storage = this._config.storage;
        this._jobs = [];
        this._stores = [];

        this.initDirs();
    }

    initDirs() {
        if (this.storage.endsWith('/')) {
            this.storage = this.storage.substring(0, this.storage.length - 1);
        }

        if (!fs.existsSync(this.storage + '/tmp')) {
            child_process.execSync('mkdir ' + this.storage + '/tmp');
        }

        if (!fs.existsSync(this.storage + '/datasets')) {
            child_process.execSync('mkdir ' + this.storage + '/datasets');
        }

        if (!fs.existsSync(this.storage + '/linked_connections')) {
            child_process.execSync('mkdir ' + this.storage + '/linked_connections');
        }

        if (!fs.existsSync(this.storage + '/linked_pages')) {
            child_process.execSync('mkdir ' + this.storage + '/linked_pages');
        }

        if (!fs.existsSync(this.storage + '/real_time')) {
            child_process.execSync('mkdir ' + this.storage + '/real_time');
        }
    }

    manage() {
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
            child_process.execSync('mkdir ' + this.storage + '/datasets/' + name);
        }

        if (!fs.existsSync(this.storage + '/linked_connections/' + name)) {
            child_process.execSync('mkdir ' + this.storage + '/linked_connections/' + name);
        }

        if (!fs.existsSync(this.storage + '/linked_pages/' + name)) {
            child_process.execSync('mkdir ' + this.storage + '/linked_pages/' + name);
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
                    this.storage + '/linked_pages/' + companyName + '/' + file_name, companyName, dataset.fragmentSize);
                logger.info('Compressing ' + companyName + ' Linked Connections fragments...')
                // Compress all linked data documents
                child_process.spawn('gzip', [file_name + '.jsonld'], { cwd: this.storage + '/linked_connections/' + companyName, detached: true });
                await exec('find . -type f -exec gzip {} +', { cwd: this.storage + '/linked_pages/' + companyName + '/' + file_name });
                let t1 = (new Date().getTime() - t0) / 1000;
                logger.info('Dataset conversion for ' + companyName + ' completed successfuly (took ' + t1 + ' seconds)');

                // Reload GTFS identifiers and static indexes for RT processing, using new GTFS feed files
                if (dataset.realTimeData) {
                    logger.info('Updating GTFS identifiers for ' + companyName + '...');
                    // First pause RT job if is already running
                    if (this.jobs[index]['rt_job']) {
                        this.jobs[index]['rt_job'].stop();
                    }
                    await utils.updateStaticFragments();
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
                    await exec('rm -r .routes .trips', { cwd: datasets_dir });
                }

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
                    let finish = function () {
                        count++;
                        if (count === 2) {
                            // Delete temporal dir with unziped GTFS files
                            exec('rm -r ' + unziped_gtfs, { cwd: datasets_dir });
                            logger.info('GTFS identifiers updated for ' + dataset.companyName);
                            resolve(true);
                        }
                    };
                } else {
                    // There are no GTFS feeds
                    logger.warn('There are no ' + dataset.companyName + ' GTFS feeds present, therefore is not possible to start the GTFS-RT job');
                    logger.warn('Make sure to obtain a static GTFS feed first');
                    resolve(false);
                }
            } catch (err) {
                reject(err);
            }
        });
    }

    launchRTJob(index, dataset) {
        let companyName = dataset.companyName;
        if (!fs.existsSync(this.storage + '/real_time/' + companyName)) {
            child_process.execSync('mkdir ' + this.storage + '/real_time/' + companyName);
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

                    // Get ordered list of versions 
                    let lsv = utils.sortVersions(timestamp, Object.keys(utils.staticFragments[companyName]));

                    // Group all connection updates into fragment based arrays
                    for (let x in rtcs) {
                        let jodata = removeDelays(JSON.parse(rtcs[x]));
                        let dt = new Date(jodata.departureTime);

                        // Use binary search algorithm to find correspondent fragment according to static data.
                        let fragment = utils.findResource(companyName, dt, lsv)[1].toISOString();

                        // Add timestamp to RT data for versioning
                        jodata['mementoVersion'] = timestamp.toISOString();
                        let rtdata = JSON.stringify(jodata);

                        if (!rtDataObject[fragment]) rtDataObject[fragment] = [];
                        rtDataObject[fragment].push(rtdata);
                    }

                    // Write new data into fragment files
                    await this.updateRTData(rtDataObject, companyName);
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
        let path = this.storage + '/real_time/' + companyName;

        let rt_compression_job = new cron.CronJob({
            cronTime: dataset.realTimeData.compressionPeriod,
            onTick: async () => {
                try {
                    let now = new Date();
                    now.setDate(now.getDate() - 1);
                    let dir_name = utils.getRTDirName(now);
                    if (fs.existsSync(path + '/' + dir_name)) {
                        await exec('find . -type f -exec gzip {} +', { cwd: path + '/' + dir_name });
                        logger.info(companyName + ' RT files from ' + dir_name + ' folder compressed successfully');
                    }
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

    updateRTData(data, companyName) {
        return new Promise((resolve, reject) => {
            try {
                // Array to store promises for writing fragment files
                let written = [];

                // Update RT fragment files with new data (asynchronously)
                Object.entries(data).forEach(async ([key, value]) => {
                    // Create folders to store real-time updates by day
                    let dir_date = new Date(key);
                    let dir_name = utils.getRTDirName(dir_date);
                    let dir_path = this.storage + '/real_time/' + companyName + '/' + dir_name;
                    if (!fs.existsSync(dir_path)) {
                        child_process.execSync('mkdir ' + dir_path);
                    }

                    let updData = value.join('\n');
                    let file_path = dir_path + '/' + key + '.jsonld';

                    if (!fs.existsSync(file_path)) {
                        fs.appendFile(file_path, updData, 'utf8', err => {
                            if (err) throw new Error();
                        });
                    } else {
                        fs.appendFile(file_path, '\n' + updData, 'utf8', err => {
                            if (err) throw new Error();
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

function removeDelays(jo) {
    let dt = new Date(jo['departureTime']);
    let at = new Date(jo['arrivalTime']);
    dt.setTime(dt.getTime() - (jo['departureDelay'] * 1000));
    at.setTime(at.getTime() - (jo['arrivalDelay'] * 1000));
    jo['departureTime'] = dt.toISOString();
    jo['arrivalTime'] = at.toISOString();
    return jo;
}