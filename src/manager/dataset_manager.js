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
const Duration = require('duration-js');
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
                if (dataset.realTimeData) {
                    // Load GTFS identifiers
                    let loaded = await this.loadGTFSIdentifiers(index, dataset);
                    // If identifiers were loaded schedule GTFS-RT processing job
                    if (loaded) this.launchRTJob(index, dataset);
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
        let companyName = dataset.companyName;
        let static_job = new cron.CronJob({
            cronTime: dataset.updatePeriod,
            onTick: async () => {
                let t0 = new Date().getTime();
                logger.info('running cron job to update ' + companyName + ' GTFS feed');
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
                        logger.info('Initiating Linked Connections fragmentation process for ' + companyName + '...');
                        // Fragment dataset into linked data documents
                        await paginator.paginateDataset(companyName, file_name, this.storage);
                        logger.info('Compressing Linked Connections fragments...')
                        // Compress all linked data documents
                        await exec('find . -type f -exec gzip {} +', { cwd: this.storage + '/linked_pages/' + companyName + '/' + file_name });
                        let t1 = (new Date().getTime() - t0) / 1000;
                        logger.info('Dataset conversion for ' + companyName + ' completed successfuly (took ' + t1 + ' seconds)');

                        // Reload GTFS identifiers for RT processing, using new GTFS feed files
                        if (dataset.realTimeData) {
                            logger.info('Updating GTFS identifiers for ' + companyName + '...');
                            // First pause RT job if is already running
                            if (this.jobs[index]['rt_job']) {
                                this.jobs[index]['rt_job'].stop();
                            }
                            await this.loadGTFSIdentifiers(index, dataset);
                            // Start RT job again or create new one if does not exist
                            if (this.jobs[index]['rt_job']) {
                                this.jobs[index]['rt_job'].start();
                            } else {
                                this.launchRTJob(index, dataset);
                            }
                        }
                    } else {
                        logger.error(companyName + " dataset download failed");
                    }
                } catch (err) {
                    logger.error(err);
                }
            },
            start: true
        });

        if (!this.jobs[index]) this.jobs[index] = {};
        this.jobs[index]['static_job'] = static_job;
        logger.info('GTFS job for ' + companyName + ' scheduled correctly');
    }

    loadGTFSIdentifiers(index, dataset) {
        return new Promise(async (resolve, reject) => {
            try {
                let datasets_dir = this.storage + '/datasets/' + dataset.companyName;

                // Delete previous existing GTFS identifier stores
                if(fs.existsSync(datasets_dir + '/.routes') || fs.existsSync(datasets_dir + '/.trips')) {
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
                    logger.warn('There are no GTFS feeds present therefore is not possible to start the GTFS-RT process');
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

                    // Group all connection updates into fragment based arrays
                    for (let x in rtcs) {
                        let jodata = removeDelays(JSON.parse(rtcs[x]));
                        let dt = new Date(jodata.departureTime);
                        //TODO: make this configurable
                        dt.setMinutes(dt.getMinutes() - (dt.getMinutes() % 10));
                        dt.setSeconds(0);
                        dt.setUTCMilliseconds(0);
                        let dt_iso = dt.toISOString();

                        // Add timestamp to RT data for versioning
                        jodata['mementoVersion'] = timestamp.toISOString();
                        let rtdata = JSON.stringify(jodata);

                        if (!rtDataObject[dt_iso]) rtDataObject[dt_iso] = [];
                        rtDataObject[dt_iso].push(rtdata);
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
                    let updData = value.join('\n');
                    let path = this.storage + '/real_time/' + companyName + '/' + key + '.jsonld.gz';

                    if (!fs.existsSync(path)) {
                        written.push(writeFile(path, await gzip(updData, { level: 9 })));
                    } else {
                        let completeData = (await utils.readAndGunzip(path)).join('').concat('\n' + updData);
                        written.push(writeFile(path, await gzip(completeData, { level: 9 })));
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
    dt.setTime(dt.getTime() - (new Duration(jo['departureDelay'].toLowerCase()).milliseconds()));
    at.setTime(at.getTime() - (new Duration(jo['arrivalDelay'].toLowerCase()).milliseconds()));
    jo['departureTime'] = dt.toISOString();
    jo['arrivalTime'] = at.toISOString();
    return jo;
}