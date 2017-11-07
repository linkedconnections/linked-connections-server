const util = require('util');
const fs = require('fs');
const child_process = require('child_process');
const cron = require('cron');
const url = require('url');
const http = require('follow-redirects').http;
const https = require('follow-redirects').https;
const zlib = require('zlib');
const logger = require('../utils/logger');
const utils = require('../utils/utils');
const paginator = require('../paginator/paginator');
const gtfsrt2lc = require('./gtfsrt2lc');

const writeFile = util.promisify(fs.writeFile);
const gzip = util.promisify(zlib.gzip);
const execFile = util.promisify(child_process.execFile);
const exec = util.promisify(child_process.exec);

const config = utils.datasetsConfig;
const datasets = config.datasets;
var storage = config.storage;


module.exports.manageDatasets = () => {
    initContext();
    launchStaticCronJobs(0);
    launchRTCronJobs(0);
}

function initContext() {
    if (storage.endsWith('/')) {
        storage = storage.substring(0, storage.length - 1);
    }

    if (!fs.existsSync(storage + '/tmp')) {
        child_process.execSync('mkdir ' + storage + '/tmp');
    }

    if (!fs.existsSync(storage + '/datasets')) {
        child_process.execSync('mkdir ' + storage + '/datasets');
    }

    if (!fs.existsSync(storage + '/linked_connections')) {
        child_process.execSync('mkdir ' + storage + '/linked_connections');
    }

    if (!fs.existsSync(storage + '/linked_pages')) {
        child_process.execSync('mkdir ' + storage + '/linked_pages');
    }

    if (!fs.existsSync(storage + '/real_time')) {
        child_process.execSync('mkdir ' + storage + '/real_time');
    }
}

// TODO: replace with for loop to make code less confusing
function launchStaticCronJobs(i) {
    if (i < datasets.length) {
        initCompanyContext(datasets[i].companyName);

        new cron.CronJob({
            cronTime: datasets[i].updatePeriod,
            onTick: async () => {
                let t0 = new Date().getTime();
                logger.info('running cron job to update ' + datasets[i].companyName + ' GTFS feed');
                try {
                    let file_name = await downloadDataset(datasets[i]);
                    if (file_name != null) {
                        let dataset = datasets[i];
                        let path = storage + '/datasets/' + dataset.companyName + '/' + file_name;
                        await utils.readAndUnzip(path);
                        logger.info(dataset.companyName + ' Dataset extracted');
                        await setBaseUris(dataset);
                        await convertGTFS2LC(dataset, file_name);
                        logger.info('Initiating Linked Connections fragmentation process for ' + dataset.companyName + '...');
                        await paginator.paginateDataset(dataset.companyName, file_name, storage);
                        logger.info('Compressing Linked Connections fragments...')
                        await exec('find . -type f -exec gzip {} +', { cwd: storage + '/linked_pages/' + dataset.companyName + '/' + file_name });
                        let t1 = (new Date().getTime() - t0) / 1000;
                        logger.info('Dataset conversion for ' + dataset.companyName + ' completed successfuly (took ' + t1 + ' seconds)');
                    } else {
                        logger.error("Dataset download failed");
                    }
                } catch (err) {
                    logger.error(err);
                }
            },
            start: true
        });
        launchStaticCronJobs(i + 1);
    }
}

function launchRTCronJobs(i) {
    if (i < datasets.length && datasets[i].realTimeData) {
        let companyName = datasets[i].companyName;
        if (!fs.existsSync(storage + '/real_time/' + companyName)) {
            child_process.execSync('mkdir ' + storage + '/real_time/' + companyName);
        }

        new cron.CronJob({
            cronTime: datasets[i].realTimeData.updatePeriod,
            onTick: async () => {
                try {
                    // Get RT data dump and convert it to Linked Connections
                    let rtcs = await gtfsrt2lc.processFeed(datasets[i]);
                    // Timestamp that indicates when the data was obtained
                    let timestamp = new Date();
                    // Object to group the updates by fragment 
                    let rtDataObject = {};

                    // Group all connection updates into fragment wise arrays
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
                    await updateRTData(rtDataObject, companyName);
                    let t1 = new Date().getTime();
                    let tf = t1 - timestamp.getTime();
                    logger.info(companyName + ' GTFS-RT feed updated for version ' + timestamp.toISOString() + ' (took ' + tf + ' ms)');
                } catch (err) {
                    logger.error('Error getting GTFS-RT feed for ' + companyName + ': ' + err);
                }
            },
            start: true
        });

        launchRTCronJobs(i + 1);
    }
}

function initCompanyContext(name) {
    if (!fs.existsSync(storage + '/datasets/' + name)) {
        child_process.execSync('mkdir ' + storage + '/datasets/' + name);
    }

    if (!fs.existsSync(storage + '/linked_connections/' + name)) {
        child_process.execSync('mkdir ' + storage + '/linked_connections/' + name);
    }

    if (!fs.existsSync(storage + '/linked_pages/' + name)) {
        child_process.execSync('mkdir ' + storage + '/linked_pages/' + name);
    }
}

function downloadDataset(dataset) {
    const durl = url.parse(dataset.downloadUrl);
    if (durl.protocol == 'https:') {
        const options = {
            hostname: durl.hostname,
            port: 443,
            path: durl.path,
            method: 'GET'
        };

        return download_https(dataset, options);
    } else {
        return download_http(dataset, durl.href);
    }
}

async function setBaseUris(dataset, cb) {
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

    await writeFile(storage + '/datasets/' + dataset.companyName + '/baseUris.json', JSON.stringify(config));
    return Promise.resolve();
}

function convertGTFS2LC(dataset, file_name, cb) {
    return new Promise((resolve, reject) => {
        const child = child_process.spawn('./gtfs2lc.sh', [dataset.companyName, file_name, storage], { cwd: './src/manager', detached: true });
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

function download_https(dataset, options) {
    return new Promise((resolve, reject) => {
        const req = https.request(options, res => {
            let file_name = new Date(res.headers['last-modified']).toISOString();
            let path = storage + '/datasets/' + dataset.companyName + '/' + file_name + '.zip';

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

function download_http(dataset, url) {
    return new Promise((resolve, reject) => {
        const req = http.get(url, res => {
            let file_name = new Date(res.headers['last-modified']).toISOString();
            let path = storage + '/datasets/' + dataset.companyName + '/' + file_name + '.zip';

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

function updateRTData(data, companyName) {
    return new Promise((resolve, reject) => {
        try {
            // Array to store promises for writing fragment files
            let written = [];

            // Update RT fragment files with new data (asynchronously)
            Object.entries(data).forEach(async ([key, value]) => {
                let updData = value.join('\n');
                let path = storage + '/real_time/' + companyName + '/' + key + '.jsonld.gz';

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

function removeDelays(jo) {
    let dt = new Date(jo['departureTime']);
    let at = new Date(jo['arrivalTime']);
    dt.setTime(dt.getTime() - (jo['departureDelay'] * 1000));
    at.setTime(at.getTime() - (jo['arrivalDelay'] * 1000));
    jo['departureTime'] = dt.toISOString();
    jo['arrivalTime'] = at.toISOString();
    return jo;
}