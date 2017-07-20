const fs = require('fs');
const child_process = require('child_process');
const cron = require('cron');
const url = require('url');
const http = require('follow-redirects').http;
const https = require('follow-redirects').https;
const unzip = require('unzip');
const paginator = require('../paginator/paginator');
const gtfsrt2lc = require('./gtfsrt2lc');

const config = JSON.parse(fs.readFileSync('./datasets_config.json', 'utf8'));
const datasets = config.datasets;
const storage = config.storage;

module.exports.manageDatasets = function () {
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

function launchStaticCronJobs(i) {
    if (i < datasets.length) {
        initCompanyContext(datasets[i].companyName);

        new cron.CronJob({
            cronTime: datasets[i].updatePeriod,
            onTick: function () {
                console.log('runnig cron job to update ' + datasets[i].companyName + ' GTFS feed');
                downloadDataset(datasets[i], (dataset, file_name) => {
                    if (dataset) {
                        console.log('starting pagination of new ' + dataset.companyName + ' dataset...');
                        processDataset(dataset, file_name);
                    }
                });
            },
            start: true
        });
        launchStaticCronJobs(i + 1);
    }
}

function launchRTCronJobs(i) {
    if (i < datasets.length && datasets[i].realTimeData) {
        if (!fs.existsSync(storage + '/real_time/' + datasets[i].companyName)) {
            child_process.execSync('mkdir ' + storage + '/real_time/' + datasets[i].companyName);
        }

        new cron.CronJob({
            cronTime: datasets[i].realTimeData.updatePeriod,
            onTick: function () {
                console.log('Updating ' + datasets[i].companyName + ' GTFS-RT feed');
                gtfsrt2lc.processFeed(datasets[i], (error, rtcs) => {
                    if (!error) {
                        let date = new Date();
                        date.setUTCMilliseconds(0);

                        let fileName = date.toISOString();
                        let file = fs.createWriteStream(storage + '/real_time/' + datasets[i].companyName + '/' + fileName + '.jsonld');

                        for (let i = 0; i < rtcs.length; i++) {
                            if (i === 0) {
                                file.write(rtcs[i]);
                            } else {
                                file.write('\n' + rtcs[i]);
                            }
                        }
                        file.end();

                        file.on('finish', () => {
                            child_process.exec('./gtfsrt2lc.sh ' + datasets[i].companyName + ' ' + fileName + ' ' + storage, { cwd: './src/manager' }, (e, sto, ste) => {
                                child_process.exec('gzip ' + fileName + '.jsonld', { cwd: storage + '/real_time/' + datasets[i].companyName }, function () {
                                    console.log('GTFS-RT feed processed for ' + datasets[i].companyName);
                                });
                            });
                        });
                    } else {
                        console.error('Error getting GTFS-RT feed for ' + datasets[i].companyName + ': ' + error);
                    }
                });
            },
            start: true
        });
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

function downloadDataset(dataset, cb) {
    const durl = url.parse(dataset.downloadUrl);
    if (durl.protocol == 'https:') {

        const options = {
            hostname: durl.hostname,
            port: 443,
            path: durl.path,
            method: 'GET'
        };

        const req = https.request(options, (res) => {
            var file_name = new Date(res.headers['last-modified']).toISOString();

            if (!fs.existsSync(storage + '/datasets/' + dataset.companyName + '/' + file_name + '.zip')) {
                var wf = fs.createWriteStream(storage + '/datasets/' + dataset.companyName + '/' + file_name + '.zip', { encoding: 'base64' });

                res.on('data', (d) => {
                    wf.write(d);
                }).on('end', function () {
                    wf.end();
                    wf.on('finish', () => {
                        cb(dataset, file_name);
                    });
                });
            } else {
                cb();
            }
        });

        req.on('error', (e) => {
            console.error(e);
        });
        req.end();
    } else {
        const req = http.get(durl.href, function (res) {
            var file_name = new Date(res.headers['last-modified']).toISOString();
            if (!fs.existsSync(storage + '/datasets/' + dataset.companyName + '/' + file_name + '.zip')) {
                var wf = fs.createWriteStream(storage + '/datasets/' + dataset.companyName + '/' + file_name + '.zip', { encoding: 'base64' });

                res.on('data', (d) => {
                    wf.write(d);
                }).on('end', () => {
                    wf.end();
                    wf.on('finish', () => {
                        cb(dataset, file_name);
                    });
                });
            } else {
                cb();
            }
        });
    }
}

function processDataset(dataset, file_name) {
    fs.createReadStream(storage + '/datasets/' + dataset.companyName + '/' + file_name + '.zip')
        .pipe(unzip.Extract({ path: storage + '/datasets/' + dataset.companyName + '/' + file_name + '_tmp' }))
        .on('close', function () {
            console.log(dataset.companyName + ' Dataset extracted');
            setBaseUris(dataset, (err) => {
                if (err) {
                    console.error('ERROR: ' + err);
                } else {
                    executeShellScript(dataset, file_name, function (err, msg, dataset, file_name) {
                        if (err) {
                            console.error('ERROR: ' + err);
                        } else {
                            console.log(msg);
                            paginator.paginateDataset(dataset.companyName, file_name, storage,
                                function () {
                                    child_process.exec('find . -type f -exec gzip {} +', { cwd: storage + '/linked_pages/' + dataset.companyName + '/' + file_name }, function () {
                                        console.log('Pagination for ' + dataset.companyName + ' dataset completed!!');
                                    });
                                });
                        }
                    });
                }
            });
        });
}

function setBaseUris(dataset, cb) {
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

    fs.writeFile(storage + '/datasets/' + dataset.companyName + '/baseUris.json', JSON.stringify(config), function (err) {
        if (err) {
            cb(err);
        } else {
            cb();
        }
    });
}

function executeShellScript(dataset, file_name, cb) {
    child_process.exec('./gtfs2lc.sh ' + dataset.companyName + ' ' + file_name + ' ' + storage, { cwd: './src/manager' }, function (err, stdout, stderr) {
        if (err != null) {
            return cb(new Error(err), null);
        } else if (typeof (stderr) != "string") {
            return cb(new Error(stderr), null);
        } else {
            return cb(null, stdout, dataset, file_name);
        }
    });
}
