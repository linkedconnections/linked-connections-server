const fs = require('fs');
const child_process = require('child_process');
const cron = require('cron');
const url = require('url');
const http = require('http');
const https = require('https');
const unzip = require('unzip');
const paginator = require('../paginator/paginator');

const datasets = JSON.parse(fs.readFileSync('./datasets_config.json', 'utf8')).datasets;

module.exports.manageDatasets = function () {
    initContext();
    launchCronJobs(0);
}

function initContext() {
    if (!fs.existsSync('./datasets')) {
        child_process.execSync('mkdir ./datasets');
    }

    if (!fs.existsSync('./linked_connections')) {
        child_process.execSync('mkdir ./linked_connections');
    }

    if (!fs.existsSync('./linked_pages')) {
        child_process.execSync('mkdir ./linked_pages');
    }
}

function launchCronJobs(i) {
    if (i < datasets.length) {
        initCompanyContext(datasets[i].companyName);

        new cron.CronJob({
            cronTime: datasets[i].updatePeriod,
            onTick: function () {
                console.log('runnig cron job for ' + datasets[i].companyName);
                downloadDataset(datasets[i], function (dataset, file_name) {
                    if (dataset) {
                        console.log('starting pagination of new ' + dataset.companyName + ' dataset...');
                        processDataset(dataset, file_name);
                    }
                });
            },
            start: true
        });
        launchCronJobs(i + 1);
    }
}

function initCompanyContext(name) {
    if (!fs.existsSync('./datasets/' + name)) {
        child_process.execSync('mkdir ./datasets/' + name);
    }

    if (!fs.existsSync('./linked_connections/' + name)) {
        child_process.execSync('mkdir ./linked_connections/' + name);
    }

    if (!fs.existsSync('./linked_pages/' + name)) {
        child_process.execSync('mkdir ./linked_pages/' + name);
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

            if (!fs.existsSync('./datasets/' + dataset.companyName + '/' + file_name + '.zip')) {
                var wf = fs.createWriteStream('./datasets/' + dataset.companyName + '/' + file_name + '.zip', { encoding: 'base64' });

                res.on('data', (d) => {
                    wf.write(d);
                }).on('end', function () {
                    wf.end();
                    cb(dataset, file_name);
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
            if (!fs.existsSync('./datasets/' + dataset.companyName + '/' + file_name + '.zip')) {
                var wf = fs.createWriteStream('./datasets/' + dataset.companyName + '/' + file_name + '.zip', { encoding: 'base64' });

                res.on('data', (d) => {
                    wf.write(d);
                }).on('end', () => {
                    wf.end();
                    cb(dataset, file_name);
                });
            } else {
                cb();
            }
        });
    }
}

function processDataset(dataset, file_name) {
    fs.createReadStream('./datasets/' + dataset.companyName + '/' + file_name + '.zip')
        .pipe(unzip.Extract({ path: './datasets/' + dataset.companyName + '/' + file_name + '_tmp' }))
        .on('close', function () {
            console.log('Dataset extracted for ' + dataset.companyName);
            setBaseUris(dataset, (err) => {
                if (err) {
                    console.error('ERROR: ' + err);
                } else {
                    executeShellScript(dataset, file_name, function (err, msg, dataset, file_name) {
                        if (err) {
                            console.error('ERROR: ' + err);
                        } else {
                            console.log(msg);
                            paginator.paginateDataset(dataset.companyName, file_name,
                                function () {
                                    child_process.exec('gzip *', { cwd: './linked_pages/' + dataset.companyName + '/' + file_name }, function () {
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
    let uri = dataset.baseURI;
    if (typeof uri == 'undefined' || uri == '') {
        uri = 'http://example.org/';
    }

    if (!uri.endsWith('/')) {
        uri = uri + '/';
    }

    let config = {
        'stops': uri + 'stops/',
        'connections': uri + 'connections/',
        'trips': uri + 'trips/',
        'routes': uri + 'routes/'
    }

    fs.writeFile('./datasets/' + dataset.companyName + '/baseUris.json', JSON.stringify(config), function (err) {
        if (err) {
            cb(err);
        } else {
            cb();
        }
    });
}

function executeShellScript(dataset, file_name, cb) {
    child_process.exec('./gtfs2lc.sh ' + dataset.companyName + ' ' + file_name, { cwd: './src/manager' }, function (err, stdout, stderr) {
        if (err != null) {
            return cb(new Error(err), null);
        } else if (typeof (stderr) != "string") {
            return cb(new Error(stderr), null);
        } else {
            return cb(null, stdout, dataset, file_name);
        }
    });
}
