const express = require('express');
const router = express.Router();
const fs = require('fs');
const zlib = require('zlib');
const moment = require('moment-timezone');

const datasets_config = JSON.parse(fs.readFileSync('./datasets_config.json', 'utf8'));
const server_config = JSON.parse(fs.readFileSync('./server_config.json', 'utf8'));

let storage = datasets_config.storage;

router.get('/:agency/connections', function (req, res) {
    let x_forwarded_proto = req.headers['x-forwarded-proto'];
    let protocol = '';
    if (typeof x_forwarded_proto == 'undefined' || x_forwarded_proto == '') {
        if (typeof server_config.protocol == 'undefined' || server_config.protocol == '') {
            protocol = 'http';
        } else {
            protocol = server_config.protocol;
        }
    } else {
        protocol = x_forwarded_proto;
    }

    const host = protocol + '://' + server_config.hostname + '/';
    const agency = req.params.agency;
    const iso = /(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})\.(\d{3})Z/;
    let departureTime = new Date(decodeURIComponent(req.query.departureTime));
    let acceptDatetime = req.headers['accept-datetime'];
    let buffer = [];

    //Redirect to NOW time in case provided date is invalid
    if (!iso.test(req.query.departureTime) || departureTime.toString() === 'Invalid Date') {
        res.location('/' + agency + '/connections?departureTime=' + new Date().toISOString());
        res.set({ 'Access-Control-Allow-Origin': '*' });
        res.status(302).send();

        return;
    }

    //Redirect to proper URL if final / is given before params
    if (req.url.indexOf('connections/') >= 0) {
        res.location('/' + agency + '/connections?departureTime=' + departureTime.toISOString());
        res.set({ 'Access-Control-Allow-Origin': '*' });
        res.status(302).send();

        return;
    }

    //Remove final / from storage path
    if (storage.endsWith('/')) {
        storage = storage.substring(0, storage.length - 1);
    }

    if (fs.existsSync(storage + '/linked_pages/' + agency)) {
        fs.readdir(storage + '/linked_pages/' + agency, (err, versions) => {
            //Check if previous version of resource is been requested
            if (typeof acceptDatetime !== 'undefined') {
                //Sort versions list according to the requested version
                sortVersions(new Date(acceptDatetime), versions, (sortedVersions) => {
                    // Find closest resource to requested version
                    findResource(agency, departureTime, sortedVersions, (last_version) => {
                        if (last_version != null) {
                            //Adjust requested resource to match 10 minutes step format
                            departureTime.setMinutes(departureTime.getMinutes() - (departureTime.getMinutes() % 10));
                            departureTime.setSeconds(0);
                            departureTime.setUTCMilliseconds(0);
                            //Find closest resource
                            while (!fs.existsSync(storage + '/linked_pages/' + agency + '/' + last_version + '/' + departureTime.toISOString() + '.jsonld.gz')) {
                                departureTime.setMinutes(departureTime.getMinutes() - 10);
                            }

                            //Set Memento headers pointng to the found version
                            res.location('/memento/' + agency + '?version=' + last_version + '&departureTime=' + departureTime.toISOString());
                            res.set({
                                'Vary': 'accept-datetime',
                                'Link': '<http://' + server.config.hostname + '/' + agency + '/connections?departureTime=' + departureTime.toISOString() + '>; rel=\"original timegate\"',
                                'Access-Control-Allow-Origin': '*'
                            });

                            //Send HTTP redirect to client
                            res.status(302).send();
                        } else {
                            res.status(404).send();
                        }
                    });
                });

            } else {
                //Find last version containing the requested resource
                findResource(agency, departureTime, versions, (last_version) => {
                    if (last_version != null) {
                        if (departureTime.getMinutes() % 10 != 0 || departureTime.getSeconds() !== 0 || departureTime.getUTCMilliseconds() !== 0) {
                            //Adjust requested resource to match 10 minutes step format
                            departureTime.setMinutes(departureTime.getMinutes() - (departureTime.getMinutes() % 10));
                            departureTime.setSeconds(0);
                            departureTime.setUTCMilliseconds(0);
                            //Find closest resource
                            while (!fs.existsSync(storage + '/linked_pages/' + agency + '/' + last_version + '/' + departureTime.toISOString() + '.jsonld.gz')) {
                                departureTime.setMinutes(departureTime.getMinutes() - 10);
                            }
                            res.location('/' + agency + '/connections?departureTime=' + departureTime.toISOString());
                            res.set({ 'Access-Control-Allow-Origin': '*' });
                            res.status(302).send();
                        } else {
                            if (!fs.existsSync(storage + '/linked_pages/' + agency + '/' + last_version + '/' + departureTime.toISOString() + '.jsonld.gz')) {
                                while (!fs.existsSync(storage + '/linked_pages/' + agency + '/' + last_version + '/' + departureTime.toISOString() + '.jsonld.gz')) {
                                    departureTime.setMinutes(departureTime.getMinutes() - 10);
                                }
                                res.location('/' + agency + '/connections?departureTime=' + departureTime.toISOString());
                                res.set({ 'Access-Control-Allow-Origin': '*' });
                                res.status(302).send();
                            } else {
                                //Complement resource with Real-Time data and Hydra metadata before sending it back to the client 
                                fs.createReadStream(storage + '/linked_pages/' + agency + '/' + last_version + '/' + departureTime.toISOString() + '.jsonld.gz')
                                    .pipe(new zlib.createGunzip())
                                    .on('data', function (data) {
                                        buffer.push(data);
                                    })
                                    .on('end', function () {
                                        var jsonld_graph = buffer.join('').split(',\n');


                                        fs.readdir(storage + '/real_time/' + agency, (err, rt_data) => {
                                            if (!err && typeof rt_data !== 'undefined' && rt_data.length > 0) {
                                                let rt_index = rt_data.length;
                                                (function findRTData() {
                                                    let rt_buffer = [];
                                                    rt_index--;
                                                    fs.createReadStream(storage + '/real_time/' + agency + '/' + rt_data[rt_index])
                                                        .pipe(new zlib.createGunzip())
                                                        .on('data', (data) => {
                                                            rt_buffer.push(data);
                                                        })
                                                        .on('end', () => {
                                                            let rt_capture = rt_buffer.join('').split('\n');
                                                            rt_capture.pop();
                                                            let irtcDate = new Date(JSON.parse(rt_capture[0]).departureTime);
                                                            let frtcDate = new Date(JSON.parse(rt_capture[rt_capture.length - 1]).departureTime);
                                                            let dtplusten = new Date(departureTime.toISOString());
                                                            dtplusten.setMinutes(dtplusten.getMinutes() + 10);

                                                            if (departureTime >= irtcDate && dtplusten < frtcDate) {
                                                                for (let x of rt_capture) {
                                                                    let rtjo = JSON.parse(x);
                                                                    let rtjoDate = new Date(rtjo.departureTime);

                                                                    if (rtjoDate >= departureTime && rtjoDate < dtplusten) {
                                                                        for (let y in jsonld_graph) {
                                                                            let stjo = JSON.parse(jsonld_graph[y]);
                                                                            if (stjo['@id'] === rtjo['@id']) {
                                                                                stjo['departureDelay'] = rtjo['departureDelay'];
                                                                                stjo['arrivalDelay'] = rtjo['arrivalDelay'];
                                                                                jsonld_graph[y] = JSON.stringify(stjo);
                                                                            }
                                                                        }
                                                                    }
                                                                }

                                                                addHydraMetada(departureTime, host, agency, last_version, jsonld_graph, res);
                                                            } else {
                                                                if (rt_index > 0) {
                                                                    findRTData();
                                                                } else {
                                                                    addHydraMetada(departureTime, host, agency, last_version, jsonld_graph, res);
                                                                }
                                                            }
                                                        });
                                                })();
                                            } else {
                                                addHydraMetada(departureTime, host, agency, last_version, jsonld_graph, res);
                                            }
                                        });
                                    });
                            }
                        }
                    } else {
                        res.status(404).send();
                    }
                });
            }
        });
    } else {
        res.status(404).send();
    }
});

function sortVersions(acceptDatetime, versions, cb) {
    let diffs = [];
    let sorted = [];

    for (v of versions) {
        let diff = Math.abs(acceptDatetime.getTime() - new Date(v).getTime());
        diffs.push({ 'version': v, 'diff': diff });
    }

    diffs.sort(function (a, b) { return b.diff - a.diff })

    for (d of diffs) {
        sorted.push(d.version);
    }

    cb(sorted);
}

function findResource(agency, departureTime, versions, cb) {
    var ver = versions.slice(0);

    (function checkVer() {
        var version = ver.splice(ver.length - 1, 1)[0];

        fs.readdir(storage + '/linked_pages/' + agency + '/' + version, (err, pages) => {
            if (err) { cb(null); return }
            if (typeof pages !== 'undefined' && pages.length > 0) {
                let di = new Date(pages[0].substring(0, pages[0].indexOf('.jsonld.gz')));
                let df = new Date(pages[pages.length - 1].substring(0, pages[pages.length - 1].indexOf('.jsonld.gz')));

                if (departureTime >= di && departureTime <= df) {
                    cb(version);
                } else if (ver.length == 0) {
                    cb(null);
                } else {
                    checkVer();
                }
            } else {
                checkVer();
            }
        });
    })();
}

function addHydraMetada(departureTime, host, agency, last_version, jsonld_graph, res) {
    fs.readFile('./statics/skeleton.jsonld', { encoding: 'utf8' }, (err, data) => {
        var jsonld_skeleton = JSON.parse(data);
        jsonld_skeleton['@id'] = host + agency + '/connections?departureTime=' + departureTime.toISOString();
        jsonld_skeleton['hydra:next'] = host + agency + '/connections?departureTime=' + getAdjacentPage(agency + '/' + last_version, departureTime, true);
        jsonld_skeleton['hydra:previous'] = host + agency + '/connections?departureTime=' + getAdjacentPage(agency + '/' + last_version, departureTime, false);
        jsonld_skeleton['hydra:search']['hydra:template'] = host + agency + '/connections/{?departureTime}';

        for (let i in jsonld_graph) {
            jsonld_skeleton['@graph'].push(JSON.parse(jsonld_graph[i]));
        }

        res.set({
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/ld+json'
        });
        res.json(jsonld_skeleton);
    });
}

function getAdjacentPage(path, departureTime, next) {
    var date = new Date(departureTime.toISOString());
    if (next) {
        date.setMinutes(date.getMinutes() + 10);
    } else {
        date.setMinutes(date.getMinutes() - 10);
    }
    while (!fs.existsSync(storage + '/linked_pages/' + path + '/' + date.toISOString() + '.jsonld.gz')) {
        if (next) {
            date.setMinutes(date.getMinutes() + 10);
        } else {
            date.setMinutes(date.getMinutes() - 10);
        }
    }

    return date.toISOString();
}

module.exports = router;