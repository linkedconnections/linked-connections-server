const util = require('util');
const express = require('express');
const fs = require('fs');
const zlib = require('zlib');
const logger = require('../utils/logger');
const utils = require('../utils/utils');

const readdir = util.promisify(fs.readdir);

const router = express.Router();
const datasets_config = utils.datasetsConfig;
const server_config = utils.serverConfig;
var storage = datasets_config.storage;

router.get('/:agency/connections', async (req, res) => {
    // Allow requests from different hosts
    res.set({ 'Access-Control-Allow-Origin': '*' });

    // Determine protocol (i.e. HTTP or HTTPS)
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
    let acceptDatetime = new Date(req.headers['accept-datetime']);

    // Redirect to NOW time in case provided date is invalid
    if (!iso.test(req.query.departureTime) || departureTime.toString() === 'Invalid Date') {
        res.location('/' + agency + '/connections?departureTime=' + new Date().toISOString());
        res.status(302).send();
        return;
    }

    // Redirect to proper URL if final / is given before params
    if (req.url.indexOf('connections/') >= 0) {
        res.location('/' + agency + '/connections?departureTime=' + departureTime.toISOString());
        res.status(302).send();
        return;
    }

    // Remove final / from storage path
    if (storage.endsWith('/')) {
        storage = storage.substring(0, storage.length - 1);
    }

    let lp_path = storage + '/linked_pages/' + agency;

    // Check if there is data for the requested company
    if (fs.existsSync(lp_path)) {
        try {
            let versions = await readdir(lp_path);
            // Check if previous version of resource is being requested through memento protocol
            if (acceptDatetime.toString() !== 'Invalid Date') {
                // Sort versions list according to the requested version
                let sortedVersions = sortVersions(acceptDatetime, versions);
                // Find closest resource to requested version
                let closest_version = await findResource(agency, departureTime, sortedVersions);
                // TODO: Make this configurable!!
                // Adjust requested resource to match 10 minutes step format
                departureTime.setMinutes(departureTime.getMinutes() - (departureTime.getMinutes() % 10));
                departureTime.setSeconds(0);
                departureTime.setUTCMilliseconds(0);
                // Find closest resource (static data)
                while (!fs.existsSync(lp_path + '/' + closest_version + '/' + departureTime.toISOString() + '.jsonld.gz')) {
                    departureTime.setMinutes(departureTime.getMinutes() - 10);
                }
                // Set Memento headers pointng to the found version
                res.location('/memento/' + agency + '?version=' + closest_version + '&departureTime=' + departureTime.toISOString());
                res.set({
                    'Vary': 'accept-datetime',
                    'Link': '<' + host + agency + '/connections?departureTime=' + departureTime.toISOString() + '>; rel=\"original timegate\"'
                });
                // Send HTTP redirect to client
                res.status(302).send();
            } else {
                // Find last version containing the requested resource (static data)
                let last_version = await findResource(agency, departureTime, versions);
                let lv_path = storage + '/linked_pages/' + agency + '/' + last_version + '/';
                if (departureTime.getMinutes() % 10 != 0 || departureTime.getSeconds() !== 0 || departureTime.getUTCMilliseconds() !== 0) {
                    // TODO: Make this configurable!!
                    // Adjust requested resource to match 10 minutes step format
                    departureTime.setMinutes(departureTime.getMinutes() - (departureTime.getMinutes() % 10));
                    departureTime.setSeconds(0);
                    departureTime.setUTCMilliseconds(0);
                    // Find closest resource (static data)
                    while (!fs.existsSync(lv_path + departureTime.toISOString() + '.jsonld.gz')) {
                        departureTime.setMinutes(departureTime.getMinutes() - 10);
                    }
                    res.location('/' + agency + '/connections?departureTime=' + departureTime.toISOString());
                    res.status(302).send();
                } else {
                    if (!fs.existsSync(lv_path + departureTime.toISOString() + '.jsonld.gz')) {
                        while (!fs.existsSync(lv_path + departureTime.toISOString() + '.jsonld.gz')) {
                            departureTime.setMinutes(departureTime.getMinutes() - 10);
                        }
                        res.location('/' + agency + '/connections?departureTime=' + departureTime.toISOString());
                        res.status(302).send();
                    } else {
                        // Get respective static data fragment according to departureTime query
                        // and complement resource with Real-Time data and Hydra metadata before sending it back to the client
                        let buffer = await utils.readAndGunzip(lv_path + departureTime.toISOString() + '.jsonld.gz');
                        let jsonld_graph = buffer.join('').split(',\n').map(JSON.parse);
                        let rt_path = storage + '/real_time/' + agency + '/' + departureTime.toISOString() + '.jsonld.gz';

                        // Look if there is real time data for this agency and requested time
                        if (fs.existsSync(rt_path)) {
                            let rt_buffer = await utils.readAndGunzip(rt_path);
                            // Create an array of all RT updates
                            let rt_array = rt_buffer.join('').split('\n').map(JSON.parse);
                            // Combine static and real-time data
                            jsonld_graph = utils.aggregateRTData(jsonld_graph, rt_array, new Date());
                        }

                        // Finally build a JSON-LD document containing the data and return it to the client
                        const headers = { 'Content-Type': 'application/ld+json' };
                        const params = {
                            storage: storage,
                            host: host,
                            agency: agency,
                            departureTime: departureTime,
                            version: last_version,
                            data: jsonld_graph,
                            http_headers: headers,
                            http_response: res
                        }
                        utils.addHydraMetada(params);
                    }
                }
            }
        } catch (err) {
            if (err) logger.error(err);
            res.status(404).send();
        }
    } else {
        res.status(404).send();
    }
});

function sortVersions(acceptDatetime, versions) {
    let diffs = [];
    let sorted = [];

    for (v of versions) {
        let diff = Math.abs(acceptDatetime.getTime() - new Date(v).getTime());
        diffs.push({ 'version': v, 'diff': diff });
    }

    diffs.sort((a, b) => {
        return b.diff - a.diff;
    });

    for (d of diffs) {
        sorted.push(d.version);
    }

    return sorted;
}

function findResource(agency, departureTime, versions, cb) {
    return new Promise((resolve, reject) => {
        let ver = versions.slice(0);
        (async function checkVer() {
            try {
                let version = ver.splice(ver.length - 1, 1)[0];
                let pages = await readdir(storage + '/linked_pages/' + agency + '/' + version);
                if (typeof pages !== 'undefined' && pages.length > 0) {
                    let di = new Date(pages[0].substring(0, pages[0].indexOf('.jsonld.gz')));
                    let df = new Date(pages[pages.length - 1].substring(0, pages[pages.length - 1].indexOf('.jsonld.gz')));
                    if (departureTime >= di && departureTime <= df) {
                        resolve(version);
                    } else if (ver.length === 0) {
                        reject();
                    } else {
                        checkVer();
                    }
                } else {
                    checkVer();
                }
            } catch (err) {
                reject(err);
            }
        })();
    });
}

module.exports = router;