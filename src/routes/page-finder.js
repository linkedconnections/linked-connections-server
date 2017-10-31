const util = require('util');
const express = require('express');
const fs = require('fs');
const zlib = require('zlib');
const md5 = require('md5');
const logger = require('../utils/logger');
const utils = require('../utils/utils');

const readdir = util.promisify(fs.readdir);

const router = express.Router();
const datasets_config = utils.datasetsConfig;
const server_config = utils.serverConfig;
let storage = datasets_config.storage;

router.get('/:agency/connections', async (req, res) => {
    // Allow requests from different hosts
    res.set({'Access-Control-Allow-Origin': '*'});

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
                    'Vary': 'Accept-Encoding, Accept-Datetime',
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
                        let jsonld_graph = buffer.join('').split(',\n');
                        let rt_path = storage + '/real_time/' + agency + '/' + departureTime.toISOString() + '.jsonld.gz';

                        // Look if there is real time data for this agency and requested time
                        if (fs.existsSync(rt_path)) {

                            if (handleConditionalGET(req, res, rt_path)) {
                                return;
                            }

                            let rt_buffer = await utils.readAndGunzip(rt_path);
                            // Create an array of all RT updates
                            let rt_array = rt_buffer.join('').split('\n');
                            // Create an indexed Map object for connection IDs and position in the RT updates array
                            // containing the last Connection updates for a given moment
                            let rt_map = utils.getIndexedMap(rt_array, new Date());
                            // Proceed to apply updates (if there is any) for the given criteria 
                            if (rt_map.size > 0) {
                                for (let i = 0; i < jsonld_graph.length; i++) {
                                    let jo = JSON.parse(jsonld_graph[i]);
                                    if (rt_map.has(jo['@id'])) {
                                        let update = JSON.parse(rt_array[rt_map.get(jo['@id'])]);
                                        jo['departureDelay'] = update['departureDelay'];
                                        jo['arrivalDelay'] = update['arrivalDelay'];
                                        jsonld_graph[i] = JSON.stringify(jo);
                                    }
                                }
                            }
                        } else {
                            if (handleConditionalGET(req, res, lv_path)) {
                                return;
                            }
                        }

                        const headers = {'Content-Type': 'application/ld+json'};
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

/**
 * Checks for Conditional GET requests, by comparing the last modified date and file hash against if-modified-since and if-none-match headers.
 * If a match is made, a 304 response is sent and the function will return true.
 * If the client needs a new version (ie a body should be sent), the function will return false
 * @param req
 * @param res
 * @param filepath The path of the file which would be used to generate the response
 * @returns boolean True if a 304 response was served
 */
function handleConditionalGET(req, res, filepath) {
    let ifModifiedSinceRawHeader = req.header('if-modified-since');

    let ifModifiedSinceHeader = undefined;
    if (ifModifiedSinceRawHeader !== undefined) {
        ifModifiedSinceHeader = new Date(ifModifiedSinceRawHeader);
    }

    let ifNoneMatchHeader = req.header('if-none-match');

    let stats = fs.statSync(filepath);
    let lastModifiedDate = new Date(util.inspect(stats.mtime));

    let etag = 'W/"' + md5(filepath + lastModifiedDate) + '"';
    res.set({'Cache-control': 'public, s-maxage=5, max-age=15, must-revalidate'});
    res.set({'Vary': 'Accept-encoding'});
    res.set({'Last-Modified': lastModifiedDate.toISOString()});
    res.set({'ETag': etag});

    // If an if-modified-since header exists, and if the realtime data hasn't been updated since, just return a 304/
    // If an if-none-match header exists, and if the realtime data hasn't been updated since, just return a 304/
    if ((lastModifiedDate !== undefined && ifModifiedSinceHeader !== undefined && ifModifiedSinceHeader >= lastModifiedDate) ||
        (ifNoneMatchHeader !== undefined && ifNoneMatchHeader === etag)) {
        res.status(304).send();
        return true;
    }

    return false;
}

function sortVersions(acceptDatetime, versions) {
    let diffs = [];
    let sorted = [];

    for (v of versions) {
        let diff = Math.abs(acceptDatetime.getTime() - new Date(v).getTime());
        diffs.push({'version': v, 'diff': diff});
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