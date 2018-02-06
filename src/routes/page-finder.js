const util = require('util');
const express = require('express');
const fs = require('fs');
const zlib = require('zlib');
const md5 = require('md5');
const logger = require('../utils/logger');
const utils = require('../utils/utils');

const readdir = util.promisify(fs.readdir);
const readfile = util.promisify(fs.readFile);

const router = express.Router();
const datasets_config = utils.datasetsConfig;
const server_config = utils.serverConfig;
let storage = datasets_config.storage;

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

    // Redirect to NOW time in case provided date is invalid
    if (!iso.test(req.query.departureTime) || departureTime.toString() === 'Invalid Date') {
        // Just save to a variable, a redirect will automatically follow since this won't perfectly resolve to an existing page
        departureTime = new Date();
    }

    // Redirect to proper URL if final / is given before params
    if (req.url.indexOf('connections/') >= 0) {
        res.location('/' + agency + '/connections?departureTime=' + departureTime.toISOString());
        res.status(302).send();
        return;
    }

    // Redirect client to the correct page according to fragment format
    if (departureTime.getMinutes() % 10 != 0 || departureTime.getSeconds() !== 0 || departureTime.getUTCMilliseconds() !== 0) {
        // TODO: Make this configurable!!
        // Adjust requested resource to match 10 minutes fragment format
        departureTime.setMinutes(departureTime.getMinutes() - (departureTime.getMinutes() % 10));
        departureTime.setSeconds(0);
        departureTime.setUTCMilliseconds(0);
        
        res.location('/' + agency + '/connections?departureTime=' + departureTime.toISOString());
        res.status(302).send();
        return;
    }

    // WARNING: storage should not end on a /.
    let lp_path = storage + '/linked_pages/' + agency;

    // Check if there is data for the requested company
    if (!fs.existsSync(lp_path)) {
        res.status(404).send("Agency not found");
        return;
    }
    
    try {
        let versions = await readdir(lp_path);
        
        // Check if previous version of resource is being requested through memento protocol
        if (req.headers['accept-datetime'] !== undefined) {
            // verify if the header is valid
            let acceptDatetime = new Date(req.headers['accept-datetime']);
            if (acceptDatetime.toString() === 'Invalid Date') {
                res.status(400).send("Invalid accept-datetime header");
                return;
            }

            // Sort versions list according to the requested version
            let sortedVersions = sortVersions(acceptDatetime, versions);
            // Find closest resource to requested version
            let closest_version = await findResource(agency, departureTime, sortedVersions);
            // Set Memento headers pointng to the found version
            res.location('/memento/' + agency + '?version=' + closest_version + '&departureTime=' + departureTime.toISOString());
            res.set({
                'Vary': 'Accept-Encoding, Accept-Datetime',
                'Link': '<' + host + agency + '/connections?departureTime=' + departureTime.toISOString() + '>; rel=\"original timegate\"'
            });
            // Send HTTP redirect to client
            res.status(302).send();
            return;
        }

        // Find last version containing the requested resource (static data)
        let last_version = findResource(agency, departureTime, versions);
        let lv_path = storage + '/linked_pages/' + agency + '/' + last_version + '/';

        // Check if RT fragment exists and whether it is compressed or not.
        let rt_exists = false;
        let compressed = false;
        let rt_path = storage + '/real_time/' + agency + '/' + utils.getRTDirName(departureTime) + '/'
            + departureTime.toISOString() + '.jsonld';
        if (fs.existsSync(rt_path)) {
            rt_exists = true;
        } else if (fs.existsSync(rt_path + '.gz')) {
            rt_path = rt_path + '.gz';
            rt_exists = true;
            compressed = true;
        }

        // Check if this is a conditional get request, and if so, if we can close this request
        // Do this before heavy work like unzipping, this response needs to be FAST!
        if (rt_exists) {
            if (handleConditionalGET(req, res, rt_path, departureTime)) {
                return;
            }
        } else {
            if (handleConditionalGET(req, res, lv_path, departureTime)) {
                return;
            }
        }

        // Get respective static data fragment according to departureTime query
        // and complement resource with Real-Time data and Hydra metadata before sending it back to the client
        let buffer = await utils.readAndGunzip(lv_path + departureTime.toISOString() + '.jsonld.gz');
        let jsonld_graph = buffer.join('').split(',\n').map(JSON.parse);
        
        // Look if there is real time data for this agency and requested time
        if (rt_exists) {
            let rt_buffer = null;
            let rt_array = [];
            if (compressed) {
                rt_buffer = await utils.readAndGunzip(rt_path);
                // Create an array of all RT updates
                rt_array = rt_buffer.join('').split('\n').map(JSON.parse);
            } else {
                rt_buffer = await readfile(rt_path, 'utf8');
                // Create an array of all RT updates
                rt_array = rt_buffer.split('\n').map(JSON.parse);
            }

            // Combine static and real-time data
            jsonld_graph = utils.aggregateRTData(jsonld_graph, rt_array, agency, departureTime, new Date());
        }


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
        };

        utils.addHydraMetada(params);

    } catch (err) {
        if (err) logger.error(err);
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
 * @param departureTime The time for which this document was requested
 * @returns boolean True if a 304 response was served
 */
function handleConditionalGET(req, res, filepath, departureTime) {
    let ifModifiedSinceRawHeader = req.header('if-modified-since');

    let ifModifiedSinceHeader = undefined;
    if (ifModifiedSinceRawHeader !== undefined) {
        ifModifiedSinceHeader = new Date(ifModifiedSinceRawHeader);
    }

    let ifNoneMatchHeader = req.header('if-none-match');

    let stats = fs.statSync(filepath);
    let lastModifiedDate = new Date(util.inspect(stats.mtime));

    let now = new Date();

    // Valid until :31 or :01
    let validUntilDate = new Date();
    validUntilDate.setSeconds(validUntilDate.getSeconds() - (validUntilDate.getSeconds() % 30) + 31);

    let maxage = (validUntilDate - now) / 1000;
    let etag = 'W/"' + md5(filepath + lastModifiedDate) + '"';

    // If both departure time and last modified lie resp. 2 and 1 hours in the past, this becomes immutable
    if (departureTime < (now - 7200000) && lastModifiedDate < (now - 3600000)) {
        // Immutable (for browsers which support it, sometimes limited to https only
        // 1 year expiry date to keep it long enough in cache for the others
        res.set({ 'Cache-control': 'public, max-age=31536000000, immutable' });
        res.set({ 'Expires': new Date(now + 31536000000).toUTCString() });
    } else {
        // Let clients hold on to this data for 1 second longer than nginx. This way nginx can update before the clients?
        res.set({ 'Cache-control': 'public, s-maxage=' + maxage + ', max-age=' + (maxage + 1) + ',stale-if-error=' + (maxage + 15) + ', proxy-revalidate' });
        res.set({ 'Expires': validUntilDate.toUTCString() });
    }

    res.set({ 'ETag': etag });
    res.set({ 'Vary': 'Accept-encoding, Accept-Datetime' });
    res.set({ 'Last-Modified': lastModifiedDate.toUTCString() });
    res.set({ 'Content-Type': 'application/ld+json' });

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

function findResource(agency, departureTime, versions) {
    let version = null;
    for(let i = versions.length - 1; i >= 0; i--) {
        if(fs.existsSync(storage + '/linked_pages/' + agency + '/' + versions[i] + '/' + departureTime.toISOString() + '.jsonld.gz')) {
            version = versions[i];
            break;
        }
    }

    if(version !== null) {
        return version;
    } else {
        throw new Error();
    }
}

module.exports = router;