const util = require('util');
const express = require('express');
const fs = require('fs');
const zlib = require('zlib');
const logger = require('../utils/logger');
var utils = require('../utils/utils');

const readdir = util.promisify(fs.readdir);
const readfile = util.promisify(fs.readFile);

const router = express.Router();
const server_config = utils.serverConfig;
var storage = utils.datasetsConfig.storage;

// Create static fragments index structure
utils.updateStaticFragments();

router.get('/:agency/connections', async (req, res) => {
    // Check for available updates of the static fragments
    await utils.updateStaticFragments();

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
    let departureTime = new Date(decodeURIComponent(req.query.departureTime));

    // Redirect to NOW time in case provided date is invalid
    if (departureTime.toString() === 'Invalid Date') {
        // Just save to a variable, a redirect will automatically follow since this won't perfectly resolve to an existing page
        departureTime = new Date();
    }

    // Redirect to proper URL if final / is given before params
    if (req.url.indexOf('connections/') >= 0) {
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
        let versions = Object.keys(utils.staticFragments[agency]);

        // Check if previous version of resource is being requested through memento protocol
        if (req.headers['accept-datetime'] !== undefined) {
            // verify that the header is valid
            let acceptDatetime = new Date(req.headers['accept-datetime']);
            if (acceptDatetime.toString() === 'Invalid Date') {
                res.status(400).send("Invalid accept-datetime header");
                return;
            }

            // Sort versions list according to the requested version
            let sortedVersions = utils.sortVersions(acceptDatetime, versions);
            // Find closest resource to requested version 
            let closest_version = utils.findResource(agency, departureTime.getTime(), sortedVersions);
            // Set Memento headers pointng to the found version
            res.location('/memento/' + agency + '?version=' + closest_version[0] + '&departureTime=' + new Date(closest_version[1]).toISOString());
            res.set({
                'Vary': 'Accept-Encoding, Accept-Datetime',
                'Link': '<' + host + agency + '/connections?departureTime=' + departureTime.toISOString() + '>; rel=\"original timegate\"'
            });
            // Send HTTP redirect to client
            res.status(302).send();
            return;
        }

        // Data is being requested for the current time
        let now = new Date();
        // Sort versions from the newest to the oldest
        let sorted_versions = utils.sortVersions(now, versions);
        // Find the fragment that covers the requested time (static data)
        let [static_version, found_fragment, index] = utils.findResource(agency, departureTime.getTime(), sorted_versions);
        let ff = new Date(found_fragment);

        //Redirect client to the apropriate fragment URL
        if (departureTime.getTime() !== found_fragment) {
            res.location('/' + agency + '/connections?departureTime=' + ff.toISOString());
            res.status(302).send();
            return;
        }

        let sf_path = storage + '/linked_pages/' + agency + '/' + static_version + '/';
        let rt_exists = false;
        let lowLimit = found_fragment;
        let highLimit = utils.staticFragments[agency][static_version][index + 1];

        // Get all real-time fragments and remove_files needed to cover the requested static fragment
        let [rtfs, rtfs_remove] = utils.findRTData(agency, lowLimit, highLimit);
        
        if (rtfs.length > 0) {
            // There are real-time data fragments available for this request
            rt_exists = true;
        }

        // Check if this is a conditional get request, and if so check if we can close this request with a 304
        if (rt_exists) {
            if (utils.handleConditionalGET(req, res, rtfs[rtfs.length - 1], departureTime)) {
                return;
            }
        } else {
            if (utils.handleConditionalGET(req, res, sf_path, departureTime)) {
                return;
            }
        }

        // Get respective static data fragment according to departureTime query
        // and complement resource with Real-Time data and Hydra metadata before sending it back to the client
        let static_buffer = await utils.readAndGunzip(sf_path + ff.toISOString() + '.jsonld.gz');
        let jsonld_graph = static_buffer.join('').split(',\n').map(JSON.parse);

        // Get real time data for this agency and requested time
        if (rt_exists) {
            let rt_data = [];

            await Promise.all(rtfs.map(async rt => {
                let rt_buffer = [];
                if (rt.indexOf('.gz') > 0) {
                    rt_buffer.push((await utils.readAndGunzip(rt)));
                } else {
                    rt_buffer.push((await readfile(rt, 'utf8')));
                }

                rt_data.push(rt_buffer.join('').split('\n'));
            }));

            // Combine static and real-time data
            jsonld_graph = await utils.aggregateRTData(jsonld_graph, rt_data, rtfs_remove, lowLimit, highLimit, now);
        }


        const headers = { 'Content-Type': 'application/ld+json' };
        const params = {
            storage: storage,
            host: host,
            agency: agency,
            departureTime: ff,
            version: static_version,
            index: index,
            data: jsonld_graph,
            http_headers: headers,
            http_response: res
        };

        await utils.addHydraMetada(params);

    } catch (err) {
        if (err) logger.error(err);
        res.status(404).send();
    }

});

module.exports = router;