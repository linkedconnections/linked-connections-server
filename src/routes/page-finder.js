const util = require('util');
const express = require('express');
const fs = require('fs');
const zlib = require('zlib');
const logger = require('../utils/logger');
var utils = require('../utils/utils');

const readdir = util.promisify(fs.readdir);
const readfile = util.promisify(fs.readFile);

const router = express.Router();
const datasets_config = utils.datasetsConfig;
const server_config = utils.serverConfig;
var storage = datasets_config.storage;

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

        // Sort versions from the newest to the oldest
        let sorted_versions = utils.sortVersions(new Date(), versions);
        // Find the fragment that covers the requested time (static data)
        let found_fragment = utils.findResource(agency, departureTime.getTime(), sorted_versions);
        let ff = new Date(found_fragment[1]);

        //Redirect client to the apropriate fragment URL
        if (departureTime.getTime() !== found_fragment[1]) {
            res.location('/' + agency + '/connections?departureTime=' + ff.toISOString());
            res.status(302).send();
            return;
        }

        let sf_path = storage + '/linked_pages/' + agency + '/' + found_fragment[0] + '/';

        // Check if RT fragment exists and whether it is compressed or not.
        let rt_exists = false;
        let compressed = false;
        let rt_path = storage + '/real_time/' + agency + '/' + sorted_versions[0] + '/' + utils.getRTDirName(ff) + '/' + ff.toISOString() + '.jsonld';
        if (fs.existsSync(rt_path)) {
            rt_exists = true;
        } else if (fs.existsSync(rt_path + '.gz')) {
            rt_path = rt_path + '.gz';
            rt_exists = true;
            compressed = true;
        }

        // Check if this is a conditional get request, and if so check if we can close this request with a 304
        if (rt_exists) {
            if (utils.handleConditionalGET(req, res, rt_path, departureTime)) {
                return;
            }
        } else {
            if (utils.handleConditionalGET(req, res, sf_path, departureTime)) {
                return;
            }
        }

        // Get respective static data fragment according to departureTime query
        // and complement resource with Real-Time data and Hydra metadata before sending it back to the client
        let buffer = await utils.readAndGunzip(sf_path + ff.toISOString() + '.jsonld.gz');
        let jsonld_graph = buffer.join('').split(',\n').map(JSON.parse);

        // Look if there is real time data for this agency and requested time
        if (rt_exists) {
            let rt_buffer = null;
            let rt_array = [];
            if (compressed) {
                rt_buffer = await utils.readAndGunzip(rt_path);
                // Create an array of all RT updates
                rt_array = rt_buffer.join('').split('\n');
            } else {
                rt_buffer = await readfile(rt_path, 'utf8');
                // Create an array of all RT updates
                rt_array = rt_buffer.split('\n');
            }

            // Path to file that contains the list of connections that shoud be removed from the static fragment due to delays
            let remove_path =  storage + '/real_time/' + agency + '/' + sorted_versions[0] + '/' + utils.getRTDirName(ff) + '/' 
                + ff.toISOString() + '_remove.json';
            if(!fs.existsSync(remove_path) && fs.existsSync(remove_path + '.gz')) {
                remove_path = remove_path + '.gz';
            }
            // Combine static and real-time data
            jsonld_graph = await utils.aggregateRTData(jsonld_graph, rt_array, remove_path, new Date());
        }


        const headers = { 'Content-Type': 'application/ld+json' };
        const params = {
            storage: storage,
            host: host,
            agency: agency,
            departureTime: ff,
            version: found_fragment[0],
            index: found_fragment[2],
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