const util = require('util');
const fs = require('fs');
const accepts = require('accepts');
const Logger = require('../utils/logger');
const StaticData = require('../data/static');
const utils = require('../utils/utils');

const readfile = util.promisify(fs.readFile);

const server_config = utils.serverConfig;
const storage = utils.datasetsConfig.storage;
const logger = Logger.getLogger(server_config.logLevel || 'info');
const mimeTypes = [
    'application/json',
    'application/ld+json',
    'text/turtle',
    'application/n-triples',
    'application/n-quads',
    'application/trig'
];

const staticData = new StaticData(storage, utils.datasetsConfig['datasets']);

// Create static fragments index structure
staticData.updateStaticFragments();

class PageFinder {
    async getConnections(req, res) {
        let t0 = new Date();
        // Check for available updates of the static fragments
        await staticData.updateStaticFragments();
        logger.debug('updateStaticFragments() took ' + (new Date().getTime() - t0.getTime()) + ' ms');

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

        // Accept header
        const accept = accepts(req).type(mimeTypes);

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

        // WARNING: storage should not end on a /
        // Check if there is data for the requested company
        if (!fs.existsSync(storage + '/linked_pages/' + agency)) {
            res.set({ 'Cache-Control': 'no-cache' });
            res.status(404).send("Agency not found");
            return;
        }

        try {
            let versions = Object.keys(staticData.staticFragments[agency]);

            // Check if previous version of resource is being requested through memento protocol
            if (req.headers['accept-datetime'] !== undefined) {
                // verify that the header is valid
                let acceptDatetime = new Date(req.headers['accept-datetime']);
                if (acceptDatetime.toString() === 'Invalid Date') {
                    res.set({ 'Cache-Control': 'no-cache' });
                    res.status(400).send("Invalid accept-datetime header");
                    return;
                }

                // Sort versions list according to the requested version
                let sortedVersions = utils.sortVersions(acceptDatetime, versions);
                // Find closest resource to requested version 
                let closest_version = utils.findResource(agency, departureTime.getTime(), sortedVersions, staticData.staticFragments);
                // Set Memento headers pointng to the found version
                res.location('/' + agency + '/connections/memento?version=' + closest_version[0] + '&departureTime=' + new Date(closest_version[1]).toISOString());
                res.set({
                    'Vary': 'Accept, Accept-Encoding, Accept-Datetime',
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
            t0 = new Date();
            let [static_version, found_fragment, index] = utils.findResource(agency, departureTime.getTime(), sorted_versions, staticData.staticFragments);
            logger.debug('findResource() took ' + (new Date().getTime() - t0.getTime()) + ' ms');
            let ff = new Date(found_fragment);

            // Redirect client to the appropriate fragment URL
            if (departureTime.getTime() !== found_fragment) {
                res.location('/' + agency + '/connections?departureTime=' + ff.toISOString());
                res.status(302).send();
                return;
            }

            let sf_path = storage + '/linked_pages/' + agency + '/' + static_version + '/';
            let rt_exists = false;
            let lowLimit = found_fragment;
            let highLimit = staticData.staticFragments[agency][static_version][index + 1];

            // Get all real-time fragments and remove_files needed to cover the requested static fragment
            t0 = new Date();
            let [rtfs, rtfs_remove] = utils.findRTData(agency, lowLimit, highLimit);
            logger.debug('findRTData() took ' + (new Date().getTime() - t0.getTime()) + ' ms');

            if (rtfs.length > 0) {
                // There are real-time data fragments available for this request
                rt_exists = true;
            }

            // Check if this is a conditional get request, and if so check if we can close this request with a 304
            if (rt_exists) {
                if (utils.handleConditionalGET(agency, req, res, accept, rtfs[rtfs.length - 1], true, departureTime)) {
                    return;
                }
            } else {
                if (utils.handleConditionalGET(agency, req, res, accept, sf_path, false, departureTime)) {
                    return;
                }
            }

            // Get respective static data fragment according to departureTime query
            // and complement resource with Real-Time data and Hydra metadata before sending it back to the client
            t0 = new Date();
            let static_buffer = await utils.readAndGunzip(sf_path + ff.toISOString() + '.jsonld.gz');
            let jsonld_graph = static_buffer.split(',\n').map(JSON.parse);
            logger.debug('Read and process static fragment took ' + (new Date().getTime() - t0.getTime()) + ' ms');

            // Get real time data for this agency and requested time
            if (rt_exists || rtfs_remove.length > 0) {
                let rt_data = [];

                t0 = new Date();
                await Promise.all(rtfs.map(async rt => {
                    let rt_buffer = [];
                    if (rt.indexOf('.gz') > 0) {
                        rt_buffer.push((await utils.readAndGunzip(rt)));
                    } else {
                        rt_buffer.push((await readfile(rt, 'utf8')));
                    }
                    rt_data.push(rt_buffer.toString().split('\n'));
                }));
                logger.debug('Load all RT fragments (' + rtfs.length + ') took ' + (new Date().getTime() - t0.getTime()) + ' ms');

                // Combine static and real-time data
                t0 = new Date();
                logger.debug('-----------aggregateRTData()-----------');
                jsonld_graph = await utils.aggregateRTData(jsonld_graph, rt_data, rtfs_remove, lowLimit, highLimit, now);
                logger.debug('---------------------------------------');
                logger.debug('aggregateRTData() took ' + (new Date().getTime() - t0.getTime()) + ' ms');
            }

            const params = {
                host: host,
                agency: agency,
                departureTime: ff,
                version: static_version,
                index: index,
                data: jsonld_graph,
                staticFragments: staticData.staticFragments
            };

            let final_data = await utils.addHydraMetada(params);

            res.set({ 'Vary': 'Accept, Accept-Encoding' });

            switch (accept) {
                case 'application/json':
                    res.set({ 'Content-Type': 'application/ld+json' });
                    res.json(final_data);
                    break;
                case 'application/ld+json':
                    res.set({ 'Content-Type': 'application/ld+json' });
                    res.json(final_data);
                    break;
                case 'text/turtle':
                    res.set({ 'Content-Type': 'application/trig' });
                    res.status(200).send(await utils.jsonld2RDF(final_data, 'text/turtle'));
                    break;
                case 'application/n-triples':
                    res.set({ 'Content-Type': 'application/n-quads' });
                    res.status(200).send(await utils.jsonld2RDF(final_data, 'application/n-triples'));
                    break;
                case 'application/n-quads':
                    res.set({ 'Content-Type': 'application/n-quads' });
                    res.status(200).send(await utils.jsonld2RDF(final_data, 'application/n-quads'));
                    break;
                case 'application/trig':
                    res.set({ 'Content-Type': 'application/trig' });
                    res.status(200).send(await utils.jsonld2RDF(final_data, 'application/trig'));
                    break;
                default:
                    res.set({ 'Content-Type': 'application/ld+json' });
                    res.json(final_data);
                    break;
            }

            t0 = new Date();
            logger.debug('Add Metadata took ' + (new Date().getTime() - t0.getTime()) + ' ms');

        } catch (err) {
            if (err) {
                logger.error(err);
                console.error(err);
            };
            res.set({ 'Cache-Control': 'no-cache' });
            res.status(404).send();
        }
    }
}

module.exports = PageFinder;