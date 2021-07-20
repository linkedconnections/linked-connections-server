const util = require('util');
const fs = require('fs');
const utils = require('../utils/utils');
const accepts = require('accepts');
const ConnectionsFeed = require('../data/connections-feed');
const StaticData = require('../data/static');
const RealTimeData = require('../data/real-time');

const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);
const writeFile = util.promisify(fs.writeFile);

const logger = Logger.getLogger(server_config.logLevel || 'info');
const storage = utils.datasetsConfig.storage;
const mimeTypes = [
    'application/json',
    'application/ld+json',
    'text/turtle',
    'application/n-triples',
    'application/n-quads',
    'application/trig'
];

let feedTrees = {};
const staticData = new StaticData(storage, utils.datasetsConfig['datasets']);
const RTData = new RealTimeData(storage, utils.datasetsConfig['datasets']);

class Feed {
    constructor() {
        this._storage = utils.datasetsConfig.storage;
        this._datasets = utils.datasetsConfig.datasets;
        this._serverConfig = utils.serverConfig;
    }

    async init() {
        // Create static fragments index structure
        await staticData.updateStaticFragments(); // TODO check if this can be done concurrently with initRTFragments()
        await RTData.initRTFragments();
        // Create an AVL tree-based index of Connections for every data source
        for (const d of this.datasets) {
            const tree = new ConnectionsFeed({ staticData: staticData, RTData: RTData, agency: d['companyName'] });
            await tree.init();
            feedTrees[d['companyName']] = tree;
        }
    }

    async getFeed(req, res) {
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

        // Request path and query parameters
        const agency = req.params.agency;
        let created = new Date(decodeURIComponent(req.query.created));
        let version = new Date(decodeURIComponent(req.query.version));

        // WARNING: storage should not end on a /
        // TODO: use path to avoid OS compatibility issues and handle the trailing slash
        // Check if there is data for the requested company
        if (!fs.existsSync(storage + '/feed_history/' + agency)) {
            res.set({ 'Cache-Control': 'immutable' });
            res.status(404).send("Agency not found");
            return;
        }

        // Accept header
        const accept = accepts(req).type(mimeTypes);

        // Redirect to NOW time in case provided date is invalid
        if (created.toString() === 'Invalid Date') {
            // Just save to a variable, a redirect will automatically follow since this won't perfectly resolve to an existing page
            created = new Date();
        }

        // Redirect to proper URL if final / is given before params
        if (req.url.indexOf('connections/feed/') >= 0) {
            res.location(`/${agency}/connections/feed?created=${created.toISOString()}`);
            res.status(302).send();
            return;
        }

        try {
            // Find the fragment that covers the requested time
            let t0 = new Date();

            // search the closest fragment to the left of the 'created' parameter (first in RT, then in static files
            // if necessary)
            let found_fragment, index, is_static;
            let result = utils.findRTResource(agency, created, RTData.RTFragments);
            if (result === null) {
                found_fragment = result[0];
                index = result[1];
                is_static = false;
            } else {
                // look in the static files
                let versions = Object.keys(staticData.staticFragments[agency]).sort();
                // TODO
            }
            logger.debug('finding fragment took ' + (new Date().getTime() - t0.getTime()) + ' ms');


            let found_fragment_date = new Date(found_fragment);

            // Redirect client to the appropriate fragment URL
            if (created.getTime() !== found_fragment) {
                res.location('/' + agency + '/connections/feed?created=' + found_fragment_date.toISOString());
                res.status(302).send();
                return;
            }

            let tree = feedTrees[agency].avlTree;
            // check if fragment is in AVL tree
            if (created.getTime() >= tree.min() && created.getTime() < tree.max()) {
                let node = tree.find(created.getTime());
            }
            else {
                // if it is not in the AVL tree, get it from the directory.

                let rt_min = RTData.RTFragments[agency][0];
                let rt_max = RTData.RTFragments[agency][RTData.RTFragments[agency].length - 1];
                let static_min = staticData.staticFragments[agency][static_version][0];
                // let static_max = staticData.staticFragments[agency][static_version][staticData.staticFragments[agency][static_version].length - 1];

                // this fragment can either be a real-time fragment from 'feed_history', or a static fragment from
                // 'linked_pages'.
                if (created.getTime() >= rt_min && created.getTime() <= rt_max) {
                    // the fragment is in 'feed_history'

                } else if (created.getTime() >= static_min) {
                    // the fragment is in 'linked_pages'

                } else {
                    // fragment cannot be found, so send 404
                    res.status(404).send();
                    return;
                }

            }






            // Array that will contain all the Connections of this fragment
            let jsonldGraph = [];
            let lowLimit = found_fragment;
            let highLimit = staticData.staticFragments[agency][static_version][index + 1];

            // Check if the in-memory AVL Tree contains this fragment
            if (departureTime.getTime() >= connTrees[agency].min && departureTime.getTime() < connTrees[agency].max) {
                t0 = new Date()
                const tree = connTrees[agency].avlTree;
                const node = tree.find(found_fragment);
                let prev = tree.prev(node);
                let next = tree.next(node);

                jsonldGraph.push(node.data);

                // Complete connection departing at the same time
                while (prev && prev.key === lowLimit) {
                    jsonldGraph.push(prev.data);
                    prev = tree.prev(prev);
                }

                // Add all connections until the beginning of the next fragment
                while (next && next.key < highLimit) {
                    jsonldGraph.push(next.data);
                    next = tree.next(next);
                }
                logger.debug('Build fragment from AVL Tree took ' + (new Date().getTime() - t0.getTime()) + ' ms');

                // Set cache headers
                const conf = utils.getCompanyDatasetConfig(agency);
                if (conf['realTimeData']) {
                    // Valid until the next update + 1 second to allow intermediate caches to update
                    const rtUpdatePeriod = conf['realTimeData']['updatePeriod'];
                    const validUntilDate = utils.getNextUpdate(rtUpdatePeriod);
                    const maxage = Math.round((validUntilDate - now) / 1000);
                    res.set({ 'Cache-Control': 'public, s-maxage=' + maxage + ', max-age=' + (maxage + 1)
                            + ', stale-if-error=' + (maxage + 15) + ', proxy-revalidate' });
                    res.set({ 'Expires': validUntilDate.toUTCString() });
                } else {
                    // Data is static so it can be cached for long time
                    res.set({ 'Cache-Control': 'public, max-age=31536000000, immutable' });
                    res.set({ 'Expires': new Date(now.getTime() + 31536000000).toUTCString() });
                }

            } else {
                // Build fragment by loading static and live connections (if available) from disk
                let sf_path = storage + '/linked_pages/' + agency + '/' + static_version + '/';
                let rt_exists = false;

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
                jsonldGraph = static_buffer.split(',\n').map(JSON.parse);
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
                    jsonldGraph = await utils.aggregateRTData(jsonldGraph, rt_data, rtfs_remove, lowLimit, highLimit, now);
                    logger.debug('---------------------------------------');
                    logger.debug('aggregateRTData() took ' + (new Date().getTime() - t0.getTime()) + ' ms');
                }
            }

            const params = {
                host: host,
                agency: agency,
                departureTime: ff,
                version: static_version,
                index: index,
                data: jsonldGraph,
                staticFragments: staticData.staticFragments
            };



            let final_data = await utils.addHydraMetada(params);

            res.set({ 'Vary': 'Accept, Accept-Encoding, Accept-Datetime' });

            switch (accept) {
                case 'application/json':
                    res.set({ 'Content-Type': 'application/ld+json' });
                    res.status(200).send(final_data);
                    break;
                case 'application/ld+json':
                    res.set({ 'Content-Type': 'application/ld+json' });
                    res.status(200).send(final_data);
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
                    res.status(200).send(final_data);
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

    createFeed(company) {

    }

    getDataset(name) {
        for (let i in this.datasets) {
            if (this.datasets[i].companyName === name) {
                return this.datasets[i];
            }
        }
    }

    get storage() {
        return this._storage;
    }

    get datasets() {
        return this._datasets
    }
}