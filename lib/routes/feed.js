const util = require('util');
const fs = require('fs');
const utils = require('../utils/utils');
const accepts = require('accepts');
const ConnectionsFeed = require('../data/connections-feed');
const StaticData = require('../data/static');
const RealTimeData = require('../data/real-time');
const Logger = require("../utils/logger");

const readFile = util.promisify(fs.readFile);
const server_config = utils.serverConfig;

const logger = Logger.getLogger(utils.serverConfig.logLevel || 'info');
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
            const tree = new ConnectionsFeed({staticData: staticData, RTData: RTData, agency: d['companyName']});
            await tree.init();
            feedTrees[d['companyName']] = tree;
        }
    }

    async getFeed(req, res) {
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

        // Request path and query parameters
        const agency = req.params.agency;
        let created = new Date(decodeURIComponent(req.query.created));
        // query can have a departureTime if and only if we request static data
        let departure_time = new Date(decodeURIComponent(req.query.departureTime));

        // TODO: use path to avoid OS compatibility issues and handle the trailing slash
        // Check if there is static and real-time data for the requested company
        if (!fs.existsSync(`${storage}/feed_history/${agency}`) || !fs.existsSync(`${storage}/linked_pages/${agency}`)) {
            res.set({'Cache-Control': 'immutable'});
            res.status(404).send("Agency not found");
            return;
        }

        // Accept header
        const accept = accepts(req).type(mimeTypes);

        // Redirect to NOW time in case provided date is invalid
        if (created.toString() === 'Invalid Date') {
            // Just save to a variable, a redirect will automatically follow since this won't
            // perfectly resolve to an existing page
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
            let [found_fragment, index, is_static] = utils.findFeedResource(agency, created.getTime(), RTData.RTFragments, staticData.staticFragments);
            logger.debug('finding fragment took ' + (new Date().getTime() - t0.getTime()) + ' ms');

            // Redirect client to the appropriate fragment URL
            console.log("times:", created.getTime(), found_fragment);
            if (created.getTime() !== found_fragment) {
                let found_fragment_date = new Date(found_fragment);
                res.location('/' + agency + '/connections/feed?created=' + found_fragment_date.toISOString());
                res.status(302).send();
                return;
            }

            // find and retrieve the actual data contained in that fragment
            let members, id, previous, next, latest, next_departure;
            members = id = previous = next = latest = next_departure = null;
            let tree = feedTrees[agency].avlTree;
            // get latest page (or rightmost node in AVL tree)
            latest = tree.max();
            if (!is_static) {
                // since fragment is a real-time fragment, check if it is in the AVL tree
                if (created.getTime() >= tree.min() && created.getTime() <= tree.max()) {
                    const node = tree.find(created.getTime());
                    members = node.data;
                    id = node.key;
                } else {
                    // if fragment is not in the AVL tree, get it from the directory.
                    members = JSON.parse(await readFile(`${storage}/feed_history/${agency}/${created.toISOString()}.json`));
                    id = created.getTime();
                }

                // get previous page
                if (index > 0) {
                    previous = RTData.RTFragments[agency][index - 1];
                } else {
                    // if index === 0, then previous is in the static data
                    const static_versions = Object.keys(staticData.staticFragments[agency]).sort();
                    previous = static_versions[static_versions.length - 1];
                }

                // get next page
                if (index < RTData.RTFragments[agency].length - 1) {
                    next = RTData.RTFragments[agency][index + 1];
                }

                // we don't need departure_time with real-time data
                departure_time = null;
            } else {
                // data is static, so get it from the directory (since we don't store static data in the AVL tree).
                // NOTE: when providing static data, we paginate the LDES by 'created' time (aka the different static
                // versions) AND by 'departure_time'.

                // first, make sure that we have a valid departure_time
                if (departure_time.toString() === 'Invalid Date') {
                    // if we don't have a departure time yet, we take the first departure time in the current
                    // version of static data
                    departure_time = new Date(staticData.staticFragments[agency][created.toISOString()][0]);
                }
                // now that we have a valid departure time, we look for a matching fragment
                let [_, static_fragment, static_index] = utils.findResource(agency, departure_time.getTime(), [created], staticData.staticFragments);

                const static_fragment_date = new Date(static_fragment);
                // Redirect client to the appropriate fragment URL
                if (departure_time.getTime() !== static_fragment) {
                    res.location(`/${agency}/connections/feed?created=${created.toISOString()}&departureTime=${static_fragment_date.toISOString()}`);
                    res.status(302).send();
                    return;
                }

                // get fragment data from disk
                let static_path = storage + '/linked_pages/' + agency + '/' + created.toISOString();
                members = JSON.parse('[' + await utils.readAndGunzip(`${static_path}/${static_fragment_date.toISOString()}.jsonld.gz`) + ']');
                id = created.getTime();

                // get previous and next version if possible and necessary
                if (static_index === 0) {
                    const static_versions = Object.keys(staticData.staticFragments[agency]).sort();
                    const version_index = static_versions.indexOf(created.getTime());

                    // if we are on the first departureTime fragment and there is a previous static version,
                    // then we set previous
                    if (version_index > 0) {
                        previous = static_versions[version_index - 1];
                    }

                    // if we are on the first departureTime fragment and there is a next static or real-time version,
                    // then we set next
                    if (version_index < static_versions.length - 1) {
                        next = static_versions[version_index + 1];
                    } else if (version_index === static_versions.length - 1) {
                        next = RTData.RTFragments[agency][0];
                    }
                }

                // get next departure time fragment if possible
                const static_fragments = staticData.staticFragments[agency][created];
                if (static_index < static_fragments.length - 1) {
                    next_departure = static_fragments[static_index + 1];
                }
            }

            const params = {
                host: host,
                agency: agency,
                members: members,       // connections
                id: id,                 // epoch time of created
                previous: previous,     // epoch time of previous fragment
                next: next,             // epoch time of next fragment
                latest: latest,         // epoch time of latest fragment
                isStatic: is_static,
                departureTime: departure_time,
                nextDeparture: next_departure,
            };

            console.log(members.length);

            // create LDES page
            t0 = new Date();
            let final_data = await utils.addLDESMetadata(params);

            res.set({'Vary': 'Accept, Accept-Encoding, Accept-Datetime'});

            switch (accept) {
                case 'application/json':
                    res.set({'Content-Type': 'application/ld+json'});
                    res.status(200).send(final_data);
                    break;
                case 'application/ld+json':
                    res.set({'Content-Type': 'application/ld+json'});
                    res.status(200).send(final_data);
                    break;
                case 'text/turtle':
                    res.set({'Content-Type': 'application/trig'});
                    res.status(200).send(await utils.jsonld2RDF(final_data, 'text/turtle'));
                    break;
                case 'application/n-triples':
                    res.set({'Content-Type': 'application/n-quads'});
                    res.status(200).send(await utils.jsonld2RDF(final_data, 'application/n-triples'));
                    break;
                case 'application/n-quads':
                    res.set({'Content-Type': 'application/n-quads'});
                    res.status(200).send(await utils.jsonld2RDF(final_data, 'application/n-quads'));
                    break;
                case 'application/trig':
                    res.set({'Content-Type': 'application/trig'});
                    res.status(200).send(await utils.jsonld2RDF(final_data, 'application/trig'));
                    break;
                default:
                    res.set({'Content-Type': 'application/ld+json'});
                    console.log(final_data);
                    res.status(200).send(final_data);
                    break;
            }

            logger.debug('Add Metadata took ' + (new Date().getTime() - t0.getTime()) + ' ms');

        } catch (err) {
            if (err) {
                logger.error(err);
                console.error(err);
            }
            res.set({'Cache-Control': 'no-cache'});
            res.status(404).send();
        }

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

module.exports = Feed;