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
    }

    /**
     * creates indexes for each dataset of static and real-time data and creates an AVL tree with the real-time data
     * @returns {Promise<void>}
     */
    async init() {
        // Create static and RT fragments index structure
        await Promise.all([
            await staticData.updateStaticFragments(),
            await RTData.initRTFragments()
        ]);

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
        let created = (req.query.created != null) ? new Date(decodeURIComponent(req.query.created)) : new Date();
        let departure_time = new Date(decodeURIComponent(req.query.departureTime));

        // TODO: use path to avoid OS compatibility issues and handle the trailing slash
        // Check if there is static and real-time data for the requested company
        if (!fs.existsSync(`${storage}/feed_history/${agency}`) || !fs.existsSync(`${storage}/linked_pages/${agency}`)) {
            res.set({'Cache-Control': 'no-cache'});
            res.status(404).send("Agency not found");
            return;
        }

        // Accept header
        const accept = accepts(req).type(mimeTypes);

        try {
            // Find the directory name (either a static version or a RT update) that covers the requested time
            let t0 = new Date(); // time how long it takes to find a fragment
            let [dir_name, _, is_static] = utils.findFeedResource(agency, created.toISOString(), RTData.RTFragments, staticData.staticFragments);

            // Redirect client to the appropriate fragment URL if necessary
            if (created.toISOString() !== dir_name) {
                let found_update_date = new Date(dir_name);
                res.location('/' + agency + '/connections/feed?created=' + found_update_date.toISOString());
                res.status(302).send();
                return;
            }

            // make sure that we have a valid departure_time
            if (departure_time.toString() === 'Invalid Date') {
                // if we don't have a valid departure_time, we take the departure time from the first fragment (either
                // from the RT or static data)
                if (is_static) {
                    departure_time = new Date(staticData.staticFragments[agency][created.toISOString()][0]);
                } else {
                    departure_time = new Date(RTData.RTFragments[agency][created.toISOString()][0]);
                }
                // once we have a valid date, we redirect
                res.location(`/${agency}/connections/feed?created=${created.toISOString()}&departureTime=${departure_time.toISOString()}`);
                res.status(302).send();
                return;
            }

            // find the fragment name and its index
            let fragment, fragment_index;
            if (!is_static) {
                [_, fragment, fragment_index] = utils.findRTResource(agency, departure_time.getTime(), created.toISOString(), RTData.RTFragments);
            } else {
                [_, fragment, fragment_index] = utils.findResource(agency, departure_time.getTime(), [created.toISOString()], staticData.staticFragments);
            }

            // Redirect client to the appropriate fragment URL if necessary
            const fragment_date = new Date(fragment);
            if (departure_time.getTime() !== fragment) {
                res.location(`/${agency}/connections/feed?created=${created.toISOString()}&departureTime=${fragment_date.toISOString()}`);
                res.status(302).send();
                return;
            }

            logger.debug('[feed] finding fragment took ' + (new Date().getTime() - t0.getTime()) + ' ms');
            t0 = new Date(); // time how long it takes to retrieve the found fragment

            // retrieve the fragment data from disk or AVL tree
            let tree = feedTrees[agency].avlTree;
            let members;
            if (!is_static) {
                if (created.getTime() >= tree.min() && created.getTime() <= tree.max()) {
                    // get RT data fragment from AVL tree
                    const node = tree.find(created.getTime());
                    members = JSON.parse(JSON.stringify(node.data[fragment_date.getTime()]));
                } else {
                    // get RT data fragment from disk
                    const path = `${storage}/feed_history/${agency}/${created.toISOString()}/${fragment_date.toISOString()}.json.gz`;
                    members = JSON.parse(await utils.readAndGunzip(path));
                }
            } else {
                // get static data fragment from disk
                const path = `${storage}/linked_pages/${agency}/${created.toISOString()}/${fragment_date.toISOString()}.jsonld.gz`;
                members = JSON.parse(`[${await utils.readAndGunzip(path)}]`);
            }

            logger.debug('[feed] retrieving fragment took ' + (new Date().getTime() - t0.getTime()) + ' ms');
            t0 = new Date(); // time how long it takes to create a LDES page with metadata using the fragment

            // create the necessary parameters to build a page
            let id = created.getTime();
            let latest, previous, next, next_departure;
            latest = previous = next = next_departure = null;
            const versions = Object.keys(staticData.staticFragments[agency]).sort();
            const updates = Object.keys(RTData.RTFragments[agency]).sort();

            // 'latest', 'previous' and 'next' can only be set when we are on the first page
            // of the version or update
            if (fragment_index === 0) {
                // latest
                latest = tree.max();
                if (latest === null) {
                    // if there is no RT data, 'latest' is the last static version
                    const latest_date = new Date(versions[versions.length - 1]);
                    latest = latest_date.getTime();
                }

                // previous and next
                if (is_static) {
                    const version_index = versions.indexOf(created.toISOString());

                    // if there is a previous static version, we set previous
                    if (version_index > 0) {
                        previous = versions[version_index - 1];
                    }

                    // if there is a next static or real-time version, we set next
                    if (version_index < versions.length - 1) {
                        next = versions[version_index + 1];
                    } else if (version_index === versions.length - 1 && updates.length > 0) {
                        next = updates[0];
                    }
                } else {
                    const update_index = updates.indexOf(created.toISOString());

                    // if there is a previous real-time or static version, we set previous
                    if (update_index > 0) {
                        previous = updates[update_index - 1];
                    } else if (update_index === 0 && versions.length > 0) {
                        previous = versions[versions.length - 1];
                    }

                    // if there is a next real-time version, we set next
                    if (update_index < updates.length - 1) {
                        next = updates[update_index + 1];
                    }
                }
            }

            // next_departure
            const fragments = (is_static) ? staticData.staticFragments[agency][created.toISOString()] : RTData.RTFragments[agency][created.toISOString()];
            if (fragment_index < fragments.length - 1) {
                next_departure = fragments[fragment_index + 1];
            }

            members.forEach((member) => {
                member["isVersionOf"] = member["@id"];
                member["@id"] += `/${created.toISOString()}`;
                member["created"] = created.toISOString();
            });

            departure_time = departure_time.getTime();

            const params = {
                host: host,
                agency: agency,
                members: members,       // array of connections
                created: id,            // epoch time of created
                previous: previous,     // epoch time of previous fragment
                next: next,             // epoch time of next fragment
                latest: latest,         // epoch time of latest fragment
                isStatic: is_static,    // says if requested page is static or real-time data
                departureTime: departure_time,  // departureTime of first connection in current fragment
                nextDeparture: next_departure,  // departureTime of first connection in next fragment
            };

            // create LDES page
            let final_data = await utils.addLDESMetadata(params);

            res.set({'Vary': 'Accept, Accept-Encoding, Accept-Datetime'});

            if (id === latest) {
                // if requested page is latest page, set Cache-Control: max-age
                const conf = utils.getCompanyDatasetConfig(agency);
                const next_update = is_static ? utils.getNextUpdate(conf["updatePeriod"]) : utils.getNextUpdate(conf["realTimeData"]["updatePeriod"]);
                const now = new Date();
                res.set({'Cache-Control': `max-age=${Math.round((next_update - now) / 1000)}`})
            } else {
                // if requested page is not latest page, set Cache-Control: immutable
                res.set({'Cache-Control': 'immutable'});
            }

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
                    res.status(200).send(final_data);
                    break;
            }

            logger.debug('[feed] Creating page + adding metadata took ' + (new Date().getTime() - t0.getTime()) + ' ms');

        } catch (err) {
            if (err) {
                logger.error('[feed] ' + err);
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