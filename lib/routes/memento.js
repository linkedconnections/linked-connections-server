const util = require('util');
const fs = require('fs');
const accepts = require('accepts');
const utils = require('../utils/utils');
const Logger = require('../utils/logger');

const readfile = util.promisify(fs.readFile);

const datasets_config = utils.datasetsConfig;
const server_config = utils.serverConfig;
const storage = datasets_config.storage;
const logger = Logger.getLogger(server_config.logLevel || 'info');
const mimeTypes = ['application/json',
    'application/ld+json',
    'text/turtle',
    'application/n-triples',
    'application/n-quads',
    'application/trig'
];

class Memento {
    async getMemento(req, res) {
        // Check for available updates of the static fragments
        let t0 = new Date();
        await utils.updateStaticFragments();
        logger.debug('updateStaticFragments() took ' + (new Date().getTime() - t0.getTime()) + ' ms');

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
        const version = req.query.version;
        const resource = req.query.departureTime;
        const acceptDatetime = req.headers['accept-datetime'];
        // Accept header
        const accept = accepts(req).type(mimeTypes);

        if (new Date(resource).toString() === 'Invalid Date') {
            // Cannot provide memento version of an undefined resource
            res.set({ 'Cache-Control': 'no-cache' });
            res.status(400).send("Invalid departure time");
            return;
        }

        if (new Date(acceptDatetime).toString() === 'Invalid Date') {
            // Cannot provide memento version without valid accept-datetime header
            res.set({ 'Cache-Control': 'no-cache' });
            res.status(400).send("Invalid accept-datetime header");
            return;
        }

        if (storage.endsWith('/')) {
            storage = storage.substring(0, storage.length - 1);
        }

        try {
            let sf_path = storage + '/linked_pages/' + agency + '/' + version + '/' + resource + '.jsonld.gz';
            t0 = new Date();
            let buffer = await utils.readAndGunzip(sf_path);
            let jsonld_graph = buffer.split(',\n').map(JSON.parse);
            logger.debug('Read and process static fragment took ' + (new Date().getTime() - t0.getTime()) + ' ms');
            let departureTime = new Date(resource);
            let mementoDate = new Date(acceptDatetime);

            let rt_exists = false;
            let lowLimit = departureTime.getTime();
            let low_index = (utils.staticFragments[agency][version]).indexOf(lowLimit);
            let highLimit = utils.staticFragments[agency][version][low_index + 1];

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
                if (utils.handleConditionalGET(req, res, accept, rtfs[rtfs.length - 1], true, departureTime, mementoDate)) {
                    return;
                }
            } else {
                if (utils.handleConditionalGET(req, res, accept, sf_path, false, departureTime, mementoDate)) {
                    return;
                }
            }

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
                jsonld_graph = await utils.aggregateRTData(jsonld_graph, rt_data, rtfs_remove, lowLimit, highLimit, mementoDate);
                logger.debug('---------------------------------------');
                logger.debug('aggregateRTData() took ' + (new Date().getTime() - t0.getTime()) + ' ms');
            }

            const headers = {
                'Memento-Datetime': mementoDate.toUTCString(),
                'Link': '<' + host + agency + '/connections?departureTime=' + departureTime.toISOString() + '>; rel=\"original timegate\"',
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/ld+json',
                'Vary': 'Accept, Accept-Encoding, Accept-Datetime'
            }
            const params = {
                host: host,
                agency: agency,
                departureTime: departureTime,
                version: version,
                index: low_index,
                data: jsonld_graph,
            }

            let final_data = await utils.addHydraMetada(params);

            let accept = accepts(req);

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
            if (err) logger.error(err);
            res.set({ 'Cache-Control': 'no-cache' });
            res.status(404).send();
        }
    }
}

module.exports = Memento;