const util = require('util');
const express = require('express');
const fs = require('fs');
const zlib = require('zlib');
const utils = require('../utils/utils');
const logger = require('../utils/logger');

const readfile = util.promisify(fs.readFile);

const router = express.Router();
const datasets_config = utils.datasetsConfig;
const server_config = utils.serverConfig;
const storage = datasets_config.storage;

router.get('/:agency', async (req, res) => {
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

    if (storage.endsWith('/')) {
        storage = storage.substring(0, storage.length - 1);
    }

    try {
        let buffer = await utils.readAndGunzip(storage + '/linked_pages/' + agency + '/' + version + '/' + resource + '.jsonld.gz');
        let jsonld_graph = buffer.join('').split(',\n').map(JSON.parse);
        let departureTime = new Date(resource);
        let mementoDate = new Date(acceptDatetime);

        // If the real-time fragment being requested is 5 hours old or more it should be compressed.
        let rt_path = '';
        let compressed = false;
        if (new Date().getTime() - departureTime.getTime() > (3600 * 5 * 1000)) {
            rt_path = storage + '/real_time/' + agency + '/' + departureTime.toISOString() + '.jsonld.gz';
            compressed = true;
        } else {
            rt_path = storage + '/real_time/' + agency + '/' + departureTime.toISOString() + '.jsonld';
        }

        // Look if there is real time data for this agency and requested time
        if (fs.existsSync(rt_path)) {
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
            jsonld_graph = utils.aggregateRTData(jsonld_graph, rt_array, agency, departureTime, mementoDate);
        }

        // Finally build a JSON-LD document containing the data and return it to the client
        const headers = {
            'Memento-Datetime': mementoDate.toUTCString(),
            'Link': '<' + host + agency + '/connections?departureTime=' + departureTime.toISOString() + '>; rel=\"original timegate\"',
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/ld+json',
        }
        const params = {
            storage: storage,
            host: host,
            agency: agency,
            departureTime: departureTime,
            version: version,
            data: jsonld_graph,
            http_headers: headers,
            http_response: res
        }

        utils.addHydraMetada(params);
    } catch (err) {
        if (err) logger.error(err);
        res.status(404).send();
    }
});

module.exports = router;