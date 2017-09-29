const util = require('util');
const express = require('express');
const fs = require('fs');
const zlib = require('zlib');
const utils = require('../utils/utils');

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
        let jsonld_graph = buffer.join('').split(',\n');
        let departureTime = new Date(resource);
        let mementoDate = new Date(acceptDatetime);
        let rt_path = storage + '/real_time/' + agency + '/' + departureTime.toISOString() + '.jsonld.gz';

        // Look if there is real time data for this agency and requested time
        if (fs.existsSync(rt_path)) {
            let rt_buffer = await utils.readAndGunzip(rt_path);
            // Create an array of all RT updates
            let rt_array = rt_buffer.join('').split('\n');
            // Create an indexed Map object for connection IDs and position in the RT updates array
            // containing the last Connection updates for a given moment
            let rt_map = utils.getIndexedMap(rt_array, mementoDate);
            // Proceed to apply updates if there is any for the given criteria 
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
        } 

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