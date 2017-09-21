const express = require('express');
const router = express.Router();
const fs = require('fs');
const zlib = require('zlib');

const dataset_config = JSON.parse(fs.readFileSync('./datasets_config.json', 'utf8'));
const server_config = JSON.parse(fs.readFileSync('./server_config.json', 'utf8'));

let storage = dataset_config.storage;

router.get('/:agency', function (req, res) {
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
    let agency = req.params.agency;
    let version = req.query.version;
    let resource = req.query.departureTime;
    let acceptDatetime = req.headers['accept-datetime'];
    let buffer = [];

    if (storage.endsWith('/')) {
        storage = storage.substring(0, storage.length - 1);
    }

    fs.createReadStream(storage + '/linked_pages/' + agency + '/' + version + '/' + resource + '.jsonld.gz')
        .pipe(new zlib.createGunzip())
        .on('data', function (data) {
            buffer.push(data);
        })
        .on('end', function () {
            var jsonld_graph = buffer.join('').split(',\n');

            let departureTime = new Date(resource);
            let mementoDate = new Date(acceptDatetime);

            // Look if there is real time data for this agency and requested time
            if (fs.existsSync(storage + '/real_time/' + agency + '/' + departureTime.toISOString() + '.jsonld.gz')) {
                let rt_buffer = [];
                fs.createReadStream(storage + '/real_time/' + agency + '/' + departureTime.toISOString() + '.jsonld.gz')
                    .pipe(new zlib.createGunzip())
                    .on('data', data => {
                        rt_buffer.push(data);
                    })
                    .on('end', () => {
                        // Create an array of all RT updates
                        let rt_array = rt_buffer.join('').split('\n');
                        // Create an indexed Map object for connection IDs and position in the RT updates array
                        // containing the last Connection updates for a given moment
                        let rt_map = getIndexedMap(rt_array, mementoDate);
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
                        addHydraMetada(departureTime, mementoDate, host, agency, version, jsonld_graph, res);
                    });
            } else {
                addHydraMetada(departureTime, mementoDate, host, agency, version, jsonld_graph, res);
            }
        });
});

function getIndexedMap(array, timeCriteria) {
    let map = new Map();

    for (let i = 0; i < array.length; i++) {
        let jo = JSON.parse(array[i]);
        let memento_date = new Date(jo['mementoVersion']);

        if (memento_date <= timeCriteria) {
            map.set(jo['@id'], i);
        } else {
            break;
        }
    }
    return map;
}

function addHydraMetada(departureTime, mementoDate, host, agency, version, jsonld_graph, res) {
    fs.readFile('./statics/skeleton.jsonld', { encoding: 'utf8' }, (err, data) => {
        var jsonld_skeleton = JSON.parse(data);
        jsonld_skeleton['@id'] = host + agency + '/connections?departureTime=' + departureTime.toISOString();
        jsonld_skeleton['hydra:next'] = host + agency + '/connections?departureTime=' + getAdjacentPage(agency + '/' + version, departureTime, true);
        jsonld_skeleton['hydra:previous'] = host + agency + '/connections?departureTime=' + getAdjacentPage(agency + '/' + version, departureTime, false);
        jsonld_skeleton['hydra:search']['hydra:template'] = host + agency + '/connections/{?departureTime}';

        for (let i in jsonld_graph) {
            jsonld_skeleton['@graph'].push(JSON.parse(jsonld_graph[i]));
        }

        res.set({
            'Memento-Datetime': mementoDate.toUTCString(),
            'Link': '<' + host + agency + '/connections?departureTime=' + departureTime.toISOString() + '>; rel=\"original timegate\"',
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/ld+json',
        });
        res.json(jsonld_skeleton);
    });
}

function getAdjacentPage(path, departureTime, next) {
    var date = new Date(departureTime);
    if (next) {
        date.setMinutes(date.getMinutes() + 10);
    } else {
        date.setMinutes(date.getMinutes() - 10);
    }
    while (!fs.existsSync(storage + '/linked_pages/' + path + '/' + date.toISOString() + '.jsonld.gz')) {
        if (next) {
            date.setMinutes(date.getMinutes() + 10);
        } else {
            date.setMinutes(date.getMinutes() - 10);
        }
    }

    return date.toISOString();
}

module.exports = router;