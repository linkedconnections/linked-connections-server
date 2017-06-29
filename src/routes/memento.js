const express = require('express');
const router = express.Router();
const fs = require('fs');
const zlib = require('zlib');

const config = JSON.parse(fs.readFileSync('./datasets_config.json', 'utf8'));
const hostname = JSON.parse(fs.readFileSync('./server_config.json', 'utf8')).hostname;

let storage = config.storage;

router.get('/:agency', function (req, res) {
    const host = req.protocol + '://' + hostname + '/';
    let agency = req.params.agency;
    let version = req.query.version;
    let resource = req.query.departureTime;
    let buffer = [];

    if(storage.endsWith('/')) {
        storage = storage.substring(0, storage.length - 1);
    }

    fs.createReadStream(storage + '/linked_pages/' + agency + '/' + version + '/' + resource + '.jsonld.gz')
        .pipe(new zlib.createGunzip())
        .on('data', function (data) {
            buffer.push(data);
        })
        .on('end', function () {
            var jsonld_graph = buffer.join('').split(',\n');
            fs.readFile('./statics/skeleton.jsonld', { encoding: 'utf8' }, (err, data) => {
                var jsonld_skeleton = JSON.parse(data);
                jsonld_skeleton['@id'] = host + 'memento/' + agency + '?version=' + version + '&departureTime=' + resource;
                jsonld_skeleton['hydra:next'] = host + 'memento/' + agency + '?version=' + version + '&departureTime=' + getAdjacentPage(agency + '/' + version, resource, true);
                jsonld_skeleton['hydra:previous'] = host + 'memento/' + agency + '?version=' + version + '&departureTime=' + getAdjacentPage(agency + '/' + version, resource, false);
                jsonld_skeleton['hydra:search']['hydra:template'] = host + 'memento/' + agency + '/{?version,departureTime}';

                for (let i in jsonld_graph) {
                    jsonld_skeleton['@graph'].push(JSON.parse(jsonld_graph[i]));
                }

                res.set({'Access-Control-Allow-Origin': '*'});
                res.json(jsonld_skeleton);
            });
        });
});


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