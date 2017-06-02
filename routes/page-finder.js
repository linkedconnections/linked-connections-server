var express = require('express');
var router = express.Router();
var fs = require('fs');
const zlib = require('zlib');


router.get('/:departureTime', function (req, res, next) {
    var departureTime = new Date(req.params.departureTime);
    var buffer = [];

    departureTime.setMinutes(departureTime.getMinutes() - (departureTime.getMinutes() % 10));

    while (!fs.existsSync('./linked_pages/SNCB/' + departureTime.toISOString() + '.jsonld.gz')) {
        departureTime.setMinutes(departureTime.getMinutes() - 10);
    }

    fs.createReadStream('./linked_pages/SNCB/' + departureTime.toISOString() + '.jsonld.gz')
        .pipe(new zlib.createGunzip())
        .on('data', function (data) {
            buffer.push(data);
        })
        .on('end', function () {
            var jsonld_graph = buffer.join('').split(',\n');
            var jsonld_skeleton = JSON.parse(fs.readFileSync('./statics/skeleton.jsonld', 'utf8'));

            jsonld_skeleton['@id'] = jsonld_skeleton['@id'] + '/' + departureTime.toISOString();
            jsonld_skeleton['hydra:next'] = jsonld_skeleton['hydra:next'] + '/' + getAdjacentPage(departureTime, true);
            jsonld_skeleton['hydra:previous'] = jsonld_skeleton['hydra:previous'] + '/' + getAdjacentPage(departureTime, false);

            for(let i in jsonld_graph) {
                jsonld_skeleton['@graph'].push(JSON.parse(jsonld_graph[i]));
            }

            res.json(jsonld_skeleton);
        });
});

function getAdjacentPage(departureTime, next) {
    var date = new Date(departureTime.toISOString());
    if(next) {
        date.setMinutes(date.getMinutes() + 10);
    } else {
        date.setMinutes(date.getMinutes() - 10);
    }
    while (!fs.existsSync('./linked_pages/SNCB/' + date.toISOString() + '.jsonld.gz')) {
        if (next) {
            date.setMinutes(date.getMinutes() + 10);
        } else {
            date.setMinutes(date.getMinutes() - 10);
        }
    }

    return date.toISOString();
}

module.exports = router;