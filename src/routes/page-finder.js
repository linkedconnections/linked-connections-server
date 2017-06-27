const express = require('express');
const router = express.Router();
const fs = require('fs');
const zlib = require('zlib');

const config = JSON.parse(fs.readFileSync('./datasets_config.json', 'utf8'));
let storage = config.storage;

router.get('/:agency/connections/:departureTime', function (req, res) {
    let agency = req.params.agency;
    let departureTime = new Date(req.params.departureTime);
    let acceptDatetime = req.headers['accept-datetime'];
    let buffer = [];

    if(storage.endsWith('/')) {
        storage = storage.substring(0, storage.length - 1);
    }

    if (fs.existsSync(storage + '/linked_pages/' + agency)) {
        fs.readdir(storage + '/linked_pages/' + agency, (err, versions) => {
            //Check if previous version of resource is been requested
            if (typeof acceptDatetime !== 'undefined') {
                //Sort versions list according to the requested version
                sortVersions(new Date(acceptDatetime), versions, (sortedVersions) => {
                    // Find closest resource to requested version
                    findResource(agency, departureTime, sortedVersions, (last_version) => {
                        if (last_version != null) {
                            //Adjust requested resource to match 10 minutes step format
                            departureTime.setMinutes(departureTime.getMinutes() - (departureTime.getMinutes() % 10));
                            //Find closest resource
                            while (!fs.existsSync(storage + '/linked_pages/' + agency + '/' + last_version + '/' + departureTime.toISOString() + '.jsonld.gz')) {
                                departureTime.setMinutes(departureTime.getMinutes() - 10);
                            }

                            //Set Memento headers pointng to the found version
                            res.location('/memento/' + agency + '/' + last_version + '/' + departureTime.toISOString());
                            res.set({
                                'Vary': 'accept-datetime',
                                'Link': '<http://' + req.headers.host + '/' + agency + '/connections/' + departureTime.toISOString() + '>; rel=\"original timegate\"'
                            });

                            //Send HTTP redirect to client
                            res.status(302).send();
                        } else {
                            res.status(404).send();
                        }
                    });
                });

            } else {
                //Find last version containing the requested resource
                findResource(agency, departureTime, versions, (last_version) => {
                    if (last_version != null) {
                        //Adjust requested resource to match 10 minutes step format
                        departureTime.setMinutes(departureTime.getMinutes() - (departureTime.getMinutes() % 10));
                        //Find closest resource
                        while (!fs.existsSync(storage + '/linked_pages/' + agency + '/' + last_version + '/' + departureTime.toISOString() + '.jsonld.gz')) {
                            departureTime.setMinutes(departureTime.getMinutes() - 10);
                        }

                        //Complement resource with Hydra metadata and send it back to the client 
                        fs.createReadStream(storage + '/linked_pages/' + agency + '/' + last_version + '/' + departureTime.toISOString() + '.jsonld.gz')
                            .pipe(new zlib.createGunzip())
                            .on('data', function (data) {
                                buffer.push(data);
                            })
                            .on('end', function () {
                                var jsonld_graph = buffer.join('').split(',\n');
                                fs.readFile('./statics/skeleton.jsonld', { encoding: 'utf8' }, (err, data) => {
                                    var jsonld_skeleton = JSON.parse(data);
                                    jsonld_skeleton['@id'] = jsonld_skeleton['@id'] + agency + '/connections/' + departureTime.toISOString();
                                    jsonld_skeleton['hydra:next'] = jsonld_skeleton['hydra:next'] + agency 
                                        + '/connections/' + getAdjacentPage(agency + '/' + last_version, departureTime, true);
                                    jsonld_skeleton['hydra:previous'] = jsonld_skeleton['hydra:previous'] + agency 
                                        + '/connections/' + getAdjacentPage(agency + '/' + last_version, departureTime, false);
                                    jsonld_skeleton['hydra:search']['hydra:template'] = jsonld_skeleton['hydra:search']['hydra:template'] + agency + '/connections/{?departureTime}';

                                    for (let i in jsonld_graph) {
                                        jsonld_skeleton['@graph'].push(JSON.parse(jsonld_graph[i]));
                                    }

                                    res.json(jsonld_skeleton);
                                });
                            });
                    } else {
                        res.status(404).send();
                    }
                });
            }
        });
    } else {
        res.status(404).send();
    }
});

function sortVersions(acceptDatetime, versions, cb) {
    let diffs = [];
    let sorted = [];

    for (v of versions) {
        let diff = Math.abs(acceptDatetime.getTime() - new Date(v).getTime());
        diffs.push({ 'version': v, 'diff': diff });
    }

    diffs.sort(function (a, b) { return b.diff - a.diff })

    for (d of diffs) {
        sorted.push(d.version);
    }

    cb(sorted);
}

function findResource(agency, departureTime, versions, cb) {
    var ver = versions.slice(0);

    (function checkVer() {
        var version = ver.splice(ver.length - 1, 1)[0];

        fs.readdir(storage + '/linked_pages/' + agency + '/' + version, (err, pages) => {
            if (err) { cb(null); return }
            if (typeof pages !== 'undefined' && pages.length > 0) {
                let di = new Date(pages[0].substring(0, pages[0].indexOf('.jsonld.gz')));
                let df = new Date(pages[pages.length - 1].substring(0, pages[pages.length - 1].indexOf('.jsonld.gz')));

                if (departureTime >= di && departureTime <= df) {
                    cb(version);
                } else if (ver.length == 0) {
                    cb(null);
                } else {
                    checkVer();
                }
            } else {
                checkVer();
            }
        });
    })();
}

function getAdjacentPage(path, departureTime, next) {
    var date = new Date(departureTime.toISOString());
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