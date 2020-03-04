const util = require('util');
const fs = require('fs');
const csv = require('fast-csv');
const uri_templates = require('uri-templates');
const del = require('del');
const utils = require('../utils/utils');

const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);
const locationTypes = ["Stop", "Station", "Entrance_Exit", "GenericNode", "BoardingArea"];

class Stops {
    constructor(source) {
        this._storage = utils.datasetsConfig.storage;
        this._datasets = utils.datasetsConfig.datasets;
        this._serverConfig = utils.serverConfig;
        this._source = source || null;
    }

    async getStops(req, res) {
        try {
            const agency = req.params.agency;
            if (utils.getCompanyDatasetConfig(agency)) {
                if (fs.existsSync(this.storage + '/stops/' + agency + '/stops.json')) {
                    res.set({
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': '*',
                        'Content-Type': 'application/ld+json'
                    });
                    res.send(await readFile(`${this.storage}/stops/${agency}/stops.json`, 'utf8'));
                } else {
                    let stops = await this.createStopList(agency);
                    writeFile(this.storage + '/stops/' + agency + '/stops.json', JSON.stringify(stops), 'utf8');
                    if (stops != null) {
                        res.set({
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Headers': '*',
                            'Content-Type': 'application/ld+json'
                        });
                        res.send(stops);
                        return;
                    } else {
                        res.set({ 'Cache-Control': 'no-cache' });
                        res.status(404).send("No stops available for " + agency);
                    }
                }
            } else {
                res.set({ 'Cache-Control': 'no-cache' });
                res.status(404).send(`${agency} does not exist in this server`);
            }
        } catch (err) {
            console.error(err);
            res.set({ 'Cache-Control': 'no-cache' });
            res.status(500).send(`Internal error when getting stop list for ${agency}`);
        }
    }

    createStopList(company) {
        return new Promise(async (resolve, reject) => {
            let dataset = this.getDataset(company);
            let feed = await utils.getLatestGtfsSource(`${this.storage}/datasets/${company}`);

            if (feed) {
                let skeleton = {
                    "@context": {
                        "gtfs": "http://vocab.gtfs.org/terms#",
                        "geo": "http://www.w3.org/2003/01/geo/wgs84_pos#",
                        "schema": "http://schema.org/",
                        "Stop": "gtfs:Stop",
                        "Station": "gtfs:Station",
                        "Entrance_Exit": "gtfs:Entrance_Exit",
                        "BoardingArea": "gtfs:BoardingArea",
                        "GenericNode": "gtfs:GenericNode",
                        "code": "gtfs:code",
                        "platformCode": "gtfs:platformCode",
                        "name": "schema:name",
                        "longitude": "geo:long",
                        "latitude": "geo:lat",
                        "parentStation": {
                            "@id": "gtfs:parentStation",
                            "@type": "@id"
                        }
                    },
                    "@graph": []
                };
                let uncompressed = this.source || await utils.readAndUnzip(feed);
                let stops_uri_template = uri_templates(dataset['baseURIs']['stop']);
                let res = dataset['baseURIs']['resolve'];
                let index = new Map();

                fs.createReadStream(uncompressed + '/stops.txt', { encoding: 'utf8', objectMode: true })
                    .pipe(csv.parse({ objectMode: true, headers: true }))
                    .on('data', data => {
                        index.set(data['stop_id'], data);
                    }).on('end', async () => {
                        if (!this.source) {
                            await del([uncompressed], { force: true });
                        }

                        index.forEach(data => {
                            let obj = {
                                "@id": utils.resolveURI(stops_uri_template, { stop: data }, res),
                                "@type": data['location_type'] ? locationTypes[parseInt(data['location_type'])] : "Stop",
                                "latitude": data['stop_lat'].trim(),
                                "longitude": data['stop_lon'].trim(),
                                "name": data['stop_name'].trim(),
                                "code": data['stop_code'].trim()
                            };

                            // Add parent station and platform code if any
                            if (obj['@type'] && obj['@type'] !== "Station") {
                                obj['parentStation'] = index.has(data['parent_station']) ?
                                    utils.resolveURI(stops_uri_template, { stop: index.get(data['parent_station']) }, res) : undefined;
                                obj['platformCode'] = data['platform_code'] ? data['platform_code'].trim() : undefined;
                            }

                            obj = this.cleanEmpties(obj);
                            skeleton['@graph'].push(obj);
                        });
                        resolve(skeleton);
                    });
            } else {
                resolve(null);
            }
        });
    }

    getDataset(name) {
        for (let i in this.datasets) {
            if (this.datasets[i].companyName === name) {
                return this.datasets[i];
            }
        }
    }

    cleanEmpties(obj) {
        let keys = Object.keys(obj);
        for (let i in keys) {
            if (!obj[keys[i]] || obj[keys[i]] === '') {
                delete obj[keys[i]];
            }
        }

        return obj;
    }

    get storage() {
        return this._storage;
    }

    get datasets() {
        return this._datasets;
    }

    get serverConfig() {
        return this._serverConfig;
    }

    get source() {
        return this._source;
    }
}

module.exports = Stops;