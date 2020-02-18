const util = require('util');
const fs = require('fs');
const csv = require('fast-csv');
const uri_templates = require('uri-templates');
const del = require('del');
const utils = require('../utils/utils');

const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);

class Stops {
    constructor() {
        this._storage = utils.datasetsConfig.storage;
        this._datasets = utils.datasetsConfig.datasets;
        this._serverConfig = utils.serverConfig;
    }

    async getStops(req, res) {
        try {
            const agency = req.params.agency;
            if (fs.existsSync(this.storage + '/stops/' + agency + '/stops.json')) {
                res.set({
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': '*',
                    'Content-Type': 'application/ld+json'
                });
                res.send(await readFile(this.storage + '/stops/' + agency + '/stops.json', 'utf8'));
                return;
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
                        "dct": "http://purl.org/dc/terms/",
                        "schema": "http://schema.org/",
                        "name": "schema:name",
                        "longitude": "http://www.w3.org/2003/01/geo/wgs84_pos#long",
                        "latitude": "http://www.w3.org/2003/01/geo/wgs84_pos#lat",
                        "dct:spatial": {
                            "@type": "@id"
                        },
                    },
                    "@id": (this.serverConfig.protocol || "http") + "://" + this.serverConfig.hostname + "/" + company + '/stops',
                    "@graph": []
                };

                let uncompressed = await utils.readAndUnzip(feed);
                let stops_uri_template = uri_templates(dataset['baseURIs']['stop']);
                let res = dataset['baseURIs']['resolve'];

                fs.createReadStream(uncompressed + '/stops.txt', { encoding: 'utf8', objectMode: true })
                    .pipe(csv.parse({ objectMode: true, headers: true }))
                    .on('data', data => {
                        let obj = {
                            "@id": utils.resolveURI(stops_uri_template, { stop: data }, res),
                            "dct:spatial": dataset['geographicArea'] || "",
                            "latitude": data['stop_lat'].trim(),
                            "longitude": data['stop_lon'].trim(),
                            "name": data['stop_name'].trim(),
                        };

                        obj = this.cleanEmpties(obj);
                        skeleton['@graph'].push(obj);
                    }).on('end', async () => {
                        await del([uncompressed], { force: true });
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
}

module.exports = Stops;