import util from 'util';
import fs from 'fs';
import { parse as csvParse } from 'fast-csv';
import utpl from 'uri-templates';
import { deleteAsync as del } from 'del';
import { Utils } from '../utils/utils.js';
import { GTFS_ROUTE_TYPES } from '../utils/constants.js';

const utils = new Utils();
const readFile = util.promisify(fs.readFile);
const writeFile = util.promisify(fs.writeFile);

export class Routes {

    constructor(source) {
        this._storage = utils.datasetsConfig.storage;
        this._datasets = utils.datasetsConfig.datasets;
        this._serverConfig = utils.serverConfig;
        this._source = source || null;
    }

    async getRoutes(req, res) {
        try {
            const agency = req.params.agency;
            if (this.datasets.find(d => d.companyName === agency)) {
                if (fs.existsSync(`${this.storage}/routes/${agency}/routes.json`)) {
                    res.set({
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': '*',
                        'Content-Type': 'application/ld+json'
                    });
                    res.send(await readFile(`${this.storage}/routes/${agency}/routes.json`, 'utf8'));
                } else {
                    let routes = await this.createRouteList(agency);
                    if (routes != null) {
                        writeFile(`${this.storage}/routes/${agency}/routes.json`, JSON.stringify(routes), 'utf8');
                        res.set({
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Headers': '*',
                            'Content-Type': 'application/ld+json'
                        });
                        res.send(routes);
                        return;
                    } else {
                        res.set({ 'Cache-Control': 'no-cache' });
                        res.status(404).send("No stops available for " + agency);
                    }
                }
            } else {

            }
        } catch (err) {
            console.error(err);
            res.set({ 'Cache-Control': 'no-cache' });
            res.status(500).send(`Internal error when getting route list for ${agency}`);
        }
    }

    createRouteList(company) {
        return new Promise(async (resolve, reject) => {
            const dataset = this.datasets.find(d => d.companyName === company);
            const feed = await utils.getLatestGtfsSource(`${this.storage}/datasets/${company}`);

            if (feed) {
                const skeleton = {
                    "@context": {
                        "dct": "http://purl.org/dc/terms/",
                        "xsd": "http://www.w3.org/2001/XMLSchema#",
                        "gtfs": "http://vocab.gtfs.org/terms#",
                        "Route": "gtfs:Route",
                        "shortName": {
                            "@id": "gtfs:shortName",
                            "@type": "xsd:string"
                        },
                        "longName": {
                            "@id": "gtfs:longName",
                            "@type": "xsd:string"
                        },
                        "routeType": {
                            "@id": "gtfs:routeType",
                            "@type": "@id"
                        },
                        "routeColor": {
                            "@id": "gtfs:color",
                            "@type": "xsd:string"
                        },
                        "textColor": {
                            "@id": "gtfs:textColor",
                            "@type": "xsd:string"
                        },
                        "description": {
                            "@id": "dct:description",
                            "@type": "xsd:string"
                        }
                    },
                    "@graph": []
                };

                const uncompressed = this.source || await utils.readAndUnzip(feed);
                const routes_uri_template = utpl(dataset['baseURIs']['route']);
                const res = dataset['baseURIs']['resolve'];
                const tripsIndex = await this.getTripsIndex(uncompressed);

                fs.createReadStream(`${uncompressed}/routes.txt`, { encoding: 'utf8', objectMode: true })
                    .pipe(csvParse({ objectMode: true, headers: true }))
                    .on('data', route => {
                        const trip = tripsIndex.get(route['route_id']);
                        let obj = {
                            "@id": utils.resolveURI(routes_uri_template, { route: route, trip: trip }, res),
                            "@type": "Route",
                            "shortName": route['route_short_name'] ? route['route_short_name'].trim() : null,
                            "longName": route['route_long_name'] ? route['route_long_name'].replace('--', '–').trim() : null,
                            "routeColor": route['route_color'] ? route['route_color'].trim() : null,
                            "textColor": route['route_text_color'] ? route['route_text_color'].trim() : null,
                            "description": route['route_desc'] ? route['route_desc'].trim() : null,
                            "routeType": GTFS_ROUTE_TYPES[route['route_type']] || null
                        };

                        obj = this.cleanEmpties(obj);
                        skeleton['@graph'].push(obj);
                    }).on('error', err => {
                        reject(err);
                    }).on('end', async () => {
                        if (!this.source) {
                            await del([uncompressed], { force: true });
                        }
                        resolve(skeleton);
                    });

            } else {
                resolve(null);
            }
        });
    }

    getTripsIndex(path) {
        return new Promise((resolve, reject) => {
            let map = new Map();
            fs.createReadStream(`${path}/trips.txt`, { encoding: 'utf8', objectMode: true })
                .pipe(csvParse({ objectMode: true, headers: true }))
                .on('data', trip => {
                    map.set(trip['route_id'], trip);
                })
                .on('error', err => reject(err))
                .on('end', () => resolve(map));
        });
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