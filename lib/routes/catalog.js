import util from 'util';
import fs from 'fs';
import { Utils } from '../utils/utils.js';

const utils = new Utils();
const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);
const writeFile = util.promisify(fs.writeFile);

export class Catalog {
    constructor() {
        this._organization = utils.datasetsConfig.organization;
        this._storage = utils.datasetsConfig.storage;
        this._datasets = utils.datasetsConfig.datasets;
        this._serverConfig = utils.serverConfig;
    }

    async getCatalog(req, res) {
        const agency = req.params.agency;
        try {
            if (this.datasets && this.datasets.find(d => d.companyName === agency)) {
                if (fs.existsSync(`${this.storage}/catalog/${agency}/catalog.json`)) {
                    res.set({
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': '*',
                        'Content-Type': 'application/ld+json'
                    });
                    res.send(await readFile(`${this.storage}/catalog/${agency}/catalog.json`, 'utf8'));
                    res.status(200);
                } else {
                    const catalog = await this.createCatalog(agency);
                    await writeFile(`${this.storage}/catalog/${agency}/catalog.json`, JSON.stringify(catalog), 'utf8');
                    res.set({
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': '*',
                        'Content-Type': 'application/ld+json'
                    });
                    res.send(catalog);
                    res.status(200);
                }
            } else {
                res.set({ 'Cache-Control': 'no-cache' });
                res.status(404).send(`${agency} does not exist in this server`);
            }
        } catch (err) {
            console.error(err);
            res.set({ 'Cache-Control': 'no-cache' });
            res.status(500).send(`Internal error when getting catalog for ${agency}`);
        }
    }

    async createCatalog(agency) {
        let catalog = {
            "@context": {
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "dcat": "http://www.w3.org/ns/dcat#",
                "dct": "http://purl.org/dc/terms/",
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "foaf": "http://xmlns.com/foaf/0.1/",
                "lc": "http://semweb.mmlab.be/ns/linkedconnections#",
                "gtfs": "http://vocab.gtfs.org/terms#",
                "access": "http://publications.europa.eu/resource/authority/access-right/",
                "Catalog": "dcat:Catalog",
                "Dataset": "dcat:Dataset",
                "Distribution": "dcat:Distribution",
                "Organization": "foaf:Organization",
                "TimePeriod": "dct:PeriodOfTime",
                "name": "foaf:name",
                "label": "rdfs:label",
                "title": "dct:title",
                "description": "dct:description",
                "lastModified": "dct:modified",
                "license": "dct:license",
                "publisher": "dct:publisher",
                "spatial": "dct:spatial",
                "conformsTo": "dct:conformsTo",
                "issued": "dct:issued",
                "temporalRange": "dct:temporal",
                "dataset": "dcat:dataset",
                "keyword": "dcat:keyword",
                "accessURL": "dcat:accessURL",
                "mediaType": "dcat:mediaType",
                "startDate": "dcat:startDate",
                "endDate": "dcat:endDate",
                "accessRights": {
                    "@id": "dcat:accessRights",
                    "@type": "@id"
                },
                "subject": {
                    "@id": "dct:subject",
                    "@type": "@id"
                }
            },
            "@id": `${this.serverConfig.protocol || "http"}://${this.serverConfig.hostname}/${agency}/catalog`,
            "@type": "Catalog",
            "title": `Data service catalog for ${agency}`,
            "label": `Data service catalog for ${agency}`,
            "description": `List of ${agency} data services for Routes, Stops and Linked Connections`,
            "modified": new Date().toISOString(),
            "license": "http://creativecommons.org/publicdomain/zero/1.0/",
            "accessRights": "access:PUBLIC",
            "publisher": {
                "@id": this.organization.id,
                "@type": "Organization",
                "name": this.organization.name
            },
            "dataset": []
        };

        const dataset = this.datasets.find(d => d.companyName === agency);

        // Check that there are Linked Connections for this agency
        if (fs.existsSync(this.storage + '/linked_pages/' + agency)
            && (await readdir(this.storage + '/linked_pages/' + agency)).length > 0) {

            let lcDataset = {
                "@id": `${this.serverConfig.protocol || "http"}://${this.serverConfig.hostname}/${agency}/Connections`,
                "@type": "Dataset",
                "subject": "lc:Connection",
                "description": `Linked Connections dataset for ${agency}`,
                "title": `${agency} Linked Connections`,
                "spatial": dataset.geographicArea || "",
                "keyword": dataset.keywords,
                "conformsTo": "http://linkedconnections.org/specification/1-0",
                "accessRights": "access:PUBLIC",
                "license": "http://creativecommons.org/publicdomain/zero/1.0/",
                "temporalRange": await this.getTemporalRange(agency),
                "dcat:distribution": [await this.getDistribution(agency, 'connections')]
            };

            catalog['dataset'].push(lcDataset);
        }

        // Check that there are Stops for this agency
        if (fs.existsSync(`${this.storage}/stops/${agency}/stops.json`)) {
            let stopsDataset = {
                "@id": `${this.serverConfig.protocol || "http"}://${this.serverConfig.hostname}/${agency}/Stops`,
                "@type": "Dataset",
                "subject": "gtfs:Stop",
                "description": `Linked GTFS Stops data service for ${agency}`,
                "title": `${agency} Linked GTFS Stops`,
                "spatial": dataset.geographicArea || "",
                "keyword": ['GTFS', 'Stops', 'Stations'],
                "conformsTo": "http://vocab.gtfs.org/terms",
                "accessRights": "access:PUBLIC",
                "license": "http://creativecommons.org/publicdomain/zero/1.0/",
                "dcat:distribution": [await this.getDistribution(agency, 'stops')]
            };

            catalog['dataset'].push(stopsDataset);
        }

        // Check that there are Routes for this agency
        if (fs.existsSync(`${this.storage}/routes/${agency}/routes.json`)) {
            let routesDataset = {
                "@id": `${this.serverConfig.protocol || "http"}://${this.serverConfig.hostname}/${agency}/Routes`,
                "@type": "Dataset",
                "subject": "gtfs:Route",
                "description": `Linked GTFS Routes data service for ${agency}`,
                "title": `${agency} Linked GTFS Routes`,
                "spatial": dataset.geographicArea || "",
                "keyword": ['GTFS', 'Routes'],
                "conformsTo": "http://vocab.gtfs.org/terms",
                "accessRights": "access:PUBLIC",
                "license": "http://creativecommons.org/publicdomain/zero/1.0/",
                "dcat:distribution": [await this.getDistribution(agency, 'routes')]
            };

            catalog['dataset'].push(routesDataset);
        }

        return catalog;
    }

    async getTemporalRange(agency) {
        let lp_path = this.storage + '/linked_pages/' + agency;
        let unsorted = (await readdir(lp_path)).map(v => { return new Date(v) });
        let sorted = utils.sortVersions(new Date(), unsorted);
        let startDateFile = (await readdir(lp_path + '/' + sorted[sorted.length - 1].toISOString()))[0];
        let startDate = startDateFile.substring(0, startDateFile.indexOf('.jsonld.gz'));
        let endDateFolder = await readdir(lp_path + '/' + sorted[0].toISOString());
        let endDateFile = endDateFolder[endDateFolder.length - 1];
        let endDateData = (await utils.readAndGunzip(lp_path + '/' + sorted[0].toISOString() + '/' + endDateFile)).split(',\n');
        let endDate = JSON.parse(endDateData[endDateData.length - 1])['departureTime'];

        return {
            "@type": "TimePeriod",
            "startDate": startDate,
            "endDate": endDate
        };
    }

    async getDistribution(agency, type) {
        let dist = {
            "@id": `${this.serverConfig.protocol || "http"}://${this.serverConfig.hostname}/${agency}/${type}`,
            "@type": "Distribution",
            "accessURL": `${this.serverConfig.protocol || "http"}://${this.serverConfig.hostname}/${agency}/${type}`,
            "mediaType": "application/ld+json",
        };

        if (type === 'connections') {
            let lp_path = this.storage + '/linked_pages/' + agency;
            let unsorted = (await readdir(lp_path)).map(v => { return new Date(v) });
            let sorted = utils.sortVersions(new Date(), unsorted);
            dist['issued'] = sorted[0].toISOString();
            dist['mediaType'] = ['application/ld+json', 'application/trig', 'application/n-quads'];
        }

        if (type === 'stops') {
            let stats = fs.statSync(`${this.storage}/stops/${agency}/stops.json`);
            let lastModifiedDate = new Date(util.inspect(stats.mtime));
            dist['issued'] = lastModifiedDate.toISOString();
        }

        if (type === 'routes') {
            let stats = fs.statSync(`${this.storage}/routes/${agency}/routes.json`);
            let lastModifiedDate = new Date(util.inspect(stats.mtime));
            dist['issued'] = lastModifiedDate.toISOString();
        }

        return dist;
    }

    get organization() {
        return this._organization;
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