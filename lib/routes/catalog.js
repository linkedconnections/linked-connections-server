const util = require('util');
const fs = require('fs');
var utils = require('../utils/utils');

const readdir = util.promisify(fs.readdir);
const writeFile = util.promisify(fs.writeFile);

const storage = utils.datasetsConfig.storage;
const datasets = utils.datasetsConfig.datasets;
const server_config = utils.serverConfig;

class Catalog {
    async getCatalog(req, res) {
        res.set({
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            'Content-Type': 'application/ld+json'
        });
        
        if (!fs.existsSync(storage + '/datasets/catalog.json')) {
            let catalog = await this.createCatalog();
            this.saveCatalog(catalog);
            res.json(catalog);
        } else {
            res.json(JSON.parse(fs.readFileSync(storage + '/datasets/catalog.json')));
        }
    }

    saveCatalog(catalog) {
        writeFile(storage + '/datasets/catalog.json', JSON.stringify(catalog), 'utf8');
    }

    async createCatalog() {
        let catalog = {
            "@context": {
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "dcat": "http://www.w3.org/ns/dcat#",
                "dct": "http://purl.org/dc/terms/",
                "foaf": "http://xmlns.com/foaf/0.1/",
                "owl": "http://www.w3.org/2002/07/owl#",
                "schema": "http://schema.org/",
                "access": "http://publications.europa.eu/resource/authority/access-right/",
                "dct:modified": {
                    "@type": "xsd:dateTime"
                },
                "dct:rights": {
                    "@type": "@id"
                },
                "dct:accessRights": {
                    "@type": "@id"
                },
                "dct:issued": {
                    "@type": "xsd:dateTime"
                },
                "dct:spatial": {
                    "@type": "@id"
                },
                "dct:license": {
                    "@type": "@id"
                },
                "dct:conformsTo": {
                    "@type": "@id"
                },
                "dcat:mediaType": {
                    "@container": "@set"
                },
                "schema:startDate": {
                    "@type": "xsd:dateTime"
                },
                "schema:endDate": {
                    "@type": "xsd:dateTime"
                }
            },
            "@id": (server_config.protocol || "http") + "://" + server_config.hostname + "/catalog",
            "@type": "dcat:Catalog",
            "dct:title": "Catalog of Linked Connection datasets",
            "dct:description": "Catalog of Linked Connection datasets published by " + utils.datasetsConfig.organization.name,
            "dct:modified": new Date().toISOString(),
            "dct:license": "http://creativecommons.org/publicdomain/zero/1.0/",
            "dct:rights": "access:PUBLIC",
            "dct:publisher": {
                "@id": utils.datasetsConfig.organization.id,
                "@type": "foaf:Organization",
                "foaf:name": utils.datasetsConfig.organization.name
            },
            "dcat:dataset": []
        };

        await Promise.all(datasets.map(async dataset => {
            // Check there is existing data about this dataset
            if (fs.existsSync(storage + '/linked_pages/' + dataset.companyName)
                && (await readdir(storage + '/linked_pages/' + dataset.companyName)).length > 0) {

                let dcatDataset = {
                    "@id": (server_config.protocol || "http") + "://" + server_config.hostname + "/" + dataset.companyName + "/Connections",
                    "@type": "dcat:Dataset",
                    "dct:description": "Linked Connections dataset for " + dataset.companyName,
                    "dct:title": dataset.companyName + " Linked Connections",
                    "dct:spatial": dataset.geographicArea || "",
                    "dcat:keyword": dataset.keywords,
                    "dct:conformsTo": "http://linkedconnections.org/specification/1-0",
                    "dct:accessRights": "access:PUBLIC",
                    "dcat:distribution": [await this.getDistribution(dataset, 'connections')]
                };

                let stopsDataset = {
                    "@id": (server_config.protocol || "http") + "://" + server_config.hostname + "/" + dataset.companyName + "/Stops",
                    "@type": "dcat:Dataset",
                    "dct:description": "Stops dataset for " + dataset.companyName,
                    "dct:title": dataset.companyName + " stops",
                    "dct:spatial": dataset.geographicArea || "",
                    "dcat:keyword": ['Stops', 'Stations'],
                    "dct:accessRights": "access:PUBLIC",
                    "dcat:distribution": [await this.getDistribution(dataset, 'stops')]
                };

                catalog['dcat:dataset'].push(dcatDataset);
                catalog['dcat:dataset'].push(stopsDataset);
            }
        }));

        return catalog;
    }

    async getDistribution(dataset, type) {
        let lp_path = storage + '/linked_pages/' + dataset.companyName;
        let unsorted = (await readdir(lp_path)).map(v => {
            return new Date(v);
        });
        let sorted = utils.sortVersions(new Date(), unsorted);

        let dist = {
            "@id": (server_config.protocol || "http") + "://" + server_config.hostname + "/" + dataset.companyName + "/" + type,
            "@type": "dcat:Distribution",
            "dcat:accessURL": (server_config.protocol || "http") + "://" + server_config.hostname + "/" + dataset.companyName + "/" + type,
            "dct:spatial": (dataset.geographicArea || ""),
            "dct:license": "http://creativecommons.org/publicdomain/zero/1.0/",
            "dcat:mediaType": ['application/ld+json', 'text/turtle', 'application/trig', 'application/n-triples'],
            "dct:issued": sorted[sorted.length - 1].toISOString(),
            "dct:modified": sorted[0].toISOString()
        };

        if (type === 'connections') {
            let startDateFile = (await readdir(lp_path + '/' + sorted[sorted.length - 1].toISOString()))[0];
            let startDate = startDateFile.substring(0, startDateFile.indexOf('.jsonld.gz'));

            let endDateFolder = await readdir(lp_path + '/' + sorted[0].toISOString());
            let endDateFile = endDateFolder[endDateFolder.length - 1];
            let endDateData = (await utils.readAndGunzip(lp_path + '/' + sorted[0].toISOString() + '/' + endDateFile)).split(',\n');
            let endDate = JSON.parse(endDateData[endDateData.length - 1])['departureTime'];
            dist['schema:startDate'] = startDate;
            dist['schema:endDate'] = endDate;
        }

        return dist;
    }
}

module.exports = Catalog;