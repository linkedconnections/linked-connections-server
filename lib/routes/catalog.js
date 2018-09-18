const util = require('util');
const fs = require('fs');
var utils = require('../utils/utils');

const readdir = util.promisify(fs.readdir);
const readFile = util.promisify(fs.readFile);
const writeFile = util.promisify(fs.writeFile);

const storage = utils.datasetsConfig.storage;
const datasets = utils.datasetsConfig.datasets;
const server_config = utils.serverConfig;

class Catalog {
    async getCatalog(req, res) {
        let catalog = null;
        if(!fs.existsSync(storage + '/datasets/catalog.json' )) {
            catalog = await this.createCatalog();
        } else {
            catalog = JSON.parse(await readFile(storage + '/datasets/catalog.json'));
        }

        this.saveCatalog(catalog);
        res.json(catalog); 
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
                "dct:modified": {
                    "@type": "xsd:dateTime"
                },
                "dct:issued": {
                    "@type": "xsd:dateTime"
                },
                "dct:spatial" : {
                    "@type": "@id"
                },
                "dct:license": {
                    "@type": "@id"
                },
                "dct:conformsTo": {
                    "@type": "@id"
                },
                "dcat:mediaType": {
                    "@type": "xsd:string"
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
            "dct:rights": "public",
            "dct:publisher": {
                "@id": utils.datasetsConfig.organization.id,
                "@type": "foaf:Organization",
                "foaf:name": utils.datasetsConfig.organization.name
            },
            "dcat:dataset": []
        };

        await Promise.all(datasets.map(async dataset => {
            // Check there is existing data about this dataset
            if(fs.existsSync(storage + '/linked_pages/' + dataset.companyName) 
                && (await readdir(storage + '/linked_pages/' + dataset.companyName)).length > 0) {
                let dctSpatial = (dataset.geographicArea && dataset.geographicArea.coverage) ? dataset.geographicArea.coverage : dataset.geographicArea;

                let dcatDataset = {
                    "@id": (server_config.protocol || "http") + "://" + server_config.hostname + "/" + dataset.companyName,
                    "@type": "dcat:Dataset",
                    "dct:description": "Linked Connections dataset for " + dataset.companyName,
                    "dct:title": dataset.companyName + " Linked Connections",
                    "dct:spatial": dctSpatial || "",
                    "dcat:keyword": dataset.keywords,
                    "dct:conformsTo": "http://linkedconnections.org/specification/1-0",
                    "dct:accessRights": "public",
                    "dcat:distribution": await this.getDistributions(dataset)
                };

                catalog['dcat:dataset'].push(dcatDataset);
            }
        }));

        return catalog;
    }

    async getDistributions(dataset) {
        let dists = [];
        let versions = await readdir(storage + '/linked_pages/' + dataset.companyName);

        // Check if sub areas are defined for this dataset
        if(dataset.geographicArea.subAreas && dataset.geographicArea.subAreas.length > 0) {
            let subAreas = dataset.geographicArea.subAreas;
            for(let v in versions) {
                for(let sa in subAreas) {
                    // Check if there is data for this sub area
                    if(fs.existsSync(storage + '/linked_pages/' + dataset.companyName + '/' + versions[v] + '/' + subAreas[sa]['name'])) {
                        let [start, end] = await this.getRangeDates(dataset, versions[v], subAreas[sa]['name']);
                        let dist = {
                            "@id": (server_config.protocol || "http") + "://" + server_config.hostname + "/" + dataset.companyName + "/" 
                                + subAreas[sa]['name'] + "/" + versions[v],
                            "@type": "dcat:Distribution",
                            "dcat:accessURL": (server_config.protocol || "http") + "://" + server_config.hostname + "/" + dataset.companyName + "/" 
                                + subAreas[sa]['name'] + "/connections",
                            "dct:spatial": subAreas[sa]['coverage'],
                            "dct:license": "http://creativecommons.org/publicdomain/zero/1.0/",
                            "dcat:mediaType": "application/ld+json",
                            "dct:issued": versions[v],
                            "schema:startDate": start,
                            "schema:endDate": end
                        };
        
                        dists.push(dist);
                    }
                }
            }
        } else {
            for(let v in versions) {
                let [start, end] = await this.getRangeDates(dataset, versions[v]);
                let dist = {
                    "@id": (server_config.protocol || "http") + "://" + server_config.hostname + "/" + dataset.companyName + "/" + versions[v],
                    "@type": "dcat:Distribution",
                    "dcat:accessURL": (server_config.protocol || "http") + "://" + server_config.hostname + "/" + dataset.companyName + "/connections",
                    "dct:license": "http://creativecommons.org/publicdomain/zero/1.0/",
                    "dcat:mediaType": "application/ld+json",
                    "dct:issued": versions[v],
                    "schema:startDate": start,
                    "schema:endDate": end
                };

                dists.push(dist);
            }
        }

        return dists;
    }

    async getRangeDates(dataset, version, zone) {
        let path = null;

        if(!zone) {
            path = storage + '/linked_pages/' + dataset.companyName + '/' + version;
        } else {
            path = storage + '/linked_pages/' + dataset.companyName + '/' + version + '/' + zone;
        }

        let pages = await readdir(path);

        let start = pages[0].substring(0, pages[0].indexOf('.jsonld.gz'));
        let lastPage = (await utils.readAndGunzip(path + '/' + pages[pages.length - 1])).join('').split(',\n').map(JSON.parse);
        let end = lastPage[lastPage.length - 1]['departureTime'];

        return [start, end];
    }
}

module.exports = Catalog;