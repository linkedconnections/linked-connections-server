const util = require('util');
const express = require('express');
const fs = require('fs');
const csv = require('fast-csv');
const uri_templates = require('uri-templates');
const del = require('del');
const Logger = require('../utils/logger');
var utils = require('../utils/utils');

const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);

const storage = utils.datasetsConfig.storage;
const datasets = utils.datasetsConfig.datasets;
const server_config = utils.serverConfig;

class Stops {
    async getStops(req, res) {
        const agency = req.params.agency;
        if (fs.existsSync(storage + '/stops/' + agency + '/stops.json')) {
            res.set({
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Content-Type': 'application/ld+json'
            });
            res.send(await readFile(storage + '/stops/' + agency + '/stops.json', 'utf8'));
            return;
        } else {
            let stops = await this.createStopList(agency);
            if (stops != null) {
                res.set({
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': '*',
                    'Content-Type': 'application/ld+json'
                });
                res.send(stops);
                return;
            } else {
                res.set({'Cache-Control': 'no-cache'});
                res.status(404).send("No stops available for " + agency);
            }
        }
    }

    createStopList(company) {
        return new Promise(async (resolve, reject) => {
            let dataset = this.getDataset(company);
            let feeds = await readdir(storage + '/datasets/' + company);
            if (feeds.length > 0) {
                let skeleton = {
                    "@context": {
                        "dct": "http://purl.org/dc/terms/",
                        "schema": "http://schema.org/",
                        "name": "http://xmlns.com/foaf/0.1/name",
                        "longitude": "http://www.w3.org/2003/01/geo/wgs84_pos#long",
                        "latitude": "http://www.w3.org/2003/01/geo/wgs84_pos#lat",
                        "dct:spatial": {
                            "@type": "@id"
                        },
                    },
                    "@id": (server_config.protocol || "http") + "://" + server_config.hostname + "/" + company + '/stops',
                    "@graph": []
                };
                let feed = feeds[feeds.length - 1];
                let uncompressed = await utils.readAndUnzip(storage + '/datasets/' + company + '/' + feed);
                let stops_uri_template = uri_templates(dataset['baseURIs']['stop']);

                fs.createReadStream(uncompressed + '/stops.txt', { encoding: 'utf8', objectMode: true })
                    .pipe(csv({ objectMode: true, headers: true }))
                    .on('data', data => {
                        skeleton['@graph'].push({
                            "@id": stops_uri_template.fill({ [stops_uri_template.varNames[0]]: data[stops_uri_template.varNames[0].split('.')[1]].trim() }),
                            "dct:spatial": dataset['geographicArea'] || "",
                            "latitude": data['stop_lat'].trim(),
                            "longitude": data['stop_lon'].trim(),
                            "name": data['stop_name'].trim(),
                        })
                    }).on('end', async () => {
                        writeFile(storage + '/stops/' + company + '/stops.json', JSON.stringify(skeleton), 'utf8');
                        await del([uncompressed], { force: true });
                        resolve(skeleton);
                    });
            } else {
                resolve(null);
            }
        });
    }

    getDataset(name) {
        for (let i in datasets) {
            if (datasets[i].companyName === name) {
                return datasets[i];
            }
        }
    }
}

module.exports = Stops;