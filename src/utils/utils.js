const util = require('util');
const fs = require('fs');
const zlib = require('zlib');

const readFile = util.promisify(fs.readFile);

module.exports = new class Utils {

    constructor() {
        this._datasetsConfig = JSON.parse(fs.readFileSync('./datasets_config.json', 'utf8'));
        this._serverConfig = JSON.parse(fs.readFileSync('./server_config.json', 'utf8'));
    }

    readAndGunzip(path) {
        return new Promise((resolve, reject) => {
            let buffer = [];
            fs.createReadStream(path)
                .pipe(new zlib.createGunzip())
                .on('error', err => {
                    reject(err);
                })
                .on('data', data => {
                    buffer.push(data);
                })
                .on('end', () => {
                    resolve(buffer);
                });
        });
    }

    readAndUnzip(path) {
        return new Promise((resolve, reject) => {
            fs.createReadStream(path + '.zip')
                .pipe(unzip.Extract({ path: path + '_tmp' }))
                .on('close', () => {
                    resolve();
                });
        });
    }

    getIndexedMap(array, timeCriteria) {
        let map = new Map();
        for (let i = 0; i < array.length; i++) {
            try {
                let jo = JSON.parse(array[i]);
                let memento_date = new Date(jo['mementoVersion']);
                if (memento_date <= timeCriteria) {
                    map.set(jo['@id'], i);
                } else {
                    break;
                }
            } catch (err) {
                continue;
            }
        }
        return map;
    }

    async addHydraMetada(params) {
        try {
            let template = await readFile('./statics/skeleton.jsonld', { encoding: 'utf8' });
            let jsonld_skeleton = JSON.parse(template);
            let host = params.host;
            let agency = params.agency;
            let departureTime = params.departureTime;
            let version = params.version;
            let jsonld_graph = params.data;

            jsonld_skeleton['@id'] = host + agency + '/connections?departureTime=' + departureTime.toISOString();
            jsonld_skeleton['hydra:next'] = host + agency + '/connections?departureTime=' 
                + this.getAdjacentPage(params.storage, agency + '/' + version, departureTime, true);
            jsonld_skeleton['hydra:previous'] = host + agency + '/connections?departureTime=' 
                + this.getAdjacentPage(params.storage, agency + '/' + version, departureTime, false);
            jsonld_skeleton['hydra:search']['hydra:template'] = host + agency + '/connections/{?departureTime}';
    
            for (let i in jsonld_graph) {
                jsonld_skeleton['@graph'].push(JSON.parse(jsonld_graph[i]));
            }
    
            params.http_response.set(params.http_headers);
            params.http_response.json(jsonld_skeleton);
    
        } catch(err) {
            console.error(err);
            throw err;
        }
    }

    // TODO: Make fragmentation criteria configurable
    getAdjacentPage(storage, path, departureTime, next) {
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

    get datasetsConfig() {
        return this._datasetsConfig;
    }

    get serverConfig() {
        return this._serverConfig;
    }
}