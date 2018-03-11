const util = require('util');
const fs = require('fs');
const zlib = require('zlib');
const unzip = require('unzip');

const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);

module.exports = new class Utils {

    constructor() {
        this._datasetsConfig = JSON.parse(fs.readFileSync('./datasets_config.json', 'utf8'));
        this._serverConfig = JSON.parse(fs.readFileSync('./server_config.json', 'utf8'));
        this._staticFragments = {};
        this.updateStaticFragments();
    }

    updateStaticFragments() {
        return new Promise((resolve, reject) => {
            try {
                let storage = this._datasetsConfig.storage + '/linked_pages/';
                let datasets = this._datasetsConfig.datasets;
                let dc = 0;
                datasets.forEach(async dataset => {
                    let companyName = dataset.companyName;
                    if (!this._staticFragments[companyName]) this._staticFragments[companyName] = {};
                    let versions = await readdir(storage + companyName);
                    let vc = 0;
                    versions.forEach(async v => {
                        if (!this._staticFragments[companyName][v]) {
                            this._staticFragments[companyName][v] = (await readdir(storage + companyName + '/' + v)).map(fragment => {
                                let fd = new Date(fragment.substring(0, fragment.indexOf('.jsonld')))
                                return fd.getTime();
                            });
                        }
                        vc++;
                        if (vc === versions.length) {
                            dc++;
                            if (dc === datasets.length) resolve();
                        }
                    });
                    if (versions.length === 0) dc++;
                    if (dc === datasets.length) resolve();
                });
            } catch (err) {
                reject(err);
            }
        });
    }

    readAndGunzip(path) {
        return new Promise((resolve, reject) => {
            let buffer = [];
            fs.createReadStream(path)
                .pipe(new zlib.createGunzip())
                .on('error', err => {
                    reject(err + ' - ocurred on file: ' + path);
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
            let dirName = path;
            if (dirName.endsWith('.zip')) {
                dirName = dirName.substring(0, dirName.indexOf('.zip')) + '_tmp';
            } else {
                path += '.zip';
            }
            fs.createReadStream(path)
                .pipe(unzip.Extract({ path: dirName }))
                .on('close', () => {
                    resolve(dirName);
                });
        });
    }

    // Sort array of dates ordered from the closest to farest to a given date.
    sortVersions(date, versions) {
        let diffs = [];
        let sorted = [];

        for (let v of versions) {
            let diff = Math.abs(date.getTime() - new Date(v).getTime());
            diffs.push({ 'version': v, 'diff': diff });
        }

        diffs.sort((a, b) => {
            return a.diff - b.diff;
        });

        for (let d of diffs) {
            sorted.push(d.version);
        }

        return sorted;
    }

    findResource(agency, targetDate, versions) {
        let version = null;
        let fragment = null;
        let index = null;

        for (let i = 0; i < versions.length; i++) {
            let fragments = this._staticFragments[agency][versions[i]];
            let min = 0;
            let max = fragments.length - 1;
            let target = targetDate.getTime();
            let found = false;

            // Checking that target date is contained in the list of fragments.
            if (target >= fragments[min] && target <= fragments[max]) {
                // Perform binary search to find the fragment that contains the target date.
                while (!found) {
                    // Divide the array in half
                    let mid = Math.floor((min + max) / 2);
                    // Target date is in the right half
                    if (target > fragments[mid]) {
                        if (target < fragments[mid + 1]) {
                            index = mid;
                            found = true;
                        } else if (target === fragments[mid + 1]) {
                            index = mid + 1;
                            found = true;
                        } else {
                            // Not found yet proceed to divide further this half in 2.
                            min = mid;
                        }
                    // Target date is exactly equals to the middle fragment
                    } else if (target === fragments[mid]) {
                        index = mid;
                        found = true;
                    // Target date is on the left half
                    } else {
                        if (target >= fragments[mid - 1]) {
                            index = mid - 1;
                            found = true;
                        } else {
                            max = mid;
                        }
                    }
                }
            }

            if (found) {
                // Register in which static version was the target found
                version = versions[i];
                // Name of the fragment that contains the target
                fragment = new Date(fragments[index]);
                break;
            }
        }

        if (version !== null && fragment !== null) {
            return [version, fragment, index];
        } else {
            throw new Error('Fragment not found among current data');
        }
    }

    aggregateRTData(static_data, rt_data, agency, queryTime, timestamp) {
        //return new Promise((resolve, reject) => {
        // Index map for the static fragment
        let static_index = this.getStaticIndex(static_data);
        // Index map for the rt fragment
        let rt_index = this.getRTIndex(rt_data, timestamp);
        // Iterate over the RT index which contains all the connections that need to be updated or included
        for (let [connId, index] of rt_index) {
            // If the connection is already present in the static fragment just add/update delay values
            if (static_index.has(connId)) {
                let std = static_data[static_index.get(connId)];
                let rtd = rt_data[index];
                std['departureDelay'] = rtd['departureDelay'];
                std['arrivalDelay'] = rtd['arrivalDelay'];
                static_data[static_index.get(connId)] = std;
            } else {
                // Is not present in the static fragment which means it's a new connection so inlcude it at the end.
                let rtd = rt_data[index];
                delete rtd['mementoVersion'];
                static_data.push(rtd);
            }
        }

        // Re-sort the fragment with the updated delay data
        static_data.sort((a, b) => {
            let a_date = new Date(a['departureTime']).getTime();
            let b_date = new Date(b['departureTime']).getTime();
            return a_date - b_date;
        });

        return static_data;

        // Check if there are connections with delays reported in future fragments
        /*let future_check = [];
        let promises = [];

        // Gather all connections that may have delays reported in future fragments
        for (let x in static_data) {
            if (typeof static_data[x]['departureDelay'] === 'undefined') {
                future_check.push(static_data[x]);
            }
        }

        // Asynchronously check next 12 fragments for delays
        for (let i = 0; i < 12; i++) {
            queryTime.setMinutes(queryTime.getMinutes() + 10);
            promises.push(this.checkRTFragment(agency, queryTime, future_check, timestamp));
        }

        Promise.all(promises).then(remove => {
            // Remove all connections with delays reported in future fragments
            let r = [].concat.apply([], remove);
            for (let y in r) {
                static_data.splice(static_index.get(r[y]), 1);
            }

            // Re-sort the fragment with the updated delay data
            static_data.sort((a, b) => {
                let a_date = new Date(a['departureTime']).getTime();
                let b_date = new Date(b['departureTime']).getTime();
                return a_date - b_date;
            });

            resolve(static_data);
        });*/
        // });
    }

    checkRTFragment(agency, fragment, conns, memento) {
        return new Promise(async (resolve) => {
            let path = this.datasetsConfig.storage + '/real_time/' + agency + '/';
            if (fs.existsSync(path + fragment.toISOString() + '.jsonld.gz')) {
                let rt_fragment = (await this.readAndGunzip(path + fragment.toISOString() + '.jsonld.gz')).join('').split('\n').map(JSON.parse);
                let rt_index = this.getRTIndex(rt_fragment, memento);
                let toRemove = [];

                for (let y in conns) {
                    let id = conns[y]['@id'];
                    if (rt_index.has(id)) {
                        toRemove.push(id);
                    }
                }

                resolve(toRemove);
            } else {
                resolve([]);
            }
        });
    }

    getStaticIndex(fragment) {
        try {
            let map = new Map();
            for (let x in fragment) {
                let conn = fragment[x];
                map.set(conn['@id'], x);
            }
            return map;
        } catch (err) {
            throw err;
        }
    }

    getRTIndex(array, timeCriteria) {
        let map = new Map();
        for (let i in array) {
            try {
                let jo = array[i];
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

            jsonld_skeleton['@id'] = host + agency + '/connections?departureTime=' + departureTime.toISOString();

            let next = this._staticFragments[agency][version][params.index + 1];
            if (next) {
                jsonld_skeleton['hydra:next'] = host + agency + '/connections?departureTime=' + new Date(next).toISOString();
            }

            let prev = this._staticFragments[agency][version][params.index - 1];
            if (prev !== null) {
                jsonld_skeleton['hydra:previous'] = host + agency + '/connections?departureTime=' + new Date(prev).toISOString();
            }

            jsonld_skeleton['hydra:search']['hydra:template'] = host + agency + '/connections/{?departureTime}';
            jsonld_skeleton['@graph'] = params.data;

            params.http_response.set(params.http_headers);
            params.http_response.json(jsonld_skeleton);

        } catch (err) {
            console.error(err);
            throw err;
        }
    }

    getRTDirName(date) {
        return date.getFullYear() + '_' + (date.getUTCMonth() + 1) + '_' + date.getUTCDate();
    }

    get datasetsConfig() {
        return this._datasetsConfig;
    }

    get serverConfig() {
        return this._serverConfig;
    }

    get staticFragments() {
        return this._staticFragments;
    }
}
