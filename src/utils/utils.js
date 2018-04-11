const util = require('util');
const fs = require('fs');
const zlib = require('zlib');
const unzip = require('unzip');
const md5 = require('md5');
const logger = require('./logger');

const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);

module.exports = new class Utils {

    constructor() {
        this._datasetsConfig = JSON.parse(fs.readFileSync('./datasets_config.json', 'utf8'));
        this._serverConfig = JSON.parse(fs.readFileSync('./server_config.json', 'utf8'));
        this._staticFragments = {};
    }

    async updateStaticFragments() {
        let storage = this._datasetsConfig.storage + '/linked_pages/';
        let datasets = this._datasetsConfig.datasets;

        for (let i in datasets) {
            let companyName = datasets[i].companyName;
            if (fs.existsSync(storage + companyName)) {
                if (!this._staticFragments[companyName]) {
                    this._staticFragments[companyName] = {};
                }

                let versions = await readdir(storage + companyName);
                for (let y in versions) {
                    if (!this._staticFragments[companyName][versions[y]]) {
                        let dir = await readdir(storage + companyName + '/' + versions[y]);
                        this._staticFragments[companyName][versions[y]] = dir.map(fragment => {
                            let fd = new Date(fragment.substring(0, fragment.indexOf('.jsonld')))
                            return fd.getTime();
                        });
                    } else {
                        continue;
                    }
                }
            }
        }
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

    // Search for a given fragment across the different static versions
    findResource(agency, target, versions) {
        let version = null;
        let fragment = null;

        for (let i = 0; i < versions.length; i++) {
            let fragments = this.staticFragments[agency][versions[i]];

            // Checking that target date is contained in the list of fragments.
            if (target >= fragments[0] && target <= fragments[fragments.length - 1]) {
                fragment = this.binarySearch(target, fragments);
                version = [versions[i]];
                break;
            } else {
                continue;
            }
        }

        if (version !== null && fragment !== null) {
            return version.concat(fragment);
        } else {
            throw new Error('Fragment not found in current data');
        }
    }

    // Binary search algorithm to find the closest element from the left to a given target, in a sorted numeric array.
    // If the target is not contained in the array it returns null. 
    binarySearch(target, array) {
        let min = 0;
        let max = array.length - 1;
        let index = null;

        // Checking that target is contained in the array.
        if (target >= array[min] && target <= array[max]) {
            // Perform binary search to find the closest, rounded down element to the target in the array .
            while (index === null) {
                // Divide the array in half
                let mid = Math.floor((min + max) / 2);
                // Target date is in the right half
                if (target > array[mid]) {
                    if (target < array[mid + 1]) {
                        index = mid;
                    } else if (target === array[mid + 1]) {
                        index = mid + 1;
                    } else {
                        // Not found yet proceed to divide further this half in 2.
                        min = mid;
                    }
                    // Target date is exactly equal to the middle element
                } else if (target === array[mid]) {
                    index = mid;
                    // Target date is on the left half
                } else {
                    if (target >= array[mid - 1]) {
                        index = mid - 1;
                    } else {
                        max = mid;
                    }
                }
            }
        } else {
            return null;
        }

        return [array[index], index];
    }

    async aggregateRTData(static_data, rt_data, remove_path, timestamp) {
        // Index map for the static fragment
        let static_index = this.getStaticIndex(static_data);
        // Index map for the rt fragment
        let rt_index = this.getRTIndex(rt_data, timestamp);
        // Array of the Connections that must be removed from static fragment due to delays
        let to_remove = await this.getConnectionsToRemove(remove_path, timestamp);

        // Iterate over the RT index which contains all the connections that need to be updated or included
        for (let [connId, index] of rt_index) {
            // If the connection is already present in the static fragment just add/update delay values
            if (static_index.has(connId)) {
                let std = static_data[static_index.get(connId)];
                let rtd = JSON.parse(rt_data[index]);
                std['departureDelay'] = rtd['departureDelay'];
                std['arrivalDelay'] = rtd['arrivalDelay'];
                // Update departure and arrival times with delays
                std['departureTime'] = new Date(new Date(std['departureTime']).getTime() + (Number(rtd['departureDelay'])) * 1000).toISOString();
                std['arrivalTime'] = new Date(new Date(std['arrivalTime']).getTime() + (Number(rtd['arrivalDelay'])) * 1000).toISOString();
                static_data[static_index.get(connId)] = std;
            } else {
                // Is not present in the static fragment which means it's a new connection so inlcude it at the end.
                let rtd = JSON.parse(rt_data[index]);
                delete rtd['mementoVersion'];
                static_data.push(rtd);
            }
        }

        // Now iterate over the array of connections that need to be removed from the static fragment due to delays and remove them
        if (to_remove !== null) {
            for (let c in to_remove) {
                let si = static_index.get(to_remove[c]);
                static_data.splice(si, 1);
            }
        }

        // Re-sort the fragment with the updated delay data
        static_data.sort((a, b) => {
            let a_date = new Date(a['departureTime']).getTime();
            let b_date = new Date(b['departureTime']).getTime();
            return a_date - b_date;
        });

        return static_data;
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
        let lastObj = array[array.length - 1].split('"');
        let lastMemento = new Date(lastObj[43]);
        let map = new Map();

        // Last update is being requested
        if (timeCriteria >= lastMemento) {
            // Register last update in the index already
            map.set(lastObj[3], (array.length - 1));
            // Iterate in reverse over the rt data getting only the last update according to memento
            for (let i = array.length - 2; i >= 0; i--) {
                let obj = array[i].split('"');
                let memento_date = new Date(obj[43]);
                if (memento_date.getTime() === lastMemento.getTime()) {
                    map.set(obj[3], i);
                } else {
                    break;
                }
            }
        } else {
            let index = 0;
            // Find the closest previous update to the requested memento
            for (let i = 0; i < array.length; i++) {
                let obj = array[i].split('"');
                let memento_date = new Date(obj[43]);
                if (timeCriteria > memento_date) {
                    index = i - 1;
                    break;
                }
            }

            if (index > 0) {
                let closestObj = array[index].split('"');
                let closestMemento = new Date(closestObj[43]);
                // Iterate in reverse over the rt data getting only the closest updates according to memento
                for (let i = index; i >= 0; i--) {
                    let obj = array[i].split('"');
                    let memento_date = new Date(obj[43]);
                    if (memento_date.getTime() === closestMemento.getTime()) {
                        map.set(obj[3], i);
                    } else {
                        break;
                    }
                }
            }
        }

        return map;
    }

    async getConnectionsToRemove(path, timestamp) {
        if (fs.existsSync(path)) {
            let remove_list = null;
            if (path.endsWith('.gz')) {
                remove_list = (await this.readAndGunzip(path)).toString().split('\n');
            } else {
                remove_list = (await readFile(path)).toString().split('\n');
            }
            // Remove empty space at the end of the array
            remove_list.pop();

            // Check if what is being requested is the last update
            let last_update = new Date(Object.keys(JSON.parse(remove_list[remove_list.length - 1]))[0]).getTime();
            if (timestamp.getTime() >= last_update) {
                // Since updates are in chronological order get and return the last element
                let last_obj = JSON.parse(remove_list[remove_list.length - 1]);
                return last_obj[Object.keys(last_obj)[0]];
            } else {
                // Use binary search algorithm to determine the closest update to what is being requested
                let versions = [];
                for (let i in remove_list) {
                    versions.push(Object.keys(JSON.parse(remove_list[i]))[0]);
                }
                versions = versions.map(v => (new Date(v).getTime()));
                let closest_version = this.binarySearch(timestamp.getTime(), versions);
                if (closest_version !== null) {
                    let obj = JSON.parse(remove_list[closest_version[1]]);
                    return obj[Object.keys(obj)[0]];
                } else {
                    return null;
                }
            }
        } else {
            return null;
        }
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

            let next = this._staticFragments[agency][version][Number(params.index) + 1];
            if (next) {
                jsonld_skeleton['hydra:next'] = host + agency + '/connections?departureTime=' + new Date(next).toISOString();
            }

            let prev = this._staticFragments[agency][version][Number(params.index) - 1];
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

    /**
    * Checks for Conditional GET requests, by comparing the last modified date and file hash against if-modified-since and if-none-match headers.
    * If a match is made, a 304 response is sent and the function will return true.
    * If the client needs a new version (ie a body should be sent), the function will return false
    * @param req
    * @param res
    * @param filepath The path of the file which would be used to generate the response
    * @param departureTime The time for which this document was requested
    * @returns boolean True if a 304 response is to be served
    */
    handleConditionalGET(req, res, filepath, departureTime) {
        let ifModifiedSinceRawHeader = req.header('if-modified-since');

        let ifModifiedSinceHeader = undefined;
        if (ifModifiedSinceRawHeader !== undefined) {
            ifModifiedSinceHeader = new Date(ifModifiedSinceRawHeader);
        }

        let ifNoneMatchHeader = req.header('if-none-match');

        let stats = fs.statSync(filepath);
        let lastModifiedDate = new Date(util.inspect(stats.mtime));

        let now = new Date();

        // Valid until :31 or :01
        let validUntilDate = new Date();
        validUntilDate.setSeconds(validUntilDate.getSeconds() - (validUntilDate.getSeconds() % 30) + 31);

        let maxage = (validUntilDate - now) / 1000;
        let etag = 'W/"' + md5(filepath + lastModifiedDate) + '"';

        // If both departure time and last modified lie resp. 2 and 1 hours in the past, this becomes immutable
        if (departureTime < (now - 7200000) && lastModifiedDate < (now - 3600000)) {
            // Immutable (for browsers which support it, sometimes limited to https only
            // 1 year expiry date to keep it long enough in cache for the others
            res.set({ 'Cache-control': 'public, max-age=31536000000, immutable' });
            res.set({ 'Expires': new Date(now + 31536000000).toUTCString() });
        } else {
            // Let clients hold on to this data for 1 second longer than nginx. This way nginx can update before the clients?
            res.set({ 'Cache-control': 'public, s-maxage=' + maxage + ', max-age=' + (maxage + 1) + ',stale-if-error=' + (maxage + 15) + ', proxy-revalidate' });
            res.set({ 'Expires': validUntilDate.toUTCString() });
        }

        res.set({ 'ETag': etag });
        res.set({ 'Vary': 'Accept-encoding, Accept-Datetime' });
        res.set({ 'Last-Modified': lastModifiedDate.toUTCString() });
        res.set({ 'Content-Type': 'application/ld+json' });

        // If an if-modified-since header exists, and if the realtime data hasn't been updated since, just return a 304/
        // If an if-none-match header exists, and if the realtime data hasn't been updated since, just return a 304/
        if ((lastModifiedDate !== undefined && ifModifiedSinceHeader !== undefined && ifModifiedSinceHeader >= lastModifiedDate) ||
            (ifNoneMatchHeader !== undefined && ifNoneMatchHeader === etag)) {
            res.status(304).send();
            return true;
        }

        return false;
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
