const util = require('util');
const fs = require('fs');
const { request } = require('undici');
const zlib = require('zlib');
const unzip = require('unzipper');
const md5 = require('md5');
const jsonld = require('jsonld');
const Logger = require('./logger');
const cronParser = require('cron-parser');
const N3 = require('n3');
const { DataFactory } = N3;
const { namedNode, literal, blankNode, defaultGraph, quad } = DataFactory;

const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);
var logger = null;

module.exports = new class Utils {

    constructor() {
        if (fs.existsSync('./datasets_config.json') && fs.existsSync('./server_config.json')) {
            this._datasetsConfig = JSON.parse(fs.readFileSync('./datasets_config.json', 'utf8'));
            this._serverConfig = JSON.parse(fs.readFileSync('./server_config.json', 'utf8'));
        } else {
            this._datasetsConfig = {};
            this._serverConfig = {};
        }
        logger = Logger.getLogger(this._serverConfig.logLevel || 'info');
    }

    async download(url, headers) {
        const checkUrl = new URL(url);
        if (["http:", "https:"].includes(checkUrl.protocol)) {
            const res = await request(url, {
                method: "GET",
                headers,
                maxRedirections: 5
            });

            if (res.statusCode <= 400) {
                logger.debug(`Downloading ${url} ...`);
                return res;
            } else {
                throw new Error(`HTTP error (${res.statusCode}) while requesting: ${url}`);
            }
        } else {
            throw new Error(`Invalid URL: ${url}`);
        }

    }

    downloadGTFSToDisk(url, headers, path) {
        return new Promise(async (resolve, reject) => {
            try {
                const res = await this.download(url, headers);
                const lastModified = res.headers['last-modified'] 
                    ? new Date(res.headers['last-modified']).toISOString() 
                    : new Date().toISOString();
                const fileName = `${path}/${lastModified}.zip`;
                const writer = fs.createWriteStream(fileName, { encoding: 'base64' });

                res.body.pipe(writer)
                    .on('error', err => reject(err))
                    .on('finish', () => {
                        resolve(lastModified);
                    });
            } catch (err) {
                reject(err);
            }
        });
    }

    copyFileFromDisk(source, path) {
        return new Promise((resolve, reject) => {
            if (fs.existsSync(source) && source.endsWith('.zip')) {
                const stat = fs.statSync(source);
                const name = new Date(util.inspect(stat.mtime)).toISOString();
                const copy = fs.createReadStream(source)
                    .pipe(fs.createWriteStream(`${path}/${name}.zip`));

                copy.on('finish', () => {
                    resolve(name);
                });
            } else {
                reject(new Error('Invalid GTFS file'));
            }
        });
    }

    readAndGunzip(path) {
        return new Promise((resolve, reject) => {
            let buffers = [];
            fs.createReadStream(path)
                .pipe(new zlib.createGunzip())
                .on('error', err => {
                    reject(err + ' - ocurred on file: ' + path);
                })
                .on('data', data => {
                    buffers.push(data);
                })
                .on('end', () => {
                    resolve(buffers.join(''));
                });
        });
    }

    /**
     * compress JSON data string and write it to <path>.gz
     *
     * @param path path to JSON file where we want to write the data
     * @param data stringified JSON data
     * @returns {Promise<unknown>}
     */
    writeAndGzip(path, data) {
        return new Promise((resolve, reject) => {
            zlib.gzip(data, (error, buffer) => {
                if (error) {
                    reject(error + ' - occurred on file: ' + path + '.gz');
                } else {
                    fs.writeFile(`${path}.gz`, buffer, 'utf-8', (err) => {
                        if (err) {
                            reject(err + ' - occurred on file: ' + path + '.gz')
                        } else {
                            resolve()
                        }
                    });
                }
            });
        })
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
                })
                .on('error', err => {
                    reject(err);
                });
        });
    }

    // Sort array of dates ordered from the closest to farthest to a given date.
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

    /**
     * find closest real-time fragment to the left of a given target date in an update directory.
     * @param agency agency name of dataset we are searching
     * @param target target date (in epoch notation) of wanted fragment
     * @param update directory name of the update (ISO 8601 creation date of update)
     * @param RTFragments index that keeps track of all real-time data fragments
     * @returns {*[]} an array containing update dir, fragment name and fragment index
     */
    findRTResource(agency, target, update, RTFragments) {
        let fragment = null;
        let fragments = RTFragments[agency][update];

        // Checking that target date is contained in the list of fragments.
        if (target >= fragments[0] && target <= fragments[fragments.length - 1]) {
            fragment = this.binarySearch(target, fragments);
            if (fragment !== null) {
                return [update, fragment[0], fragment[1]]
            }
        }
        throw new Error('Fragment not found in real-time data');
    }

    // Search for a given fragment across the different static versions
    findResource(agency, target, versions, staticFragments) {
        let version = null;
        let fragment = null;

        for (let i = 0; i < versions.length; i++) {
            let fragments = staticFragments[agency][versions[i]];

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
            throw new Error('Fragment not found in static data');
        }
    }

    /**
     * Find the closest directory with creation date to the left of a given target date.
     * We first look in the real-time fragments for an update directory, then we look in the static
     * fragments for a version directory.
     *
     * @param agency the company name for which we are finding a directory
     * @param target name of wanted directory (creation date in ISO 8601 notation)
     * @param RTFragments index that keeps track of all real-time data fragments
     * @param staticFragments index that keeps track of all static data fragments
     * @returns {(*|boolean)[]} an array containing folder name, folder index and a bool that says
     *                          whether the folder contains static or real-time data fragments
     */
    findFeedResource(agency, target, RTFragments, staticFragments) {
        // sort static versions and RT updates from oldest to newest date
        let updates = Object.keys(RTFragments[agency]).sort();
        let versions = Object.keys(staticFragments[agency]).sort();

        try {
            // is_static tells us whether we have to search in the static or RT data
            let is_static = !(target >= updates[0]);
            let index_array = is_static ? versions : updates;

            let result = this.binarySearch(target, index_array);
            if (result === null) {
                // if result is null, refer to the last folder name in the index
                result = [index_array[index_array.length - 1], index_array.length - 1]
            }
            return [result[0], result[1], is_static]
        } catch (err) {
            // if an exception occurred, throw it again so we can handle it
            throw err;
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

    findRTData(agency, lowerLimit, upperLimit) {
        let dataConfig = this.getCompanyDatasetConfig(agency);

        if (dataConfig.realTimeData) {
            let fts = (dataConfig.realTimeData.fragmentTimeSpan || 600) * 1000;
            let rtfs = [];
            let rtfs_remove = [];

            let lowerDate = new Date(lowerLimit - (lowerLimit % fts));
            let upperDate = new Date(upperLimit - (upperLimit % fts));

            // Only one real-time fragment is needed to cover the requested static fragment
            if (lowerDate === upperDate) {
                let path = this.getRTFilePath(lowerDate.toISOString(), agency);
                let path_remove = this.getRTRemoveFilePath(lowerDate.toISOString(), agency);
                if (path !== null) {
                    rtfs.push(path);
                }
                if (path_remove !== null) {
                    rtfs_remove.push(path_remove);
                }
            } else {
                // Get all real-time fragments that cover the requested static fragment
                while (lowerDate.getTime() <= upperDate.getTime()) {
                    let path = this.getRTFilePath(lowerDate.toISOString(), agency);
                    let path_remove = this.getRTRemoveFilePath(lowerDate.toISOString(), agency);
                    if (path !== null) {
                        rtfs.push(path);
                    }
                    if (path_remove !== null) {
                        rtfs_remove.push(path_remove);
                    }
                    lowerDate.setTime(lowerDate.getTime() + fts);
                }
            }

            return [rtfs, rtfs_remove];
        } else {
            return [[], []];
        }
    }

    async aggregateRTData(static_data, rt_data, remove_paths, lowLimit, highLimit, timestamp) {
        // Index map for the static fragment
        let t0 = new Date();
        let static_index = this.getStaticIndex(static_data);
        logger.debug('getStaticIndex() took ' + (new Date().getTime() - t0.getTime()) + ' ms');
        // Index map for the associated real-time fragments
        t0 = new Date();
        let [rt_index, rt_remove] = await this.getRTIndex(rt_data, static_index, lowLimit, highLimit, timestamp);
        logger.debug('getRTIndex() took ' + (new Date().getTime() - t0.getTime()) + ' ms');

        // Array of the Connections that may be removed from the static fragment due to delays
        t0 = new Date();
        let to_remove = await this.getConnectionsToRemove(remove_paths, timestamp);
        logger.debug('getConnectionsToRemove() took ' + (new Date().getTime() - t0.getTime()) + ' ms');

        // Iterate over the RT index which contains all the connections that need to be updated or included
        t0 = new Date();
        for (let [connId, conn] of rt_index) {
            let rtd = null;
            try {
                rtd = JSON.parse(conn);
            } catch (err) {
                continue;
            }
            // If the connection is already present in the static fragment just add delay values and update departure and arrival times
            if (static_index.has(connId)) {
                let std = static_data[static_index.get(connId)];
                // Update @type in case the connection was cancelled
                std['@type'] = rtd['@type'];
                // Add delays
                std['departureDelay'] = rtd['departureDelay'];
                std['arrivalDelay'] = rtd['arrivalDelay'];
                // Update departure and arrival times with delays
                std['departureTime'] = rtd['departureTime'] || new Date(new Date(std['departureTime']).getTime() + Number(rtd['departureDelay']) * 1000).toISOString();
                std['arrivalTime'] = rtd['arrivalTime'] || new Date(new Date(std['arrivalTime']).getTime() + Number(rtd['arrivalDelay']) * 1000).toISOString();
                static_data[static_index.get(connId)] = std;
            } else {
                // Is not present in the static fragment which means it's a connection that comes from a different fragment 
                // and it is here due to delays.
                delete rtd['mementoVersion'];
                static_data.push(rtd);
                static_index.set(connId, static_data.length - 1);
            }
        }
        logger.debug('Combine static and rt indexes took ' + (new Date().getTime() - t0.getTime()) + ' ms');

        // Now iterate over the arrays of connections that were reported to change real-time fragment due to delays and see 
        // if it necessary to remove them.
        t0 = new Date();

        for (let [connId, timestamp] of to_remove) {
            // Check if they have been reported in real-time updates
            if (rt_index.has(connId)) {
                // Date of the last real-time update registered for this connection within the scope of the requested static fragment
                let rt_memento = new Date(rt_index.get(connId).split('"')[43]);
                // Date of the last remove update registered for this connection
                let remove_memento = new Date(timestamp);
                // If the real-time update is older than the remove update proceed to remove the connection. The reason for this
                // is that an older real-time update means that the connection already has a delay that puts it beyond the range
                // of the requested static fragment.
                if (remove_memento > rt_memento) {
                    let si = static_index.get(connId);
                    static_data.splice(si, 1);
                    static_index = this.getStaticIndex(static_data);
                }
            } else {
                // If the connection is present in the static fragment without an associated real-time update, this means that its
                // real-time updates are beyond the static fragment range so proceed to remove it. 
                if (static_index.has(connId)) {
                    let si = static_index.get(connId);
                    static_data.splice(si, 1);
                    static_index = this.getStaticIndex(static_data);
                }
            }
        }

        // Connections that were identified to be removed during the real-time index creation
        for (let [connId, memento] of rt_remove) {
            if (static_index.has(connId)) {
                let si = static_index.get(connId);
                static_data.splice(si, 1);
                static_index = this.getStaticIndex(static_data);
            }
        }
        logger.debug('Connection removal process took ' + (new Date().getTime() - t0.getTime()) + ' ms');

        // Re-sort the fragment with the updated delay data
        t0 = new Date();
        static_data.sort((a, b) => {
            let a_date = new Date(a['departureTime']).getTime();
            let b_date = new Date(b['departureTime']).getTime();
            return a_date - b_date;
        });
        logger.debug('Re-sorting process took ' + (new Date().getTime() - t0.getTime()) + ' ms');
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

    async getRTIndex(arrays, static_index, lowLimit, highLimit, timeCriteria) {
        let map = new Map();
        // Array to keep track of the connections that must be removed due to fragment change withing the range of a single 
        // real-time data fragment. E.g. frag1 = 2018-10-09T02:43:00.000Z, frag2 = 2018-10-09T02:45:00.000Z, 
        // rt-frag = 2018-10-09T02:40:00.000Z (spanning 10 minutes).
        let toRemove = new Map();
        let possibleRemove = new Map();
        let low = new Date(lowLimit);
        let high = new Date(highLimit);

        // Process every real-time fragment asynchronously to speed up the process
        await Promise.all(arrays.map(async array => {
            for (let i in array) {
                let obj = array[i].split('"');
                let memento_date = new Date(obj[obj.indexOf('mementoVersion') + 2]);

                // Discard invalid dates
                if (memento_date.toString() === 'Invalid Date') {
                    continue;
                }
                // Check that we are dealing with data according to the requested time
                if (memento_date <= timeCriteria) {
                    let connId = obj[obj.indexOf('@id') + 2];
                    // Check that this connection belongs to the time range of the requested static fragment
                    let depDate = new Date(obj[obj.indexOf('departureTime') + 2]);
                    if (depDate >= low && depDate < high) {
                        // Check if it has been reported for removal before and if this update is newer
                        if (toRemove.has(connId) && memento_date > toRemove.get(connId)) {
                            toRemove.delete(connId);
                        }
                        if (map.has(connId)) {
                            // Check if this is more updated data
                            let old_obj = map.get(connId).split('"');
                            let sm = new Date(old_obj[old_obj.indexOf('mementoVersion') + 2]);
                            if (memento_date > sm) {
                                map.set(connId, array[i]);
                            }
                        } else {
                            map.set(connId, array[i]);
                        }
                    } else {
                        // Check if it was previously added to the real-time index and mark it as possible remove
                        if (map.has(connId)) {
                            if (possibleRemove.has(connId)) {
                                let sm = new Date(possibleRemove.get(connId));
                                if (memento_date > sm) {
                                    possibleRemove.set(connId, memento_date);
                                }
                            } else {
                                possibleRemove.set(connId, memento_date);
                            }
                        }
                        // Check if it originally belongs to the time range of the requested static fragment
                        // for it to be scheduled for removal
                        if (static_index.has(connId)) {
                            toRemove.set(connId, memento_date);
                        }

                    }
                } else {
                    break;
                }
            }
        }));

        // Process the possible removals over the real-time index
        for (let [id, memento] of possibleRemove) {
            let pr = map.get(id).split('"');
            let sm = new Date(pr[pr.indexOf('mementoVersion') + 2]);
            if (memento > sm) {
                map.delete(id);
            }
        }

        return [map, toRemove];
    }

    async getConnectionsToRemove(paths, timestamp) {
        let remove_list = new Map();
        // Process every .remove file asynchronously to speed up the process
        await Promise.all(paths.map(async path => {
            // Read .remove file
            let remove_data = null;
            if (path.endsWith('.gz')) {
                remove_data = (await this.readAndGunzip(path)).toString().split('\n');
            } else {
                remove_data = (await readFile(path, 'utf8')).split('\n');
            }

            for (let i in remove_data) {
                let remove = remove_data[i].split(',');
                let memento = new Date(remove[1]);
                if (memento <= timestamp) {
                    remove_list.set(remove[0], memento);
                } else {
                    break;
                }
            }
        }));

        return remove_list;
    }

    async addHydraMetada(params) {
        try {
            let template = await readFile('./statics/skeleton.jsonld', { encoding: 'utf8' });
            let jsonld_skeleton = JSON.parse(template);
            let host = params.host;
            let agency = params.agency;
            let departureTime = params.departureTime;
            let version = params.version;
            let staticFragments = params.staticFragments;

            jsonld_skeleton['@id'] = host + agency + '/connections?departureTime=' + departureTime.toISOString();

            let next = staticFragments[agency][version][Number(params.index) + 1];
            if (next) {
                jsonld_skeleton['hydra:next'] = host + agency + '/connections?departureTime=' + new Date(next).toISOString();
            } else {
                delete jsonld_skeleton['hydra:next'];
            }

            let prev = staticFragments[agency][version][Number(params.index) - 1];
            if (prev && prev !== null) {
                jsonld_skeleton['hydra:previous'] = host + agency + '/connections?departureTime=' + new Date(prev).toISOString();
            } else {
                delete jsonld_skeleton['hydra:previous'];
            }

            jsonld_skeleton['hydra:search']['hydra:template'] = host + agency + '/connections{?departureTime}';
            jsonld_skeleton['@graph'] = params.data;

            return jsonld_skeleton;

        } catch (err) {
            console.error(err);
            throw err;
        }
    }

    async addLDESMetadata(params) {
        try {
            let template = await readFile('./statics/skeleton-ldes.jsonld', { encoding: 'utf8' });
            let ldes = JSON.parse(template);
            let host = params.host;
            let agency = params.agency;
            let members = params.members; // connections
            let created = new Date(params.created); // epoch time of created
            let next = (params.next !== null) ? new Date(params.next) : null; // epoch time of next fragment
            let previous = (params.previous !== null) ? new Date(params.previous) : null; // epoch time of previous fragment
            let latest = (params.latest !== null) ? new Date(params.latest) : null; // epoch time of latest fragment
            let is_static = params.isStatic; // bool that will tell you if data fragment is static
            let max_dt = params.maxDeparture;    // ISO8601 string of latest departureTime in fragment

            await this.addSHACLMetadata({
                "id": host + agency + '/connections/feed/shape',
                "ldes": host + agency + '/connections/feed/'
            });

            let departureTime = (params.departureTime !== null) ? new Date(params.departureTime) : null;
            let nextDeparture = (params.nextDeparture !== null) ? new Date(params.nextDeparture) : null;

            let dt = (departureTime !== null) ? "&departureTime=" + departureTime.toISOString() : "";
            let ndt = (nextDeparture !== null) ? "&departureTime=" + nextDeparture.toISOString() : "";

            ldes['@id'] = host + agency + '/connections/feed';
            ldes['tree:shape'] = host + agency + '/connections/feed/shape';
            ldes['member'] = [];
            for (const member of members) {
                ldes['member'].push(member);
            }

            ldes['tree:view']['@id'] = host + agency + `/connections/feed?created=${created.toISOString()}` + dt;

            let treeRelations = [];
            // next departure time
            if (nextDeparture !== null) {
                treeRelations.push({
                    "@type": "tree:GreaterThanOrEqualToRelation",
                    "tree:node": host + agency + `/connections/feed?created=${created.toISOString()}` + ndt,
                    "tree:value": {
                        "@value": max_dt,
                        "@type": "xsd:dateTime"
                    },
                    "tree:path": "lc:departureTime"
                });
            }
            // next created
            if (next !== null) {
                treeRelations.push({
                    "@type": "tree:GreaterThanRelation",
                    "tree:node": host + agency + `/connections/feed?created=${next.toISOString()}`,
                    "tree:value": {
                        "@value": `${created.toISOString()}`,
                        "@type": "xsd:dateTime"
                    },
                    "tree:path": "dcterms:created"
                });
            }
            // previous created
            if (previous !== null) {
                treeRelations.push({
                    "@type": "tree:LessThanRelation",
                    "tree:node": host + agency + `/connections/feed?created=${previous.toISOString()}`,
                    "tree:value": {
                        "@value": `${created.toISOString()}`,
                        "@type": "xsd:dateTime"
                    },
                    "tree:path": "dcterms:created"
                });
            }
            // latest created
            if ((latest !== null) && (latest.getTime() !== created.getTime())) {
                treeRelations.push({
                    "@type": "tree:GreaterThanRelation",
                    "tree:node": host + agency + `/connections/feed?created=${latest.toISOString()}`,
                    "tree:value": {
                        "@value": `${created.toISOString()}`,
                        "@type": "xsd:dateTime"
                    },
                    "tree:path": "dcterms:created"
                });
            }
            ldes['tree:view']['tree:relation'] = treeRelations;

            return ldes;
        } catch (err) {
            console.error(err);
            throw err;
        }
    }

    async addSHACLMetadata(params) {
        try {
            let template = await readFile('./statics/shape.jsonld', { encoding: 'utf8' });
            let shape = JSON.parse(template);

            let id = params.id;
            let ldes = params.ldes;

            shape['@id'] = id;
            shape['shapeOf'] = ldes;

            return shape;
        } catch (err) {
            console.error(err);
            throw err;
        }
    }

    /**
     * Checks for Conditional GET requests, by comparing the last modified date and file hash
     * against if-modified-since and if-none-match headers.
     * If a match is made, a 304 response is sent and the function will return true.
     * If the client needs a new version (i.e. a body should be sent), the function will return false
     * @param req
     * @param res
     * @param filepath The path of the file which would be used to generate the response
     * @param departureTime The time for which this document was requested
     * @returns boolean True if a 304 response is to be served
     */
    handleConditionalGET(company, req, res, accept, filepath, hasLiveData, departureTime, memento) {
        let ifModifiedSinceRawHeader = req.header('if-modified-since');
        let ifModifiedSinceHeader = undefined;

        if (ifModifiedSinceRawHeader) {
            ifModifiedSinceHeader = new Date(ifModifiedSinceRawHeader);
        }

        let ifNoneMatchHeader = req.header('if-none-match');

        let stats = fs.statSync(filepath);
        let lastModifiedDate = new Date(util.inspect(stats.mtime));

        let now = new Date();

        let validUntilDate = null;
        if (hasLiveData) {
            // Valid until the next update + 1 second to allow intermediate caches to update
            let rtUpdatePeriod = this.getCompanyDatasetConfig(company)['realTimeData']['updatePeriod'];
            validUntilDate = this.getNextUpdate(rtUpdatePeriod);
        } else {
            if (departureTime < (now - 10800000)) {
                // If departure time requested is older than 3 hours set validity for one year
                validUntilDate = new Date(now.getTime() + 31536000000);
            } else {
                // If is not older than 3 hours it is valid for 1 day
                validUntilDate = new Date(now.getTime() + (3600 * 24 * 1000))
            }
        }


        let maxage = Math.round((validUntilDate - now) / 1000);
        let etag_reference = null;

        // Take into account Memento requests to define the ETag header
        if (memento) {
            etag_reference = filepath + lastModifiedDate + accept + memento;
        } else {
            etag_reference = filepath + lastModifiedDate + accept;
        }

        let etag = 'W/"' + md5(etag_reference) + '"';

        // If it is a memento request of an older resource or departure time is older than 3 hours it becomes immutable
        if ((memento && memento < now) || departureTime < (now - 10800000)) {
            // Immutable (for browsers which support it, sometimes limited to https only
            // 1 year expiry date to keep it long enough in cache for the others
            res.set({ 'Cache-Control': 'public, max-age=31536000000, immutable' });
            res.set({ 'Expires': new Date(now.getTime() + 31536000000).toUTCString() });
        } else {
            // Let clients hold on to this data for 1 second longer than nginx. This way nginx can update before the clients
            res.set({ 'Cache-Control': 'public, s-maxage=' + maxage + ', max-age=' + (maxage + 1) + ', stale-if-error=' + (maxage + 15) + ', proxy-revalidate' });
            res.set({ 'Expires': validUntilDate.toUTCString() });
        }

        res.set({ 'ETag': etag });
        res.set({ 'Vary': 'Accept-Encoding, Accept-Datetime' });
        res.set({ 'Last-Modified': lastModifiedDate.toUTCString() });
        res.set({ 'Content-Type': 'application/ld+json' });

        // If an if-none-match header exists, and if the real-time data hasn't been updated since, just return a 304
        // According to the spec this header takes priority over if-modified-since 
        if (ifNoneMatchHeader && ifNoneMatchHeader === etag) {
            res.status(304).send();
            return true;
        }

        // If an if-modified-since header exists, and if the real-time data hasn't been updated since, just return a 304
        if (lastModifiedDate && ifModifiedSinceHeader && ifModifiedSinceHeader >= lastModifiedDate) {
            res.status(304).send();
            return true;
        }

        return false;
    }

    getNextUpdate(cron) {
        const nextUpdate = cronParser.parseExpression(cron).next();
        return new Date(nextUpdate.getTime() + 1000);
    }

    getCompanyDatasetConfig(company) {
        let datasets = this.datasetsConfig.datasets
        for (let i in datasets) {
            if (company == datasets[i].companyName) {
                return datasets[i];
            }
        }
    }

    async getLatestGtfsSource(dataset_folder) {
        let versions = (await readdir(dataset_folder))
            .reduce((filtered, v) => {
                if (v.endsWith('.zip')) {
                    filtered.push(new Date(v.substring(0, v.indexOf('.zip'))));
                }

                return filtered;
            }, []);

        if (versions.length > 0) {
            versions = versions.filter(v => v.toString() !== 'Invalid Date');
            return dataset_folder + '/' + this.sortVersions(new Date(), versions)[0].toISOString() + '.zip';
        } else {
            return null;
        }
    }

    getRTFilePath(fragment, company) {
        let path = this.datasetsConfig.storage + '/real_time/' + company + '/';
        if (fs.existsSync(path + fragment + '.jsonld')) {
            return path + fragment + '.jsonld';
        } else if (fs.existsSync(path + fragment + '.jsonld.gz')) {
            return path + fragment + '.jsonld.gz';
        } else {
            return null;
        }
    }

    getRTRemoveFilePath(fragment, company) {
        let path = this.datasetsConfig.storage + '/real_time/' + company + '/';
        if (fs.existsSync(path + fragment + '.remove')) {
            return path + fragment + '.remove';
        } else if (fs.existsSync(path + fragment + '.remove.gz')) {
            return path + fragment + '.remove.gz';
        } else {
            return null;
        }
    }

    jsonld2RDF(jsld, format) {
        return new Promise(async (resolve, reject) => {
            try {
                let quads = await jsonld.toRDF(jsld);
                let parser = new N3.Writer({
                    format: format,
                    prefixes: {
                        xsd: 'http://www.w3.org/2001/XMLSchema#',
                        lc: 'http://semweb.mmlab.be/ns/linkedconnections#',
                        gtfs: 'http://vocab.gtfs.org/terms#',
                        hydra: 'http://www.w3.org/ns/hydra/core#'
                    }
                });
                for (let i in quads) {
                    parser.addQuad(quad(
                        this.resolveRDFJSTerm(quads[i].subject),
                        this.resolveRDFJSTerm(quads[i].predicate),
                        this.resolveRDFJSTerm(quads[i].object),
                        this.resolveRDFJSTerm(quads[i].graph)
                    ));
                }
                parser.end((err, result) => resolve(result));
            } catch (err) {
                logger.error(err);
                reject(err);
            }
        });
    }

    resolveRDFJSTerm(term) {
        switch (term.termType) {
            case 'NamedNode':
                return namedNode(term.value);
            case 'BlankNode':
                return blankNode(term.value.substring(2));
            case 'Literal':
                return literal(term.value, namedNode(term.datatype.value));
            case 'DefaultGraph':
                return defaultGraph();
            default:
                throw new Error('Unknown term type: ' + term.termType);
        }
    }

    resolveURI(template, element, resolve) {
        let varNames = template.varNames;
        let fillerObj = {};

        for (let i in varNames) {
            fillerObj[varNames[i]] = this.resolveValue(varNames[i], element, resolve);
        }

        return template.fill(fillerObj);
    }

    resolveValue(param, element, resolve) {
        // Try first to resolve using keys in 'resolve' object
        if (resolve && resolve[param]) {
            let trips = element.trip;
            let routes = element.route;
            let stops = element.stop;
            return eval(resolve[param]);
        }

        // Otherwise, keep behaviour for backward compatibility

        // GTFS source file and attribute name
        let source = param.split('.')[0];
        let attr = param.split('.')[1];
        let value = null;

        switch (source) {
            case 'trips':
                value = element['trip'][attr].trim();
                break;
            case 'routes':
                value = element['route'][attr].trim();
                break;
            case 'stops':
                value = element['stop'][attr].trim();
                break;
        }

        return value;
    }

    get datasetsConfig() {
        return this._datasetsConfig;
    }

    get serverConfig() {
        return this._serverConfig;
    }
}
