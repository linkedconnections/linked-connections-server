const fs = require('fs');
const util = require('util');
const jsonld = require('jsonld');
const Catalog = require('../../lib/routes/catalog');
const Stops = require('../../lib/routes/stops');
var utils = require('../../lib/utils/utils');
const readfile = util.promisify(fs.readFile);

utils._datasetsConfig = {
    "storage": __dirname + "/storage",
    "organization": {
        "id": "https://example.org/your/URL",
        "name": "Data publisher name"
    },
    "datasets": [
        {
            "companyName": "test",
            "downloadUrl": "./test/generation/raw_data/cancelled_static.zip",
            "fragmentSize": 50000,
            "realTimeData": {
                "downloadUrl": "./test/generation/raw_data/cancelled_live",
                "updatePeriod": "*/30 * * * * *",
                "fragmentTimeSpan": 600,
                "compressionPeriod": "0 0 3 * * *"
            },
            "baseURIs": {
                "stop": "http://example.test/stations/{stops.stop_id}",
                "route": "http://example.test/routes/{routeName}/{routes.route_id}",
                "trip": "http://example.test/trips/{trips.trip_headsign}/{trips.service_id}",
                "connection": "http://example.test/connections/{connection.departureStop}/{routeName}/{tripStartTime}/",
                "resolve": {
                    "routeName": "routes.route_long_name.replace(/\\s/gi, '')",
                    "tripStartTime": "format(trips.startTime, 'YYYYMMDD')"
                }
            }
        }
    ]
};

var v = null;
var sf = null;
var i = null;
var rtf = null;
var rmf = null;
var low_limit = null;
var high_limit = null;
var liveData = null;
var combined = null

test('Test that the in memory static fragments index is created', async () => {
    expect.assertions(2);
    await utils.updateStaticFragments();
    expect(utils.staticFragments['test']).toBeDefined();
    expect(utils.staticFragments['test']['2019-06-12T11:13:10.334Z'].length).toBeGreaterThan(0);
});

test('Test that the correct fragment is found for a given departure time', () => {
    expect.assertions(3);
    let [version, fragment, index] = utils.findResource('test', new Date('2019-06-04T15:10:00.000Z').getTime(),
        ['2019-06-12T11:13:10.334Z']);
    v = version;
    sf = new Date(fragment);
    i = index;
    expect(v).toMatch('2019-06-12T11:13:10.334Z');
    expect(sf.toISOString()).toMatch('2019-06-04T14:36:00.000Z');
    expect(i).toBeGreaterThanOrEqual(0);
});

test('Test that the correct real-time fragments are found', () => {
    expect.assertions(3);
    low_limit = sf.getTime();
    high_limit = utils.staticFragments['test'][v][i + 1];
    let [rt_fragments, remove] = utils.findRTData('test', low_limit, high_limit);
    rtf = rt_fragments;
    rmf = remove;
    expect(rtf[0]).toContain('2019-06-04T14:30:00.000Z');
    expect(rtf.length).toBe(23);
    expect(rmf.length).toBe(13);
});

test('Test that static and real-time data are correctly combined - Should find a cancelled trip', async () => {
    expect.assertions(1);
    let staticData = await loadStaticData();
    liveData = await loadRTData();
    combined = await utils.aggregateRTData(staticData, liveData, rmf, low_limit, high_limit, new Date());
    let cancelled = findConnection('http://example.test/connections/8872009/Schaerbeek%E2%80%93Chatelet/20190604T1600/', combined);
    expect(cancelled['@type']).toMatch('CancelledConnection');
});

test('Test Memento feature to find the same trip of the previous test before it was cancelled', async () => {
    expect.assertions(1);
    let staticData = await loadStaticData();
    // Live update cancelling the trip happened at 2019-06-13T14:55:31.940Z
    let combined = await utils.aggregateRTData(staticData, liveData, rmf, low_limit, high_limit, new Date('2019-06-13T13:00:00.000Z'));
    let notCancelledYet = findConnection('http://example.test/connections/8872009/Schaerbeek%E2%80%93Chatelet/20190604T1600/', combined);
    expect(notCancelledYet['@type']).toMatch('Connection');
});

test('Test a Connection that should be added due to a live update', async () => {
    expect.assertions(2);
    let staticData = await loadStaticData();
    let sc = findConnection('http://example.test/connections/8812005/Welkenraedt%E2%80%93Courtrai/20190604T1449/', staticData);
    expect(sc).not.toBeDefined();
    let addedConn = findConnection('http://example.test/connections/8812005/Welkenraedt%E2%80%93Courtrai/20190604T1449/', combined);
    // This connection has a delay of 15 mins which is why it gets added only when the live update is processed
    expect(addedConn['departureDelay']).toBe(900);
});

test('Test that all Connections are correctly sorted by departure time', () => {
    expect.assertions(1);
    let error = false;
    for(let k = 0; k < combined.length - 1; k++) {
        if(combined[k]['departureTime'] > combined[k + 1]['departureTime']) {
            error = true;
            break;
        }
    }
    expect(error).toBeFalsy();
});

test('Test that the cache headers handled correctly', () => {
    expect.assertions(9);

    // Create request and response mock objects
    let req = {
        headers: new Map(),
        header(header) {
            return this.headers.get(header);
        }
    };
    let res = {
        sts: null,
        headers: new Map(),
        set(header) {
            this.headers.set(Object.keys(header)[0], header[Object.keys(header)[0]]);
        },
        status(status) {
            this.sts = status;
            return this;
        },
        send(){}
    };

    // Path to some test data fragment used to define LastModifiedDate header
    let path = utils.datasetsConfig['storage'] + '/linked_pages/test/' + v + '/' + sf.toISOString() + '.jsonld.gz';
    let now = new Date();

    // Request for a departure time older than 3 hours with no live data -> should be immutable
    utils.handleConditionalGET(req, res, path, false, new Date(now.getTime() - (3600 * 4 * 1000)));
    expect(res.headers.get('Cache-Control').indexOf('immutable')).toBeGreaterThan(-1);

    res.headers = new Map();

    // Request for a departure time older than 3 hours with live data -> should be immutable
    utils.handleConditionalGET(req, res, path, true, new Date(now.getTime() - (3600 * 4 * 1000)));
    expect(res.headers.get('Cache-Control').indexOf('immutable')).toBeGreaterThan(-1);

    res.headers = new Map();

    // Request for a departure time happening now with no live data -> should be valid for 24 hours
    utils.handleConditionalGET(req, res, path, false, now);
    expect(res.headers.get('Cache-Control').indexOf('max-age=86401')).toBeGreaterThan(-1);

    res.headers = new Map();
    
    // Request for a departure time happening now with live data -> should be valid for 32 seconds at most
    utils.handleConditionalGET(req, res, path, true, now);
    let cacheControl = res.headers.get('Cache-Control');
    let maxAge = cacheControl.substring(cacheControl.indexOf('max-age='), cacheControl.indexOf('max-age=') + 10).split('=')[1];
    if(maxAge.endsWith(',')) {
        maxAge = maxAge.slice(0, -1);
    }
    expect(Number(maxAge)).toBeGreaterThan(0);
    expect(Number(maxAge)).toBeLessThan(33);

    res.headers = new Map();

    // Request using the if-modified-since header -> should give a 304 response
    req.headers.set('if-modified-since', now.toISOString());
    utils.handleConditionalGET(req, res, path, true, now);
    expect(res.sts).toBe(304);

    res.headers = new Map();
    res.status(null);
    req.headers = new Map();

    // Request using the If-Modified-Since header -> should not give a 304 response
    req.headers.set('if-modified-since', new Date('2017-06-10T08:00:00.000Z').toISOString());
    utils.handleConditionalGET(req, res, path, true, now);
    expect(res.sts).toBeNull();

    res.headers = new Map();
    res.status(null);
    req.headers = new Map();

    // Issue a first request to get an ETag header value
    let etag = null;
    utils.handleConditionalGET(req, res, path, true, now);
    etag = res.headers.get('ETag');
    expect(etag).toBeDefined();

    res.headers = new Map();
    res.status(null);
    req.headers = new Map();

    // Issue a second request using the obtained ETag value in the If-None-Match header -> should return a 304 response
    req.headers.set('if-none-match', etag);
    utils.handleConditionalGET(req, res, path, true, now);
    expect(res.sts).toBe(304);
});

test('Test that resulting JSON-LD data is correct', async () => {
    expect.assertions(1);
    let data = await utils.addHydraMetada({
        host: 'http://localhost:3000/',
        agency: 'test',
        departureTime: sf,
        version: v,
        index: i,
        data: combined
    });
    let rdf = await jsonld.toRDF(data);
    expect(rdf).toBeDefined();
});

test('Test to create DCAT catalog', async () => {
    expect.assertions(1);
    let res = {
        sts: null,
        headers: new Map(),
        set(header) {
            this.headers.set(Object.keys(header)[0], header[Object.keys(header)[0]]);
        },
        status(status) {
            this.sts = status;
            return this;
        },
        send(){}
    };
    let catalog = new Catalog({}, res);
    catalog._utils = utils;
    catalog._storage = utils.datasetsConfig['storage'];
    catalog._datasets = utils.datasetsConfig['datasets'];
    let cat = await catalog.createCatalog();
    expect(cat['@context']).toBeDefined();
});

test('Test to retrieve list of stops', async () => {
    expect.assertions(1);
    let stops = new Stops();
    stops._utils = utils;
    stops._storage = utils.datasetsConfig['storage'];
    stops._datasets = utils.datasetsConfig['datasets'];
    let stps = await stops.createStopList('test');
    expect(stps['@graph'].length).toBeGreaterThan(0);
});

function findConnection(id, array) {
    for (let i in array) {
        if (array[i]['@id'] === id) {
            return array[i];
        }
    }
}

async function loadStaticData() {
    let sf_path = utils.datasetsConfig['storage'] + '/linked_pages/test/' + v + '/';
    let static_buffer = await utils.readAndGunzip(sf_path + sf.toISOString() + '.jsonld.gz');
    return static_buffer.split(',\n').map(JSON.parse);
}

async function loadRTData() {
    let rt_data = [];
    await Promise.all(rtf.map(async rt => {
        let rt_buffer = [];
        if (rt.indexOf('.gz') > 0) {
            rt_buffer.push((await utils.readAndGunzip(rt)));
        } else {
            rt_buffer.push((await readfile(rt, 'utf8')));
        }
        rt_data.push(rt_buffer.toString().split('\n'));
    }));

    return rt_data;
}