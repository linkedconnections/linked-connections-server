import { jest, test, expect } from '@jest/globals';
import fs from 'fs';
import util from 'util';
import path from 'path';
import { deleteAsync as del } from 'del';
import jsonld from 'jsonld';
import { Catalog } from '../../lib/routes/catalog.js';
import { Stops } from '../../lib/routes/stops.js';
import { Routes } from '../../lib/routes/routes.js';
import { Utils } from '../../lib/utils/utils.js';
import { StaticData } from '../../lib/data/static';

const __dirname = path.resolve();
const utils = new Utils();
const readfile = util.promisify(fs.readFile);

utils._datasetsConfig = {
    "storage": __dirname + "/test/serving/storage",
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
                    "tripStartTime": "format(trips.startTime, 'yyyyMMdd')"
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
var staticFragments = null;

jest.setTimeout(30000);

test('Test that the in memory static fragments index is created', async () => {
    expect.assertions(2);
    const staticData = new StaticData(utils.datasetsConfig['storage'], utils.datasetsConfig['datasets'])
    await staticData.updateStaticFragments();
    staticFragments = staticData.staticFragments;
    expect(staticFragments['test']).toBeDefined();
    expect(staticFragments['test']['2019-06-12T11:13:10.334Z'].length).toBeGreaterThan(0);
});

test('Test that the correct fragment is found for a given departure time', () => {
    expect.assertions(3);
    let [version, fragment, index] = utils.findResource('test', new Date('2019-06-04T15:10:00.000Z').getTime(),
        ['2019-06-12T11:13:10.334Z'], staticFragments);
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
    high_limit = staticFragments['test'][v][i + 1];
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
    for (let k = 0; k < combined.length - 1; k++) {
        if (combined[k]['departureTime'] > combined[k + 1]['departureTime']) {
            error = true;
            break;
        }
    }
    expect(error).toBeFalsy();
});

test('Test that resulting JSON-LD data is correct', async () => {
    expect.assertions(2);
    let data = await utils.addHydraMetada({
        host: 'http://localhost:3000/',
        agency: 'test',
        departureTime: sf,
        version: v,
        index: i,
        data: combined,
        staticFragments: staticFragments
    });
    let rdf = await jsonld.toRDF(data);
    expect(rdf).toBeDefined();

    let trig = await utils.jsonld2RDF(data, 'application/trig');
    expect(trig).toBeDefined();
});

test('Test that the cache headers handled correctly', () => {
    expect.assertions(12);

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
        send() { }
    };

    let accept = 'application/ld+json';

    // Path to some test data fragment used to define LastModifiedDate header
    let path = utils.datasetsConfig['storage'] + '/linked_pages/test/' + v + '/' + sf.toISOString() + '.jsonld.gz';
    let now = new Date();

    // Request for a departure time older than 3 hours with no live data -> should be immutable
    utils.handleConditionalGET('test', req, res, accept, path, false, new Date(now.getTime() - (3600 * 4 * 1000)));
    expect(res.headers.get('Cache-Control').indexOf('immutable')).toBeGreaterThan(-1);

    res.headers = new Map();

    // Request for a departure time older than 3 hours with live data -> should be immutable
    utils.handleConditionalGET('test', req, res, accept, path, true, new Date(now.getTime() - (3600 * 4 * 1000)));
    expect(res.headers.get('Cache-Control').indexOf('immutable')).toBeGreaterThan(-1);

    res.headers = new Map();

    // Request for a departure time happening now with no live data -> should be valid for 24 hours
    utils.handleConditionalGET('test', req, res, accept, path, false, now);
    expect(res.headers.get('Cache-Control').indexOf('max-age=86401')).toBeGreaterThan(-1);

    res.headers = new Map();

    // Request for a departure time happening now with live data -> should be valid for 32 seconds at most
    utils.handleConditionalGET('test', req, res, accept, path, true, now);
    let cacheControl = res.headers.get('Cache-Control');
    let maxAge = cacheControl.substring(cacheControl.indexOf('max-age='), cacheControl.indexOf('max-age=') + 10).split('=')[1];
    if (maxAge.endsWith(',')) {
        maxAge = maxAge.slice(0, -1);
    }
    expect(Number(maxAge)).toBeGreaterThan(0);
    expect(Number(maxAge)).toBeLessThan(33);

    res.headers = new Map();

    // Request using the if-modified-since header -> should give a 304 response
    req.headers.set('if-modified-since', now.toISOString());
    utils.handleConditionalGET('test', req, res, accept, path, true, now);
    expect(res.sts).toBe(304);

    res.headers = new Map();
    res.status(null);
    req.headers = new Map();

    // Request using the If-Modified-Since header -> should NOT give a 304 response
    req.headers.set('if-modified-since', new Date('2017-06-10T08:00:00.000Z').toISOString());
    utils.handleConditionalGET('test', req, res, accept, path, true, now);
    expect(res.sts).toBeNull();

    res.headers = new Map();
    res.status(null);
    req.headers = new Map();

    // Issue a first request to get an ETag header value
    let etag = null;
    utils.handleConditionalGET('test', req, res, accept, path, true, now);
    etag = res.headers.get('ETag');
    expect(etag).toBeDefined();

    res.headers = new Map();
    res.status(null);
    req.headers = new Map();

    // Issue a second request using the obtained ETag value in the If-None-Match header -> should return a 304 response
    req.headers.set('if-none-match', etag);
    utils.handleConditionalGET('test', req, res, accept, path, true, now);
    expect(res.sts).toBe(304);

    res.headers = new Map();
    res.status(null);

    // Issue a third request using the obtained ETag value in the If-None-Match header but a different accept 
    // header -> should NOT give a 304 response
    req.headers.set('if-none-match', etag);
    utils.handleConditionalGET('test', req, res, 'application/trig', path, true, now);
    expect(res.sts).toBeNull();

    res.headers = new Map();
    res.status(null);
    req.headers = new Map();

    // Request a memento of a resource from 10 mins ago -> should be immutable
    utils.handleConditionalGET('test', req, res, accept, path, true, now, new Date(now.getTime() - (60 * 10 * 1000)));
    expect(res.headers.get('Cache-Control').indexOf('immutable')).toBeGreaterThan(-1);

    res.headers = new Map();

    // Request a memento of a resource without live data 10 mins in the future (which makes no sense but is still possible) 
    // -> should be valid for 24 hours
    utils.handleConditionalGET('test', req, res, accept, path, false, now, new Date(now.getTime() + (60 * 10 * 1000)));
    expect(res.headers.get('Cache-Control').indexOf('max-age=86401')).toBeGreaterThan(-1);
});

test('Test to retrieve list of stops', async () => {
    expect.assertions(1);
    let stops = new Stops();
    stops._storage = utils.datasetsConfig['storage'];
    stops._datasets = utils.datasetsConfig['datasets'];
    let stps = await stops.createStopList('test');
    expect(stps['@graph'].length).toBeGreaterThan(0);
});

test('Test to retrieve list of routes', async () => {
    expect.assertions(1);
    let routes = new Routes();
    routes._storage = utils.datasetsConfig['storage'];
    routes._datasets = utils.datasetsConfig['datasets'];
    let rts = await routes.createRouteList('test');
    expect(rts['@graph'].length).toBeGreaterThan(0);
});

test('Simulate http request for routes', async () => {
    expect.assertions(2);
    const routes = new Routes();
    routes._storage = utils.datasetsConfig['storage'];
    routes._datasets = utils.datasetsConfig['datasets'];
    let res = {
        headers: new Map(),
        toSend: null,
        set: h => {},
        send(data) { this.toSend = data }
    };
    await routes.getRoutes({ params: { agency: 'test' } }, res);
    expect(res.toSend).not.toBeNull();
    res.toSend = null;
    await routes.getRoutes({ params: { agency: 'test' } }, res);
    expect(res.toSend).not.toBeNull();
});

test('Test to create DCAT catalog', async () => {
    expect.assertions(2);
    fs.mkdirSync(`${utils.datasetsConfig['storage']}/catalog`);
    fs.mkdirSync(`${utils.datasetsConfig['storage']}/catalog/test`);
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
        send() { }
    };
    let catalog = new Catalog();
    await catalog.getCatalog({ params: { agency: "fakeAgency"}}, res);
    expect(res.sts).toBe(404);
    await catalog.getCatalog({ params: { agency: "test" }}, res);
    await catalog.getCatalog({ params: { agency: "test" }}, res);
    await del([`${utils.datasetsConfig['storage']}/catalog`], { force: true});
    catalog._storage = utils.datasetsConfig['storage'];
    catalog._datasets = utils.datasetsConfig['datasets'];
    let cat = await catalog.createCatalog('test');
    expect(cat['@context']).toBeDefined();
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