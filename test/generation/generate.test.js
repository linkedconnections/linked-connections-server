const fs = require('fs');
const del = require('del');
const util = require('util');
const DSM = require('../../lib/manager/dataset_manager');
const utils = require('../../lib/utils/utils');
const { Connections, Connections2JSONLD } = require('gtfs2lc');
const jsonldstream = require('jsonld-stream');
const pageWriterStream = require('../../lib/manager/pageWriterStream');
const readdir = util.promisify(fs.readdir);

var dsm = new DSM();
dsm._storage = __dirname + '/storage';
dsm._datasets = [
    {
        "companyName": "test",
        "downloadUrl": "./test/generation/raw_data/cancelled_static.zip",
        "updatePeriod": "0 0 3 * * *",
        "fragmentSize": 50000,
        "baseURIs": {
            "stop": "http://example.test/stations/{stops.stop_id}",
            "route": "http://example.test/routes/{routeName}/{routes.route_id}",
            "trip": "http://example.test/trips/{trips.trip_headsign}/{trips.service_id}",
            "connection": "http://example.test/connections/{connection.departureStop}/{routeName}/{tripStartTime}/",
            "resolve": {
                "routeName": "routes.route_long_name.replace(/\\s/gi, '')",
                "tripStartTime": "format(trips.startTime, 'yyyyMMdd\\'T\\'HHmm')"
            }
        }
    }
];
var source = null;
var decompressed = null;
var unsorted = null;
var sorted = null;

// Should take around 17s to complete all tests but Travis is not the fastest.
jest.setTimeout(30000);

// Clean up after tests.
afterAll(async () => {
    await del([
        dsm.storage + '/tmp',
        dsm.storage + '/real_time',
        dsm.storage + '/datasets',
        dsm.storage + '/stops',
        dsm.storage + '/linked_connections',
        dsm.storage + '/linked_pages'
    ], { force: true });
});

test('Test creation of required folders', async () => {
    expect.assertions(6);
    dsm.initDirs();
    dsm.initCompanyDirs(dsm._datasets[0]['companyName']);
    expect(fs.existsSync(dsm.storage + '/tmp')).toBeTruthy();
    expect(fs.existsSync(dsm.storage + '/real_time/test')).toBeTruthy();
    expect(fs.existsSync(dsm.storage + '/datasets/test')).toBeTruthy();
    expect(fs.existsSync(dsm.storage + '/stops/test')).toBeTruthy();
    expect(fs.existsSync(dsm.storage + '/linked_connections/test')).toBeTruthy();
    expect(fs.existsSync(dsm.storage + '/linked_pages/test')).toBeTruthy();
});

test('Test downloading GTFS source', async () => {
    expect.assertions(1);
    source = await dsm.downloadDataset(dsm._datasets[0]);
    expect(source).not.toBeNull();
});

test('Test unzipping and pre-sorting GTFS source', async () => {
    expect.assertions(2);
    decompressed = await utils.readAndUnzip(dsm.storage + '/datasets/test/' + source + '.zip');
    expect(decompressed).not.toBeNull();
    await dsm.preSortGTFS(decompressed);
    expect(fs.existsSync(decompressed + '/connections.txt')).toBeTruthy();
});

test('Test creating Linked Connections', () => {
    expect.assertions(1);
    return new Promise((resolve, reject) => {
        let connGen = new Connections({});
        connGen.resultStream(decompressed, (connStream, stopsdb) => {
            connStream.pipe(new Connections2JSONLD(dsm._datasets[0]['baseURIs'], stopsdb))
                .pipe(new jsonldstream.Serializer())
                .pipe(fs.createWriteStream(dsm.storage + '/linked_connections/test/unsorted.jsonld', 'utf8'))
                .on('finish', () => {
                    resolve(dsm.storage + '/linked_connections/test/unsorted.jsonld');
                });
        });
    }).then(path => {
        unsorted = path;
        expect(unsorted).not.toBeNull();
    });


});

test('Test sorting Connections by departure time', async () => {
    expect.assertions(1);
    sorted = dsm.storage + '/linked_connections/test/sorted.jsonld'
    await dsm.sortLCByDepartureTime(unsorted, sorted);
    expect(fs.existsSync(sorted)).toBeTruthy();
});

test('Test fragmenting the Linked Connections', async () => {
    expect.assertions(1);
    return new Promise((resolve, reject) => {
        fs.createReadStream(sorted, 'utf8')
            .pipe(new jsonldstream.Deserializer())
            .pipe(new pageWriterStream(dsm.storage + '/linked_pages/test/', dsm._datasets[0]['fragmentSize']))
            .on('finish', () => {
                resolve();
            })
            .on('error', err => {
                reject(err);
            });
    }).then(async () => {
        expect((await readdir(dsm.storage + '/linked_pages/test/')).length).toBeGreaterThan(0);
    });
});

// Add live config params to start gtfs-rt related tests
dsm._datasets[0]['realTimeData'] = {
    "downloadUrl": "./test/generation/raw_data/cancelled_live",
    "updatePeriod": "*/30 * * * * *",
    "fragmentTimeSpan": 180,
    "compressionPeriod": "0 0 0 1 * *",
    "indexStore": "MemStore"
};

test('Test loading all required GTFS indexes to process GTFS-RT updates', async () => {
    expect.assertions(4);
    await dsm.loadGTFSIdentifiers(0, dsm._datasets[0], dsm.storage + '/real_time/test/.indexes');
    expect(dsm.indexes[0]['routes'].size).toBeGreaterThan(0);
    expect(dsm.indexes[0]['trips'].size).toBeGreaterThan(0);
    expect(dsm.indexes[0]['stops'].size).toBeGreaterThan(0);
    expect(dsm.indexes[0]['stop_times'].size).toBeGreaterThan(0);
});

test('Test processing a GTFS-RT update', async () => {
    expect.assertions(1);
    await dsm.processLiveUpdate(0, dsm._datasets[0], dsm.storage + '/real_time/test', {});
    let size = (await readdir(dsm.storage + '/real_time/test')).length;
    expect(size).toBeGreaterThan(0);
});

test('Call functions to increase coverage', async () => {
    expect.assertions(11);
    await expect(dsm.manage()).resolves.not.toBeDefined();
    expect(dsm.launchStaticJob(0, dsm._datasets[0])).not.toBeDefined();
    expect(await dsm.processStaticGTFS(0, dsm._datasets[0])).not.toBeDefined();
    expect(dsm.launchRTJob(0, dsm._datasets[0])).not.toBeDefined();
    expect(dsm.rtCompressionJob(dsm._datasets[0])).not.toBeDefined();
    await expect(dsm.downloadDataset({ downloadUrl: 'https' })).rejects.toBeDefined();
    await expect(dsm.download_http()).rejects.toBeDefined();
    await expect(dsm.download_https()).rejects.toBeDefined();
    expect(dsm.cleanRemoveCache({'2020-01-25T10:00:00.000Z': []}, new Date())).toBeDefined();
    expect(dsm.storeRemoveList([['key', { '@id': 'id', track: [] }]], dsm.storage + '/real_time/test', new Date())).not.toBeDefined();
    await expect(dsm.cleanUpIncompletes()).resolves.toHaveLength(1);
});