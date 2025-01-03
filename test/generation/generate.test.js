import { jest, test, expect, afterAll } from '@jest/globals';
import fs from 'fs';
import util from 'util';
import path from 'path';
import { deleteAsync as del } from 'del';
import { DatasetManager } from '../../lib/manager/dataset_manager.js';
import { Utils } from '../../lib/utils/utils.js';
import cp from 'child_process';
import JsonLParser from 'stream-json/jsonl/Parser.js';
import { PageWriterStream } from '../../lib/manager/pageWriterStream.js';

const __dirname = path.resolve();
const utils = new Utils();
const readdir = util.promisify(fs.readdir);
const exec = util.promisify(cp.exec);

var dsm = new DatasetManager();
dsm._storage = __dirname + '/test/generation/storage';
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

jest.setTimeout(15000);

// Clean up after tests.
afterAll(async () => {
    await del([
        dsm.storage + '/tmp',
        dsm.storage + '/real_time',
        dsm.storage + '/datasets',
        dsm.storage + '/stops',
        dsm.storage + '/routes',
        dsm.storage + '/catalog',
        dsm.storage + '/linked_connections',
        dsm.storage + '/linked_pages'
    ], { force: true });
});

test('Test creation of required folders', async () => {
    expect.assertions(8);
    dsm.initDirs();
    dsm.initCompanyDirs(dsm._datasets[0]['companyName']);
    expect(fs.existsSync(dsm.storage + '/tmp')).toBeTruthy();
    expect(fs.existsSync(dsm.storage + '/real_time/test')).toBeTruthy();
    expect(fs.existsSync(dsm.storage + '/datasets/test')).toBeTruthy();
    expect(fs.existsSync(dsm.storage + '/stops/test')).toBeTruthy();
    expect(fs.existsSync(dsm.storage + '/routes/test')).toBeTruthy();
    expect(fs.existsSync(dsm.storage + '/catalog/test')).toBeTruthy();
    expect(fs.existsSync(dsm.storage + '/linked_connections/test')).toBeTruthy();
    expect(fs.existsSync(dsm.storage + '/linked_pages/test')).toBeTruthy();
});

test('Test downloading GTFS source', async () => {
    expect.assertions(1);
    source = await dsm.getDataset(dsm._datasets[0]);
    expect(source).not.toBeNull();
});

test('Test unzipping GTFS source', async () => {
    expect.assertions(1);
    decompressed = await utils.readAndUnzip(dsm.storage + '/datasets/test/' + source + '.zip');
    expect(decompressed).not.toBeNull();
});

test('Test creating Linked Connections', async () => {
    await exec(`./node_modules/gtfs2lc/bin/gtfs2lc.js -f jsonld --fresh --compressed ${decompressed}`);
    unsorted = `${decompressed}/linkedConnections.json.gz`;
    expect.assertions(1);
    expect(fs.existsSync(unsorted)).toBeTruthy();
});

test('Test sorting Connections by departure time', async () => {
    expect.assertions(1);
    sorted = `${dsm.storage}/linked_connections/test/sorted.json`
    const sortedConns = await dsm.sortLCByDepartureTime(unsorted);
    const writer = fs.createWriteStream(sorted, "utf8");

    for await (const data of sortedConns) {
        writer.write(data);
    }
    writer.end();
    expect(fs.existsSync(sorted)).toBeTruthy();
});

test('Test fragmenting the Linked Connections', async () => {
    expect.assertions(1);
    fs.mkdirSync(`${dsm.storage}/linked_pages/test/sorted`);
    const readStream = fs.createReadStream(sorted, 'utf8')
        .pipe(JsonLParser.parser({ checkErrors: false })).on('error', (err) => console.error(err));
    const writer = new PageWriterStream(`${dsm.storage}/linked_pages/test/sorted`, dsm._datasets[0]['fragmentSize']);

    for await (const data of readStream) {
        writer.write(data);
    }
    writer.end();
    expect((await readdir(`${dsm.storage}/linked_pages/test/`)).length).toBeGreaterThan(0);
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