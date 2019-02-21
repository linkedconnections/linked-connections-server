const util = require('util');
const fs = require('fs');
const path = require('path');
const chokidar = require('chokidar');
const Logger = require('../utils/logger.js');
const utils = require('../utils/utils.js');
const EventsManager = require('../manager/events_manager.js');
const readDir = util.promisify(fs.readdir);
const readFile = util.promisify(fs.readFile);

class Events {
    constructor() {
        this._listeners = [];
        this._lastPubSubTime = new Date();
        this._eventsStorage = utils.datasetsConfig.storage + '/events';
        this._logger =  Logger.getLogger(utils.serverConfig.logLevel || 'info');
        this._knownAgencies = [];
        /*this._watcher = chokidar.watch('./sncb', {
            persistent: true,
            cwd: this.eventsStorage // Only events data should be watched
        });*/
        this._reconnectionTime = utils.datasetsConfig.reconnectionTime;

        // Trigger publication to listeners when new events are added by the EventsManager
        /*this.watcher.on('add', (path) => {
            console.log(path);
            this.handlePubSub('update', path);
        });*/

        /*fs.watch(this.eventsStorage, function(event, targetfile){
            console.log(targetfile, 'is', event);
        });*/
    }

    async getEvents(req, res) {
        let t0 = new Date();
        let t1;
        let lastSyncTime = new Date(decodeURIComponent(req.query.lastSyncTime));

        // Redirect to NOW time in case provided date is invalid, missing events are ignored.
        if (lastSyncTime.toString() === 'Invalid Date') {
            console.warn('Invalid data received, unable to retrieve missing events');
            lastSyncTime = new Date();
        }

        let agency = req.params.agency;
        // Check if we have this agency already in our cache
        if(this.knownAgencies.indexOf(agency) > -1) {
            // Maybe this agency was recently added, check it before rejecting
            let dir = path.join(this.eventsStorage, agency);
            if(!fs.existsSync(dir)) {
                this.logger.error(`Unable to retrieve events for agency: ${agency}, agency doesn't exist`);
                res.status(404);
                res.json({
                    'error': 404,
                    'message': `Agency ${agency} not found`
                });
                return;
            }
            // New agency, push it to the cache
            else {
                this.knownAgencies.push(agency);
            }
        }
        // REJECT unknown agencies TODO

        if(req.headers.accept.indexOf('text/event-stream') > -1) {
            this.logger.debug('SSE client connected');
            this.addListenerPubSub(req, res, lastSyncTime, agency);
        }
        else {
            this.logger.debug('HTTP polling client connected');
            await this.handlePolling(req, res, lastSyncTime, agency);
        }

        t1 = new Date();
        this.logger.info('Handling events client took: ' + (t1.getTime() - t0.getTime()) + ' ms' );
    }

    addListenerPubSub(req, res, lastSyncTime, agency) {
        // Set the required HTTP headers for SSE
        res.setHeader('Content-Type', 'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        res.setHeader('Expires', 0);
        res.setHeader('Connection', 'keep-alive');

        // Generate listeners queue for new agencies
        if(!(agency in this.listeners)) {
            this.logger.debug(`Event SSE request for a new agency: ${agency}, creating listeners queue...`)
            this.listeners[agency] = [];
        }

        // Add the listener to the pool, attach the 'close' event to remove the listener when the connection is closed.
        this.listeners[agency].push(res);
        res.on('close', () => {
            this.listeners[agency].splice(this.listeners[agency].indexOf(res), 1);
        });

        // Send missing events since lastSyncTime (only for this listener)
        res.write('data: test init');
    }

    handlePubSub(eventName, path) {
        // Read event TODO
        let agency = 'sncb';

        // Publish SSE events for all listeners
        console.log(this.listeners[agency]);
        this.listeners[agency].forEach((client) => {
            client.write(`event: ${eventName}\n`);
            client.write(`retry: ${this.reconnectionTime}\n`);
            client.write('data: ' + 'test' + '\n');
            client.write('\n'); // Indicate end
        });
    }

    async handlePolling(req, res, lastSyncTime, agency) {
        // Set the required HTTP headers for polling
        res.setHeader('Cache-Control', 'max-age=' + utils.datasetsConfig.realTimeUpdateInterval);

        // Send missing events since lastSyncTime
        let events = await this.getEventsPage(req, res, new Date(lastSyncTime), agency);
        res.json(events);
    }

    /**
     * Gets all the generated events between the from timestamp and the the until timestamp.
     * @param when the timestamp of the events we need
     * @param agency the company for which we're publishing events
     */
    async getEventsPage(req, res, when, agency) {

        // Check all published events and compile a list of all events in the given time range.
        let page = {};
        let entries = await readDir(dir);
        console.log(entries);
        for(let e=1; e < entries.length-1; e++) {
            let eventsDateBeforeBefore = null;
            if(e >= 2) {
                eventsDateBeforeBefore = new Date(path.basename(entries[e - 2], '.jsonld')); // previous hydra when page found
            }
            let eventsDateBefore = new Date(path.basename(entries[e-1], '.jsonld'));
            let eventsDateAfter = new Date(path.basename(entries[e+1], '.jsonld'));
            // Linear search, maybe we can improve this later with a binary search? TODO
            console.log('BEFORE: ' + eventsDateBefore.toISOString());
            console.log('WHEN: ' + when.toISOString());
            console.log('AFTER: ' + eventsDateAfter.toISOString());
            if(eventsDateBefore.getTime() <= when.getTime() && when.getTime() <= eventsDateAfter.getTime()) {
                page = await readFile(path.join(dir, entries[e-1]));
                page = JSON.parse(page);
                page = this.addHydraMetaData(req, res, page, agency, eventsDateBeforeBefore, eventsDateBefore, eventsDateAfter);
                return page;
            }
        }

        this.logger.error(`No page found for timestamp: ${when.toISOString()}`);
        throw Promise.reject(`No page found for timestamp: ${when.toISOString()}`);
    }

    addHydraMetaData(req, res, page, agency, previousPageTimestamp, currentPageTimestamp, nextPageTimestamp) {
        // Determine protocol (i.e. HTTP or HTTPS)
        let x_forwarded_proto = req.headers['x-forwarded-proto'];
        let protocol = '';

        if (typeof x_forwarded_proto == 'undefined' || x_forwarded_proto == '') {
            if (typeof utils.serverConfig.protocol == 'undefined' || utils.serverConfig.protocol == '') {
                protocol = 'http';
            } else {
                protocol = utils.serverConfig.protocol;
            }
        } else {
            protocol = x_forwarded_proto;
        }

        let host = protocol + '://' + utils.serverConfig.hostname + '/';
        let eventsURI = host + agency + '/events?lastSyncTime=';
        let pageURI = host + agency + '/connections?departureTime=';

        // Update graph
        page['@id'] = eventsURI + currentPageTimestamp.toISOString();
        page['hydra:previous'] = eventsURI + previousPageTimestamp.toISOString();
        page['hydra:next'] = eventsURI + nextPageTimestamp.toISOString();
        let graph = page['@graph'];
        for(let g=0; g < graph.length; g++) {
            graph[g]['hydra:view'] = pageURI + graph[g]['sosa:hasResult']['Connection']['departureTime'];
        }

        return page;
    }

    get listeners() {
        return this._listeners;
    }

    set listeners(l) {
        this._listeners = l;
    }

    get lastPubSubTime() {
        return this._lastPubSubTime;
    }

    get logger() {
        return this._logger;
    }

    get eventsStorage() {
        return this._eventsStorage;
    }

    get watcher() {
        return this._watcher;
    }

    get reconnectionTime() {
        return this._reconnectionTime;
    }

    get knownAgencies() {
        return this._knownAgencies;
    }
}

module.exports = Events;
