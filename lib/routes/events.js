const util = require('util');
const fs = require('fs');
const path = require('path');
const Logger = require('../utils/logger.js');
const utils = require('../utils/utils.js');
const EventsManager = require('../manager/events_manager.js');
const readDir = util.promisify(fs.readdir);
const readFile = util.promisify(fs.readFile);

class Events {
    constructor() {
        this._listeners = [];
        this._currentEvents = {};
        this._eventsStorage = utils.datasetsConfig.storage + '/events';
        this._logger =  Logger.getLogger(utils.serverConfig.logLevel || 'info');

        // Connect to EventsManager events using .on('event')
    }

    async getEvents(req, res) {
        let t0 = new Date();
        let t1;
        console.log(req);
        console.log(req.query);
        let lastSyncTime = new Date(decodeURIComponent(req.query.lastSyncTime));

        // Redirect to NOW time in case provided date is invalid, missing events are ignored.
        if (lastSyncTime.toString() === 'Invalid Date') {
            console.warn('Invalid data received, unable to retrieve missing events');
            lastSyncTime = new Date();
        }

        let agency = req.params.agency;
        if(req.headers.accept.indexOf('text/event-stream') > -1) {
            this.logger.debug('SSE client connected');
            this.handlePubSub(req, res, lastSyncTime, agency);
        }
        else {
            this.logger.debug('HTTP polling client connected');
            await this.handlePolling(req, res, lastSyncTime, agency);
        }

        t1 = new Date();
        this.logger.info('Handling events client took: ' + (t1.getTime() - t0.getTime()) + ' ms' );
    }

    handlePubSub(req, res, lastSyncTime, agency) {
        // Set the required HTTP headers for SSE
        res.setHeader('Content-Type', 'text/event-stream');
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
        res.setHeader('Expires', 0);
        res.setHeader('Connection', 'keep-alive');


        // Add the listener to the pool, attach the 'close' event to remove the listener when the connection is closed.
        this.listeners.push(res);
        res.on('close', () => {
            this.listeners.splice(this.listeners.indexOf(res), 1);
        });

        // Send missing events since lastSyncTime
        res.json({});
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
        // We can't retrieve events for non-existing agencies
        let dir = path.join(this.eventsStorage, agency);
        if(!fs.existsSync(dir)) {
            this.logger.error(`Unable to retrieve events for agency: ${agency}, agency doesn't exist`);
            return Promise.reject(`Unable to retrieve events for agency: ${agency}, agency doesn't exist`);
        }

        // Check all published events and compile a list of all events in the given time range.
        let page = {};
        let entries = await readDir(dir);
        for(let e=1; e < entries.length-1; e++) {
            let eventsDateBeforeBefore = null;
            if(e >= 2) {
                eventsDateBeforeBefore = new Date(path.basename(entries[e - 2], '.jsonld')); // previous hydra when page found
            }
            let eventsDateBefore = new Date(path.basename(entries[e-1], '.jsonld'));
            let eventsDateAfter = new Date(path.basename(entries[e+1], '.jsonld'));
            // Linear search, maybe we can improve this later with a binary search? TODO
            if(eventsDateBefore.getTime() <= when.getTime() && when.getTime() <= eventsDateAfter.getTime()) {
                page = await readFile(path.join(dir, entries[e-1]));
                page = JSON.parse(page);
                page = this.addHydraMetaData(req, res, page, agency, eventsDateBeforeBefore, eventsDateBefore, eventsDateAfter);
            }
        }

        this.logger.error(`No page found for timestamp: ${when.toISOString()}`);
        return page;
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
        let uri = host + agency + '/events?lastSyncTime=';

        // Update graph
        page['@id'] = uri + currentPageTimestamp.toISOString();
        page['hydra:previous'] = uri + previousPageTimestamp.toISOString();
        page['hydra:next'] = uri + nextPageTimestamp.toISOString();

        return page;
    }

    get listeners() {
        return this._listeners;
    }

    set listeners(l) {
        this._listeners = l;
    }

    get currentEvents() {
        return this._currentEvents;
    }

    get logger() {
        return this._logger;
    }

    get eventsStorage() {
        return this._eventsStorage;
    }
}

module.exports = Events;