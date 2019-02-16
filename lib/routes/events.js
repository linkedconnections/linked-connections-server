const Logger = require('../utils/logger.js');
const utils = require('../utils/utils.js');
const EventsManager = require('../manager/events_manager.js');

class Events {
    constructor() {
        this._listeners = [];
        this._currentEvents = {};
        this._EventsManager = new EventsManager();

        // Connect to EventsManager events using .on('event')
    }

    getEvents(req, res) {
        let t0 = new Date();
        let t1;
        let lastSyncTime = new Date(decodeURIComponent(req.query.lastSyncTime));

        // Redirect to NOW time in case provided date is invalid, missing events are ignored.
        if (lastSyncTime.toString() === 'Invalid Date') {
            console.warn('Invalid data received, unable to retrieve missing events');
            lastSyncTime = new Date();
        }

        if(req.headers.accept.indexOf('text/event-stream') > -1) {
            Logger.debug('SSE client connected');
            this.handlePubSub(req, res, lastSyncTime);
        }
        else {
            Logger.debug('HTTP polling client connected');
            this.handlePolling(req, res, lastSyncTime);
        }

        t1 = new Date();
        Logger.info('Handling events client took: ' + (t1.getTime() - t0.getTime()) + ' ms' );
    }

    handlePubSub(req, res, lastSyncTime) {
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
        res.json(this.EventsManager.getEventsSince(lastSyncTime));
    }

    handlePolling(req, res, lastSyncTime) {
        // Set the required HTTP headers for polling
        res.setHeader('Cache-Control', 'max-age=' + utils.datasetsConfig.realTimeUpdateInterval);

        // Send missing events since lastSyncTime
        res.json(this.EventsManager.getEventsSince(lastSyncTime));
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

    get EventsManager() {
        return this._EventsManager;
    }
}

module.exports = Events;