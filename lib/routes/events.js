const Logger = require('../utils/logger.js');
const utils = require('../utils/utils.js');

class Events {
    constructor() {
        this._listeners = [];
    }

    getEvents(req, res) {
        let t0 = new Date();
        let t1;

        if(req.headers.accept.indexOf('text/event-stream') > -1) {
            Logger.debug('SSE client connected');
            this.handlePubSub(req, res);
        }
        else {
            Logger.debug('HTTP polling client connected');
            this.handlePolling(req, res);
        }

        t1 = new Date();
        Logger.info('Handling events client took: ' + (t1.getTime() - t0.getTime()) + ' ms' );
    }

    handlePubSub(req, res) {
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
    }

    handlePolling(req, res) {
        // Set tje required HTTP headers for polling
        res.setHeader('Cache-Control', 'max-age=' + utils.datasetsConfig.realTimeUpdateInterval);

    }

    get listeners() {
        return this._listeners;
    }

    set listeners(l) {
        this._listeners = l;
    }
}

module.exports = Events;