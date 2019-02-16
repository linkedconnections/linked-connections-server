const redis = require('redis');
const utils = require('../utils/utils.js');

class EventsManager {
    constructor() {
        // Create Redis client and subscribe on all subscriptions
        this._redis = redis.createClient();
        this._subscriptions = utils.datasetsConfig.subscriptions;
        for(let s=0; s < this.subscriptions.length; s++) {
            this.redis.subscribe(this.subscriptions[s]);
        }

        // Attach callbacks to Redis events
        this.redis.on('error', this.handleError(err));
        this.redis.on('message', this.handleMessage(channel, message));
    }

    /**
     * Retrieves all the occured events since the given last sync time.
     * @param lastSyncTime
     */
    getEventsSince(lastSyncTime) {
        console.debug('Retrieving events since: ' + lastSyncTime.getISOString());
        // Look up in the REDIS store all the events between now and the lastSyncTime
    }

    /**
     * Prints each Redis error to the console and emits the 'error' event.
     * @param err
     */
    handleError(err) {
        console.error(`Redis error: ${err}`);
        // emit here error signal for EventsManager
    }

    /**
     * Prints each Redis message and channel to the console (in debugging mode). Afterwards, it emits the 'message' event.
     * @param channel
     * @param message
     */
    handleMessage(channel, message) {
        console.debug(`Received message: ${message} on channel: ${channel}`);
        // emit here msg signal for EventsManager
    }

    get redis() {
        return this._redis;
    }

    get subscriptions() {
        return this._subscriptions;
    }
}

module.exports = EventsManager;