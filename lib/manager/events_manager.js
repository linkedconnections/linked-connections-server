const utils = require('../utils/utils.js');
const Logger = require('../utils/logger');
const fs = require('fs');
const readFile = util.promisify(fs.readFile);

class EventsManager {
    constructor() {
        this._pendingEvents = [];
        this._eventsStorage;
        this._logger = Logger.getLogger(utils.serverConfig.logLevel || 'info');

        // Start publishing timer
        setInterval(this._publisher, utils.datasetsConfig.realTimeUpdateInterval, this);
    }

    /**
     * Add an event for publication. The manager will automatically publish it later.
     * @param channel for example: delays, cancellations, ...
     * @param data which describes the information of the event
     */
    addEvent(id, channel, agency, data) {
        // Check if we're pushing valid events to the queue
        if(typeof id === 'undefined' || id === null) {
            console.error('Invalid ID, unable to push event to pending events!');
            return;
        }
        if(typeof channel === 'undefined' || channel === null) {
            console.error('Invalid channel name, unable to push event to pending events!');
            return;
        }
        if(typeof agency === 'undefined' || agency === null) {
            console.error('Invalid agency name, unable to push event to pending events!');
            return;
        }
        if(typeof data === 'undefined' || data === null) {
            console.error('Invalid data, unable to push event to pending events!');
            return;
        }

        // Add event to the queue
        this.pendingEvents.push({
            id: id,
            channel: channel,
            data: data,
            generatedAt: new Date()
        });
    }

    /**
     * Callback method which automatically publishes the events that are pending after the `setInterval` timer has run out.
     * @param self provides access to `this` of the EventsManager in the callback method.
     * @private
     */
    async _publisher(self) {
        // Skeleton of the event publication
        let publicationData = new Date();
        let template = await readFile('./statics/events_skeleton.jsonld', { encoding: 'utf8' });
        let skeleton = JSON.parse(template);

        // Read each pending event, add it to the knowledge graph
        while(self.pendingEvents.length > 0) {
            // Keep order of publication using .shift()
            let eventData = self.pendingEvents.shift();
            // Add LOD voc here
            let event = {
                '@id': 'http://irail.be/event/' + eventData['id'],
                '@type': 'Event',
                'sosa:resultTime': eventData['generatedAt'],
                'sosa:hasResult': {
                    '@id': eventData['data']['@id'],
                    '@type': 'sosa:hasResult',
                    'Connection': eventData['Connection']
                }
            };
            skeleton['@graph'].push(event);
        }

        // Process the publication of the events
        let publicationFileStream = fs.createWriteStream(self.eventsStorage + '/'
            + publicationData.toISOString() + '.jsonld', { flags:'w' });
        publicationFileStream.write(JSON.stringify(skeleton));
        publicationFileStream.end();
        this.logger.debug(`Publication of pending events (${publicationData}) succesfully!`);
    }

    get pendingEvents() {
        return this._pendingEvents;
    }

    set pendingEvents(pE) {
        this._pendingEvents = pE;
    }

    get eventsStorage() {
        return this._eventsStorage;
    }

    get logger() {
        return this._logger;
    }
}

module.exports = EventsManager;