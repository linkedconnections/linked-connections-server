const utils = require('../utils/utils.js');
const Logger = require('../utils/logger');
const fs = require('fs');
const readFile = util.promisify(fs.readFile);

class EventsManager {
    constructor() {
        this._pendingEvents = {};
        this._eventsStorage = utils.datasetsConfig.storage + '/events';
        this._logger = Logger.getLogger(utils.serverConfig.logLevel || 'info');

        // NodeJS 10.X.X or later
        if (!fs.existsSync(this.eventsStorage)) {
            fs.mkdirSync(this.eventsStorage, { recursive: true });
        }

        // Start publishing timer
        setInterval(this._publisher, utils.datasetsConfig.realTimeUpdateInterval, this);
    }

    /**
     * Add an event for publication. The manager will automatically publish it later.
     * @param pageURI where the connection can be found.
     * @param connection which describes the information of the event (connection).
     * @param agency the agency of the connection.
     */
    addEvent(pageURI, connection, agency) {
        // Check if we're pushing valid events to the queue
        if(typeof pageURI === 'undefined' || pageURI === null) {
            console.error('Invalid page URI, unable to push event to pending events!');
            return;
        }
        if(typeof connection === 'undefined' || connection === null) {
            console.error('Invalid connection, unable to push event to pending events!');
            return;
        }
        if(typeof agency === 'undefined' || agency === null) {
            console.error('Invalid agency name, unable to push event to pending events!');
            return;
        }

        // Create agency if needed
        if(this.pendingEvents[agency] === 'undefined') {
            this.logger.debug(`Events received for a new agency: ${agency}, creating storage...`)
            this.pendingEvents[agency] = [];
            // NodeJS 10.X.X or later
            if (!fs.existsSync(this.eventsStorage + '/' + agency)) {
                fs.mkdirSync(this.eventsStorage + '/' + agency, { recursive: true });
            }
        }

        // Add event to the queue
        this.pendingEvents[agency].push({
            id: new Date(),
            pageURI: pageURI,
            connection: connection,
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
        for(let pE=0; pE < self.pendingEvents.keys().length; pE++) {
            let agency = self.pendingEvents.keys()[pE];
            let pendingEventsForAgency = self.pendingEvents[agency];
            while (pendingEventsForAgency.length > 0) {
                // Keep order of publication using .shift()
                let eventData = pendingEventsForAgency.shift();
                // Add LOD vocabulary for each event
                let event = {
                    '@id': eventData['connection']['@id'] + '#' + eventData['id'],
                    '@type': 'Event',
                    'hydra:view': eventData['pageURI'],
                    'sosa:resultTime': eventData['id'],
                    'sosa:hasResult': {
                        '@id': eventData['connection']['@id'],
                        '@type': 'sosa:hasResult',
                        'Connection': eventData['connection']
                    }
                };
                skeleton['@graph'].push(event);
            }

            // Process the publication of the events
            let publicationFileStream = fs.createWriteStream(self.eventsStorage + '/'
                + agency + publicationData.toISOString() + '.jsonld', { flags:'w' });
            publicationFileStream.write(JSON.stringify(skeleton));
            publicationFileStream.end();
            this.logger.debug(`Publication of pending events (${publicationData}) successfully for agency: ${agency}!`);
        }
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

    get agency() {
        return this._agency;
    }
}

module.exports = EventsManager;