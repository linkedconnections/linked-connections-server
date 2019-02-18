const utils = require('../utils/utils.js');
const Logger = require('../utils/logger');
const md5 = require('md5');
const path = require('path');
const fs = require('fs');
const util = require('util');
const readFile = util.promisify(fs.readFile);

class EventsManager {
    constructor() {
        this._previousEvents = {};
        this._pendingEvents = {};
        this._eventsStorage = utils.datasetsConfig.storage + '/events';
        this._logger = Logger.getLogger(utils.serverConfig.logLevel || 'info');

        // Populate events and start interval publishing timer
        this._publisher(this).then(() => {
            let interval = utils.datasetsConfig.realTimeUpdateInterval;
            setInterval(this._publisher, interval, this);
        });
    }

    /**
     * Add an event for publication. The manager will automatically publish it later.
     * @param pageURI where the connection can be found.
     * @param connection which describes the information of the event (connection).
     * @param agency the agency of the connection.
     */
    addEvent(connection, agency) {
        // Check if we're pushing valid events to the queue
        if(typeof connection === 'undefined' || connection === null) {
            console.error('Invalid connection, unable to push event to pending events!');
            return;
        }
        if(typeof agency === 'undefined' || agency === null) {
            console.error('Invalid agency name, unable to push event to pending events!');
            return;
        }

        // Create agency if it doesn't exist yet
        if(!(agency in this.pendingEvents)) {
            this.logger.debug(`Events received for a new agency: ${agency}, creating storage...`)
            this.pendingEvents[agency] = [];
            this.previousEvents[agency] = [];
            // NodeJS 10.X.X or later
            if (!fs.existsSync(this.eventsStorage + '/' + agency)) {
                fs.mkdirSync(this.eventsStorage + '/' + agency, { recursive: true });
            }
        }

        // Event is already published, check if the event has been updated
        let isNew = false;
        let existingEvent = this.previousEvents[agency].find(event => event['@id'] === connection['@id']);

        // Already generated this event, check if it's updated...
        if(typeof existingEvent !== 'undefined') {
            let type = existingEvent['@type'];
            let departureDelay = existingEvent['departureDelay'];
            let arrivalDelay = existingEvent['arrivalDelay'];
            if(type !== connection['@type'] || departureDelay !== connection['departureDelay'] || arrivalDelay !== connection['arrivalDelay']) {
                isNew = true;
                this.logger.debug('Existing event has been updated for connection: ' + connection['@id']);
                if(type !== connection['@type']) {
                    this.logger.debug('Reason: ' + connection['@type']);
                }

                if(departureDelay !== connection['departureDelay']) {
                    this.logger.debug('Reason: departure delay ' + existingEvent['departureDelay'] + ' min -> ' + connection['departureDelay'] + ' min');
                }

                if(arrivalDelay !== connection['arrivalDelay']){
                    this.logger.debug('Reason: arrival delay ' + existingEvent['arrivalDelay'] + ' min -> ' + connection['arrivalDelay'] + ' min');
                }

                // Existing event updated, removing the old one
                this.previousEvents[agency].splice(this.previousEvents[agency].indexOf(existingEvent), 1);
            }
        }
        // New event
        else {
            isNew = true;
        }

        // Add event to the queue if changed, update the previousEvents with the new event
        if(isNew) {
            this.pendingEvents[agency].push({
                id: new Date(),
                connection: connection,
            });
            this.previousEvents[agency].push(connection);
        }
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
        for(let pE=0; pE < Object.keys(self.pendingEvents).length; pE++) {
            let pendingEventsForAgencyCounter = 0;
            let agency = Object.keys(self.pendingEvents)[pE];
            // NodeJS 10.X.X or later
            if (!fs.existsSync(self.eventsStorage + '/' + agency)) {
                fs.mkdirSync(self.eventsStorage + '/' + agency, { recursive: true });
            }

            let pendingEventsForAgency = self.pendingEvents[agency];
            if(pendingEventsForAgency.length === 0) {
                self.logger.debug(`No events pending for agency: ${agency}`)
                continue;
            }
            while (pendingEventsForAgency.length > 0) {
                // Keep order of publication using .shift()
                let eventData = pendingEventsForAgency.shift();
                // Add LOD vocabulary for each event
                let event = {
                    '@id': eventData['connection']['@id'] + '#' + eventData['id'].toISOString(),
                    '@type': 'Event',
                    'hydra:view': '',
                    'sosa:resultTime': eventData['id'].toISOString(),
                    'sosa:hasResult': {
                        '@type': 'sosa:hasResult',
                        'Connection': eventData['connection']
                    }
                };
                skeleton['@graph'].push(event);
                pendingEventsForAgencyCounter++;
            }

            // Process the publication of the events
            let publicationFileStream = fs.createWriteStream(self.eventsStorage + '/'
                + agency + '/' + publicationData.toISOString() + '.jsonld', { flags:'w' });
            publicationFileStream.write(JSON.stringify(skeleton));
            publicationFileStream.end();
            self.logger.debug(`Publication of ${pendingEventsForAgencyCounter} pending events (${publicationData.toISOString()}) successfully for agency: ${agency}`);
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

    get previousEvents() {
        return this._previousEvents;
    }
}

module.exports = EventsManager;