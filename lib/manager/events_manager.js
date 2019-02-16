const utils = require('../utils/utils.js');
const Logger = require('../utils/logger');
const fs = require('fs');
const util = require('util');
const readFile = util.promisify(fs.readFile);

class EventsManager {
    constructor() {
        this._pendingEvents = {
            'sncb': [{
                id: new Date(),
                pageURI: 'http://graph.irail.be/connections?departureTime=2019-01-01T00:00:00.000Z',
                connection: {
                    "@id": "http://irail.be/connections/S103859/20190216/8812062",
                    "@type": "Connection",
                    "departureStop": "http://irail.be/stations/8812062",
                    "arrivalStop": "http://irail.be/stations/8812047",
                    "departureTime": "2019-02-16T09:35:00.000Z",
                    "arrivalTime": "2019-02-16T09:39:00.000Z",
                    "gtfs:trip": "http://irail.be/vehicle/S103859/20190216",
                    "gtfs:route": "http://irail.be/routes/S103859",
                    "direction": "Alost",
                    "gtfs:pickupType": "gtfs:Regular",
                    "gtfs:dropOffType": "gtfs:Regular"
                }
            }, {
                id: new Date(),
                pageURI: 'http://graph.irail.be/connections?departureTime=2019-01-01T01:00:00.000Z',
                connection: {
                    "@id": "http://irail.be/connections/IC2509/20190216/8811445",
                    "@type": "Connection",
                    "departureStop": "http://irail.be/stations/8811445",
                    "arrivalStop": "http://irail.be/stations/8811460",
                    "departureTime": "2019-02-16T09:35:00.000Z",
                    "arrivalTime": "2019-02-16T09:36:00.000Z",
                    "gtfs:trip": "http://irail.be/vehicle/IC2509/20190216",
                    "gtfs:route": "http://irail.be/routes/IC2509",
                    "direction": "Dinant",
                    "gtfs:pickupType": "gtfs:NotAvailable",
                    "gtfs:dropOffType": "gtfs:NotAvailable"
                }
            }]
        };
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
            id: new Date().toISOString(),
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
        for(let pE=0; pE < Object.keys(self.pendingEvents).length; pE++) {
            let pendingEventsForAgencyCounter = 0;
            let agency = Object.keys(self.pendingEvents)[pE];
            // NodeJS 10.X.X or later
            if (!fs.existsSync(self.eventsStorage + '/' + agency)) {
                fs.mkdirSync(self.eventsStorage + '/' + agency, { recursive: true });
            }

            let pendingEventsForAgency = self.pendingEvents[agency];
            if(pendingEventsForAgency.length == 0) {
                self.logger.debug(`No events pending for agency: ${agency}`)
                continue;
            }
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
                        '@id': eventData['connection']['@id']+ '#' + eventData['id'],
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
}

module.exports = EventsManager;