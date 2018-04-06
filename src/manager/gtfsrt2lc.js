const url = require('url');
const http = require('follow-redirects').http;
const https = require('follow-redirects').https;
const zlib = require('zlib');
const gtfsrt = require('gtfs-realtime-bindings');
const moment = require('moment-timezone');
const logger = require('../utils/logger');
const fs = require('fs');

class Gtfsrt2lc {
    constructor(dataset, stores) {
        this._dataset = dataset;
        this._routesStore = stores['routes'];
        this._tripsStore = stores['trips'];
    }

    processFeed() {
        return new Promise(async (resolve, reject) => {
            try {
                const durl = url.parse(this.dataset.realTimeData.downloadUrl);
                let feed = await this.fetchFeed(durl);
                resolve(this.buildConnections(feed, this.dataset.baseURIs));
            } catch (err) {
                reject(err);
            }
        });
    }

    fetchFeed(url) {
        return new Promise((resolve, reject) => {
            if (url.protocol === 'https:') {
                let req = https.request(url, res => {
                    let encoding = res.headers['content-encoding']
                    let responseStream = res;
                    let buffer = false;

                    if (encoding && encoding == 'gzip') {
                        responseStream = res.pipe(zlib.createGunzip());
                    } else if (encoding && encoding == 'deflate') {
                        responseStream = res.pipe(zlib.createInflate())
                    }

                    responseStream.on('data', chunk => {
                        if (!buffer) {
                            buffer = chunk;
                        } else {
                            buffer = Buffer.concat([buffer, chunk], buffer.length + chunk.length);
                        }
                    });
                    res.on('error', error => {
                        reject(error);
                    });
                    responseStream.on('end', () => {
                        resolve(buffer);
                    })
                });

                req.on('error', err => {
                    reject(err);
                });

                req.end();
            } else {
                let req = http.request(url, res => {
                    let encoding = res.headers['content-encoding']
                    let responseStream = res;
                    let buffer = false;

                    if (encoding && encoding == 'gzip') {
                        responseStream = res.pipe(zlib.createGunzip());
                    } else if (encoding && encoding == 'deflate') {
                        responseStream = res.pipe(zlib.createInflate())
                    }

                    responseStream.on('data', chunk => {
                        if (!buffer) {
                            buffer = chunk;
                        } else {
                            buffer = Buffer.concat([buffer, chunk], buffer.length + chunk.length);
                        }
                    });
                    res.on('error', error => {
                        reject(error);
                    });
                    responseStream.on('end', () => {
                        resolve(buffer);
                    })
                });

                req.on('error', err => {
                    reject(err);
                });

                req.end();
            }
        });
    }

    async buildConnections(data, uris) {
        // parse the RT data to JSON object
        let feed = '';
        try {
            feed = gtfsrt.FeedMessage.decode(data);
        } catch (err) {
            return null;
        }

        let array = [];
        let index = 0;

        // Snippet to quickly dump decoded real-time updates to a json file
        /*fs.writeFile('/home/julian/Desktop/rt-update.json', JSON.stringify(feed), err => {
            if(err) console.error(err);
        });*/

        for (let i = 0; i < feed.entity.length; i++) {
            if (feed.entity[i].trip_update) {
                var trip_update = feed.entity[i].trip_update;
                var trip_id = trip_update.trip.trip_id;

                // Check if train is canceled or not
                var type = this.getConnectionType(feed.entity[i]);

                // for each stop time update
                let stop_times = trip_update.stop_time_update;
                let st_length = stop_times.length;

                for (let j = 0; j < st_length; j++) {
                    try {
                        if (j + 1 < st_length) {
                            var departureStop = stop_times[j].stop_id.split(':')[0];
                            var arrivalStop = trip_update.stop_time_update[j + 1].stop_id.split(':')[0];
                            var departureTime = null;
                            var arrivalTime = null;
                            var departureDelay = 0;
                            var arrivalDelay = 0;

                            if (stop_times[j].departure && stop_times[j].departure.time && stop_times[j].departure.time.low) {
                                departureTime = moment(stop_times[j].departure.time.low * 1000);
                            }

                            if (stop_times[j + 1].arrival && stop_times[j + 1].arrival.time && stop_times[j + 1].arrival.time.low) {
                                arrivalTime = moment(stop_times[j + 1].arrival.time.low * 1000);
                            }

                            if (stop_times[j].departure && stop_times[j].departure.delay) {
                                departureDelay = stop_times[j].departure.delay;
                            }

                            if (stop_times[j].arrival && stop_times[j].arrival.delay) {
                                arrivalDelay = stop_times[j].arrival.delay;
                            }

                            // WARNING: HACK TO CORRECT NMBS 1 HOUR DISPLACEMENT OF REAL-TIME UPDATES
                            //-----------------------------------------------------------------------
                            departureTime.subtract(1, 'h');
                            arrivalTime.subtract(1, 'h');
                            //-----------------------------------------------------------------------

                            // Get Trip and Route short names from GTFS stores
                            let gtfs_trip = await this.tripsStore.get(trip_id);
                            let trip_short_name = gtfs_trip.trip_short_name;
                            let gtfs_route = await this.routesStore.get(gtfs_trip.route_id);
                            let route_short_name = gtfs_route.route_short_name;

                            let connectionId = uris.connections + departureStop + '/' + encodeURIComponent(departureTime.format('YYYYMMDD')) + '/'
                                + route_short_name + trip_short_name;

                            var obj = {
                                "@id": connectionId,
                                "@type": type,
                                "departureStop": uris.stops + departureStop,
                                "arrivalStop": uris.stops + arrivalStop,
                                "departureTime": departureTime.toISOString(),
                                "arrivalTime": arrivalTime != null ? arrivalTime.toISOString() : null,
                                "direction": gtfs_trip.trip_headsign,
                                "departureDelay": departureDelay,
                                "arrivalDelay": arrivalDelay,
                                "gtfs:trip": uris.trips + route_short_name + trip_short_name + '/' + encodeURIComponent(departureTime.format('YYYYMMDD')),
                                "gtfs:route": uris.routes + route_short_name + trip_short_name
                            }

                            array.push(obj);
                        }
                    } catch (err) {
                        logger.warn('Trip id ' + trip_id + ' not found in current Trips store');
                        continue;
                    }
                }
            }
        }

        return array;
    }

    getConnectionType(entity) {
        if (entity.is_deleted) {
            return 'CanceledConnection';
        }
        else {
            return 'Connection';
        }
    }

    get dataset() {
        return this._dataset;
    }

    get routesStore() {
        return this._routesStore;
    }

    get tripsStore() {
        return this._tripsStore;
    }
}

module.exports = Gtfsrt2lc;