const url = require('url');
const http = require('follow-redirects').http;
const https = require('follow-redirects').https;
const zlib = require('zlib');
const gtfsrt = require('gtfs-realtime-bindings');
const moment = require('moment-timezone');

module.exports.processFeed = function (datasetInfo, cb) {
    const durl = url.parse(datasetInfo.realTimeData.downloadUrl);
    fetchFeed(durl, (error, feed) => {
        if (!error) {
            let rtconn = buildConnections(feed, datasetInfo.baseURIs);
            if(rtconn != null) {
                cb(null, rtconn);
            } else {
                cb('error');
            }
        } else {
            cb(error);
        }
    });
}

function fetchFeed(url, cb) {
    if (url.protocol === 'https:') {
        let req = https.request(url, (res) => {
            let encoding = res.headers['content-encoding']
            let responseStream = res;
            let buffer = false;

            if (encoding && encoding == 'gzip') {
                responseStream = res.pipe(zlib.createGunzip());
            } else if (encoding && encoding == 'deflate') {
                responseStream = res.pipe(zlib.createInflate())
            }

            responseStream.on('data', function (chunk) {
                if (!buffer) {
                    buffer = chunk;
                } else {
                    buffer = Buffer.concat([buffer, chunk], buffer.length + chunk.length);
                }
            });
            res.on('error', function (error) {
                cb(error);
            });
            responseStream.on('end', function () {
                cb(null, buffer);
            })
        });

         req.on('error', (err) => {
            cb(err);
        });

        req.end();
    } else {
        let req = http.request(url, (res) => {
            let encoding = res.headers['content-encoding']
            let responseStream = res;
            let buffer = false;

            if (encoding && encoding == 'gzip') {
                responseStream = res.pipe(zlib.createGunzip());
            } else if (encoding && encoding == 'deflate') {
                responseStream = res.pipe(zlib.createInflate())
            }

            responseStream.on('data', function (chunk) {
                if (!buffer) {
                    buffer = chunk;
                } else {
                    buffer = Buffer.concat([buffer, chunk], buffer.length + chunk.length);
                }
            });
            res.on('error', function (error) {
                cb(error);
            });
            responseStream.on('end', function () {
                cb(null, buffer);
            })
        });

         req.on('error', (err) => {
            cb(err);
        });

        req.end();
    }
}

function buildConnections(data, uris) {
    // parse the RT data to JSON object
    let feed = '';
    try {
        feed = gtfsrt.FeedMessage.decode(data);
    } catch(err) {
        return null;
    }
    
    let array = [];
    let index = 0;

    for (let i = 0; i < feed.entity.length; i++) {
        if (feed.entity[i].trip_update) {
            var trip_update = feed.entity[i].trip_update;
            var trip_id = trip_update.trip.trip_id;
            var gtfs_route = feed.entity[i].trip_update.vehicle != null ? feed.entity[i].trip_update.vehicle.id : '';

            // Check if train is canceled or not
            var type = getConnectionType(feed.entity[i]);

            // for each stop time update
            let stop_times = trip_update.stop_time_update;
            let st_length = stop_times.length;

            for (let j = 0; j < st_length; j++) {
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

                    var connectionId = encodeURIComponent(((stop_times[j].departure.time.low * 1000) - (departureDelay * 1000)) + departureStop + trip_id);

                    var obj = {
                        "@id": uris.connections + connectionId,
                        "@type": type,
                        "departureStop": uris.stops + departureStop,
                        "arrivalStop": uris.stops + arrivalStop,
                        "departureTime": departureTime.toISOString(),
                        "arrivalTime": arrivalTime != null ? arrivalTime.toISOString() : null,
                        "departureDelay": departureDelay,
                        "arrivalDelay": arrivalDelay,
                        "gtfs:trip": uris.trips + trip_id,
                        "gtfs:route": gtfs_route != '' ? uris.routes + gtfs_route : ""
                    }

                    array.push(JSON.stringify(obj));
                }
            }
        }
    }

    return array;
}

function getConnectionType(entity) {
    if (entity.is_deleted) {
        return 'CanceledConnection';
    }
    else {
        return 'Connection';
    }
}