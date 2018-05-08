# Linked Connections Server
Express based Web Server that exposes [Linked Connections](http://linkedconnections.org/) data fragments using [JSON-LD](https://json-ld.org/) serialization format. It also provides a built-in tool to parse [GTFS](https://developers.google.com/tansit/gtfs/reference/) and [GTFS Realtime](https://developers.google.com/transit/gtfs-realtime/) transport dataset feeds into a Linked Connections Directed Acyclic Graph using [GTFS2LC](https://github.com/linkedconnections/gtfs2lc) and fragment it following a configurable predefined size.

## Installation
First make sure to have [Node](https://nodejs.org/en/) 8.x or superior installed. To install the server proceed as follows:
``` bash
$ git clone https://github.com/julianrojas87/linked-connections-server.git
$ cd linked-connections-server
$ npm install
```

## Configuration
The configuration is made through two different config files. One is for defining Web Server parameters ([server_config.json](https://github.com/julianrojas87/linked-connections-server/blob/master/server_config.json)) and the other is for defining the different data sources that will be managed and exposed through the Linked Connections Server ([datasets_config.json](https://github.com/julianrojas87/linked-connections-server/blob/master/datasets_config.json)). Next you could find an example and a description of each config file.

### Web Server configuration
As mentioned above the Web server configuration is made using the ([server_config.json](https://github.com/julianrojas87/linked-connections-server/blob/master/server_config.json)) config file which uses the JSON format and defines the following properties:

- **hostname:** Used to define the Web Server host name. **Is a mandatory parameter**.

- **port:** TCP/IP port to be used by the Web Server to receive requests. **Is a mandatory parameter**.

- **protocol:** Used to define the accepted protocol by the Web Server which could be either HTTP o HTTPS. In case that both protocols are supported there is no need to define this parameter, but all requests made to the server **MUST** contain the **X-Forwarded-Proto** header stating the procotol being used. This is useful when the server is used along with cache management servers.

- **logLevel:** Used to define the logging level of the server. We use the [Winston](https://github.com/winstonjs/winston#logging-levels) library to manage logs. If not specified, the default level is `info`.

This is a configuration example:

```js
{
    "hostname": "localhost:3000",
    "port": 3000,
    "protocol": "http" // or https
    "logLevel": "info" //error, warn, info, verbose, debug, silly
}
```

### Datasets configuration
The Web Server does not provide any functionality by itself, it needs at least one dataset (in GTFS format) that can be downloaded to be processed and exposed as Linked Connections. To tell the server where to find and store such datasets, we use the ([datasets_config.json](https://github.com/julianrojas87/linked-connections-server/blob/master/datasets_config.json)) config file. All the parameters in this config file are **Mandatory**, otherwise the server won't function properly. This file contains the following parameters:

- **storage:** This is the path that tells the server where to store and where to look for the data fragments, created from the different datasets. **This should not include a trailing slash**. Make sure you have enough disk space to store and process datasets.

- **companyName:** Name of the transport company that provides a GTFS dataset feed.

- **downloadUrl:** URL where the GTFS dataset feed can be downloaded.

- **downloadOnLaunch:** Boolean parameter that indicates if the GTFS feed is to be downloaded and processed upon server launch.

- **updatePeriod:** Cron expression that defines how often should the server look for and process a new version of the dataset. We use the [node-cron](https://github.com/kelektiv/node-cron) library for this.

- **fragmentSize:** Defines the maximum size of every linked data fragment in bytes.

- **realTimeData:** If available, here we define all the parameters related with a GTFS-RT feed.

    - **downloadUrl:** Here we define the URL to download the GTFS-RT data feed.

    - **updatePeriod:** Cron expression that defines how often should the server look for and process a new version of the dataset. We use the [node-cron](https://github.com/kelektiv/node-cron) library for this.

    - **fragmentTimeSpan:** This defines the fragmentation of real-time data. It represents the time span of every fragment in seconds.

    - **compressionPeriod:** Cron expression that defines how often will the real-time data be compressed using gzip in order to reduce storage consumption.

- **baseURIs:** Here we define the base URIs that will be used to create the unique identifiers of each of the entities found in the Linked Connections. Is necessary to define the base URI for [Connections](http://semweb.datasciencelab.be/ns/linkedconnections#Connection), [Stops](https://github.com/OpenTransport/linked-gtfs/blob/master/spec.md), [Trips](https://github.com/OpenTransport/linked-gtfs/blob/master/spec.md) and [Routes](https://github.com/OpenTransport/linked-gtfs/blob/master/spec.md). This is the only optional parameter and in case that is not defined, all base URIs will have a http://example.org/ pattern, but we recommend to always use dereferenceable URIs.

Here is an example of how to configure it:
```js
{
    "storage": "/opt/linked-connections-data", //datasets storage path
    "datasets":[
        {
            "companyName": "companyX",
            "downloadUrl": "http://...",
            "downloadOnLaunch": true,
            "updatePeriod": "0 0 2 * * *", //every day at 2am
            "fragmentSize": 400000, //400 Kb
            "realTimeData": {
                "downloadUrl": "http://...",
                "updatePeriod": "*/30 * * * * *", //every 30s
                "fragmentTimeSpan": 600, // 10 minutes
                "compressionPeriod": "0 0 3 * * *" //every day at 3am
            },
            "baseURIs": {
                "connections": "http://example.org/connections/",
                "stops": "http://example.org/stops/",
                "trips": "http://example.org/trips/",
                "routes": "http://example.org/routes/"
            }
        },
        {
            "companyName": "companyY",
            "downloadUrl": "http://...",
            "downloadOnLaunch": false,
            "updatePeriod": "0 0 3 * * *", //every day at 3am
            "baseURIs": {
                "connections": "http://example.org/connections/",
                "stops": "http://example.org/stops/",
                "trips": "http://example.org/trips/",
                "routes": "http://example.org/routes/"
            }
        }
    ]
}
```
## Run it
Once you have properly configured the server you can start it like this:
```bash
$ cd linked-connections-server
$ npm start
```
After started your server will start fetching the datasets you configured according to their Cron configuration.

## Use it
To use it make sure you already have at least one fully processed dataset (the logs will tell you when). If so you can query the Linked Connections using the departure time as a parameter like this for example:
```http
http://localhost:3000/companyX/connections?departureTime=2017-08-11T16:45:00.000Z
```
If available, the server will redirect you to the Linked Connections fragment that contains connections with departure times as close as possible to the one requested.

## Historic Data
The server also allows quierying historic data by means of the [Memento Framework](https://tools.ietf.org/html/rfc7089) which enables time-based content negotiation over HTTP. By using the **Accept-Datetime** header a client can request the state of a resource at a given moment. If existing, the server will respond with a 302 Found containing the URI of the stored version of such resource. For example:
```bash
$ curl -v -L -H "Accept-Datetime: 2017-10-06T13:00:00.000Z" http://localhost:3000/companyX/connections?departureTime=2017-10-06T15:50:00.000Z

> GET /companyX/connections?departureTime=2017-10-06T15:50:00.000Z HTTP/1.1
> Host: localhost:3000
> User-Agent: curl/7.52.1
> Accept: */*
> Accept-Datetime: 2017-10-06T13:00:00.000Z

< HTTP/1.1 302 Found
< X-Powered-By: Express
< Access-Control-Allow-Origin: *
< Location: /memento/companyX?version=2017-10-28T03:07:47.000Z&departureTime=2017-10-06T15:50:00.000Z
< Vary: Accept-Encoding, Accept-Datetime
< Link: <http://localhost:3000/companyX/connections?departureTime=2017-10-06T15:50:00.000Z>; rel="original timegate"
< Date: Mon, 13 Nov 2017 15:00:36 GMT
< Connection: keep-alive
< Content-Length: 0

> GET /memento/companyX?version=2017-10-28T03:07:47.000Z&departureTime=2017-10-06T15:50:00.000Z HTTP/1.1
> Host: localhost:3000
> User-Agent: curl/7.52.1
> Accept: */*
> Accept-Datetime: 2017-10-06T13:00:00.000Z

< HTTP/1.1 200 OK
< X-Powered-By: Express
< Memento-Datetime: Fri, 06 Oct 2017 13:00:00 GMT
< Link: <http://localhost:3000/companyX/connections?departureTime=2017-10-06T15:50:00.000Z>; rel="original timegate"
< Access-Control-Allow-Origin: *
< Content-Type: application/ld+json; charset=utf-8
< Content-Length: 289915
< ETag: W/"46c7b-TOdDIcDjCvUXTC/gzqr5hxVDZjg"
< Date: Mon, 13 Nov 2017 15:00:36 GMT
< Connection: keep-alive
```
The previous example shows a request made to obtain the Connections fragment identified by the URL http://localhost:3000/companyX/connections?departureTime=2017-10-06T15:50:00.000Z, but specifically the state of this fragment as it was at **Accept-Datetime: 2017-10-06T13:00:00.000Z**. This means that is possible to know what was the state of the delays at 13:00 for the departures at 15:50 on 2017-10-06.

## Authors
Julian Rojas - julianandres.rojasmelendez@ugent.be  
Pieter Colpaert - pieter.colpaert@ugent.be  
