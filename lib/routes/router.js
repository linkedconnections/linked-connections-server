import express from 'express';
import { PageFinder } from './page-finder.js';
import { Memento } from './memento.js';
import { Catalog } from './catalog.js';
import { Stops } from './stops.js';
import { Routes } from './routes.js';

/**
 * This functionality is deprecated. A Linked Connections LDES feed will be implemented elsewhere.
 * const Feed = require('./feed');
 */

const router = express.Router();
const pageFinder = new PageFinder();
const memento = new Memento();
const catalog = new Catalog();
const stops = new Stops();
const routes = new Routes();
/** const feed = new Feed(); */

// Define HTTP routes only after required data has been initialized
pageFinder.init().then(() => {
    router.get('/:agency/connections', (req, res) => {
        pageFinder.getConnections(req, res);
    });
});

memento.init().then(() => {
    router.get('/:agency/connections/memento', (req, res) => {
        memento.getMemento(req, res);
    });
});


/**feed.init().then(() => {
    router.get('/:agency/connections/feed', (req, res) => {
        feed.getFeed(req, res);
    });
    router.get('/:agency/connections/feed/shape', async (req, res) => {
        let host = req.headers.host;

        let shape = await utils.addSHACLMetadata({
            "id": utils.serverConfig['protocol'] + '://' + host + req.originalUrl,
            "ldes": utils.serverConfig['protocol'] + '://' + host + req.originalUrl.substring(0, req.originalUrl.indexOf("shape"))
        });

        res.set({'Content-Type': 'application/ld+json'});
        res.status(200).send(shape);
    });
});*/

router.get('/:agency/catalog', (req, res) => {
    catalog.getCatalog(req, res);
});

router.get('/:agency/stops', (req, res) => {
    stops.getStops(req, res);
});

router.get('/:agency/routes', (req, res) => {
    routes.getRoutes(req, res);
});

router.options('/*', (req, res) => {
    res.set({
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': '*',
        'Content-Type': 'application/ld+json'
    });

    res.sendStatus(200);
});

export default router;