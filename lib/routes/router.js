const express = require('express');
const PageFinder = require('./page-finder');
const Memento = require('./memento');
const Catalog = require('./catalog');
const Stops = require('./stops');
const Routes = require('./routes');
const Feed = require('./feed');

const router = express.Router();
const pageFinder = new PageFinder();
const memento = new Memento();
const catalog = new Catalog();
const stops = new Stops();
const routes = new Routes();
const feed = new Feed();

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


feed.init().then(() => {
    router.get('/:agency/connections/feed', (req, res) => {
        feed.getFeed(req, res);
    });
});

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

module.exports = router; 