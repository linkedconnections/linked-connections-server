const express = require('express');
const PageFinder = require('./page-finder');
const Memento = require('./memento');
const Catalog = require('./catalog');
const Stops = require('./stops');
const Routes = require('./routes');

const router = express.Router();

router.get('/:agency/connections', (req, res) => {
    new PageFinder().getConnections(req, res);
});

router.get('/:agency/connections/memento', (req, res) => {
    new Memento().getMemento(req, res);
});

router.get('/:agency/catalog', (req, res) => {
    new Catalog().getCatalog(req, res);
});

router.get('/:agency/stops', (req, res) => {
    new Stops().getStops(req, res);
});

router.get('/:agency/routes', (req, res) => {
    new Routes().getRoutes(req, res);
});

router.options('/*', (req, res) => {
    res.set({ 
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': '*', 
        'Content-Type': 'application/ld+json' 
    });

    res.send(200);
});

module.exports = router; 