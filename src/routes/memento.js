const express = require('express');
const router = express.Router();
const fs = require('fs');
const zlib = require('zlib');


router.get('/:agency/:version/:resource', function (req, res) {
    console.log('sdfsdfsdfsdf');
    res.send();
});

module.exports = router;