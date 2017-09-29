const winston = require('winston');

module.exports = new (winston.Logger)({
    level: 'info',
    handleExceptions: false,
    transports: [
        new (winston.transports.Console)({
            colorize: true,
            timestamp: true
        })
    ],
});