const winston = require('winston');
const levels = ['error', 'warn', 'info', 'verbose', 'debug', 'silly'];

const getLogger = function (level) {
    if(levels.indexOf(level) < 0) level = 'info';

    let transports = [
        new (winston.transports.Console)({
            colorize: true,
            timestamp: true
        })];

    return new (winston.Logger)({
        level: level,
        handleExceptions: false,
        transports: transports
    });
}

module.exports = { getLogger };