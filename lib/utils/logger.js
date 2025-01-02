import winston from 'winston';

const levels = ['error', 'warn', 'info', 'verbose', 'debug', 'silly'];

export const getLogger = function (level) {
    if (levels.indexOf(level) < 0) level = 'info';

    return winston.createLogger({
        level: level,
        format: winston.format.combine(
            winston.format.colorize(),
            winston.format.timestamp({
                format: 'YYYY-MM-DD HH:mm:ss'
            }),
            winston.format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
        ),
        transports: [
            new winston.transports.Console()
        ]
    });
}