var winston = require('winston');
var moment = require('moment');

var logger = new (winston.Logger)({
    transports:[
        new (require('winston-daily-rotate-file'))({
            level: 'debug',
            filename: 'dmap-exporter',
            dirname: __dirname + '/../logs',
            datePattern: '.yyyy-MM-dd',
            timestamp: function() {
                return moment().format('YYYY-MM-DD HH:mm:ss');
            },
            json: false,
            colorize: true,
            humanReadableUnhandledException: true
        }),
		new (winston.transports.Console)({
            level: 'debug',
            datePattern: '.yyyy-MM-dd',
            timestamp: function() {
                return moment().format('YYYY-MM-DD HH:mm:ss');
            },
            json: false,
            colorize: true,
            humanReadableUnhandledException: true
        })
    ],
    exceptionHandlers: [
        new (require('winston-daily-rotate-file'))({
            level: 'debug',
            filename: 'dmap-exporter-exception',
            dirname: __dirname + '/../logs',
            datePattern: '.yyyy-MM-dd',
            timestamp: function() {
                return moment().format('YYYY-MM-DD HH:mm:ss');
            },
            json: false,
            colorize: true,
            humanReadableUnhandledException: true
        }),
		new (winston.transports.Console)({
            level: 'debug',
            datePattern: '.yyyy-MM-dd',
            timestamp: function() {
                return moment().format('YYYY-MM-DD HH:mm:ss');
            },
            json: false,
            colorize: true,
            humanReadableUnhandledException: true
        })
    ],
    exitOnError: false
});

module.exports = logger;
module.exports.stream = {
    write: function(message, encoding) {
        logger.info(message);
    }
};
