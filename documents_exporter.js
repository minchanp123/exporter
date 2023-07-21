var cluster = require('cluster');
var express = require('express');
var logger = require('./lib/logger_documents');

var sourcePaths = [
	'./documents/'
	//	,'./topics/' //<- topicMaker ./topics -> ./topics_processing -> ./emotions or ./topics_done
	//	,'./emotions/'
];

if (cluster.isMaster) {
	// 클러스터 워커 프로세스 포크
	for (var idx = 0; idx < sourcePaths.length; idx++) {
		cluster.fork();
	}

	cluster.on('exit', function (worker, code, signal) {
		logger.error('[index] worker ' + worker.process.pid + ' died - code : ' + code + ', signal : ' + signal);
	});

	var app = express();
	app.set('port', 6051);
	// app.use(morgan('combined', {'stream': logger.stream}));
	app.get('/health', function (req, res) {
		res.send('OK');
	});

	var server = app.listen(app.get('port'), function () {
		var host = server.address().address;
		var port = server.address().port;

		logger.info('[index] Server Listening on port %d', port);
	});
} else {
	var async = require('async');
	var sleep = require('sleep');

	var sourcePath = sourcePaths[cluster.worker.id - 1];
	logger.info('[index] Start Exporter Path ' + sourcePath);

	async.forever(
		function (next) {
			if (sourcePath === sourcePaths[0]) {
				logger.debug('[index] Start Documents Exporter');

				var documetExporter = require('./workflow/documents');
				var startTime = new Date();
				logger.debug('[documents] #EXECTIME#_START' + startTime + ' ms');

				documetExporter.process_50(sourcePath, function (err) {
					if (err) {
						logger.error('[index] ' + sourcePath + ' Error occured : ' + err);
						logger.info('documents Sleeping...');

						// sleep until 10 minutes
						//sleep.sleep(10 * 60);
						sleep.sleep(60);
						next(null);
					} else {
						logger.info('[index] ' + sourcePath + ' All files processed.');
						logger.info('[documents] Sleeping...');
						var endTime = new Date();
						logger.debug('[documents] #EXECTIME#_END' + endTime + ' ms');
						logger.debug('[documents] #EXECTIME#' + (endTime - startTime) + ' ms');

						// sleep until 10 minutes
						//sleep.sleep(10 * 60);
						sleep.sleep(60);
						next(null);
					}
				});
			} else if (sourcePath === sourcePaths[1]) {
				logger.debug('[index] Start Topics Analyzer');

				var topicsAnalyzer = require('./workflow/topics');
				var startTime = new Date();
				logger.debug('[topics] #EXECTIME#_START' + startTime + ' ms');

				topicsAnalyzer.process_50(sourcePath, function (err) {
					if (err) {
						logger.error('[index] ' + sourcePath + ' Error occured : ' + err);
						logger.info('topics Sleeping...');

						// sleep until 10 minutes
						//sleep.sleep(10 * 60);
						sleep.sleep(60);
						next(null);
					} else {
						logger.info('[index] ' + sourcePath + ' All files processed.');
						logger.info('[topics] Sleeping...');
						var endTime = new Date();
						logger.debug('[topics] #EXECTIME#_END' + endTime + ' ms');
						logger.debug('[topics] #EXECTIME#' + (endTime - startTime) + ' ms');

						// sleep until 10 minutes
						//sleep.sleep(10 * 60);
						sleep.sleep(60);
						next(null);
					}
				});
			} else if (sourcePath === sourcePaths[2]) {
				logger.debug('[index] Start Emotions Analyzer');
				var emotionsAnalyzer = require('./workflow/emotions');
				var startTime = new Date();
				logger.debug('[emotions] #EXECTIME#_START' + startTime + ' ms');

				emotionsAnalyzer.process_50(sourcePath, function (err) {
					if (err) {
						logger.error('[index] ' + sourcePath + ' Error occured : ' + err);
						logger.info('emotions Sleeping...');

						// sleep until 10 minutes
						//sleep.sleep(10 * 60);
						sleep.sleep(60);
						next(null);
					} else {
						logger.info('[index] ' + sourcePath + ' All files processed.');
						logger.info('[emotions] Sleeping...');
						var endTime = new Date();
						logger.debug('[emotions] #EXECTIME#_END' + endTime + ' ms');
						logger.debug('[emotions] #EXECTIME#' + (endTime - startTime) + ' ms');

						// sleep until 10 minutes
						//sleep.sleep(10 * 60);
						sleep.sleep(60)
						next(null);
					}
				});
			}
		},
		function (err) {

			if (err) {
				logger.error('[index] ' + err);

				// slack alert
				var slackMessage = {
					color: 'danger',
					title: 'index',
					value: '[index] application process failed : ' + err
				};

				slack.sendMessage(slackMessage, function (err) {
					if (err) {
						logger.error(err);
					} else {
						logger.info('[index] Successfully push message to Slack');
						cluster.worker.kill(-1);
					}
				});
			} else {
				logger.error('[index] Module unexpectedly finished.');
				cluster.worker.kill(0);
			}
		}
	);
}
