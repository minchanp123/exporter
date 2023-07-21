const async = require('async');
const fs = require('fs-extra');
const logger = require('../lib/logger');
const slack = require('../lib/slack');
const config = require('../lib/config');

var documents = (function () {
	var sourceProcessingPath = './documents_processing/';
	var sourceTopicsPath = './topics/'; //화제어 분석을 하는 경우
	var sourceEmotionsPath = './emotions/'; //화제어 분석을 하는 경우
	var sourceDonePath = './documents_done/'; //원문 insert만 하는 경우

	var process_50 = function (sourcePath, callback) {
		var MAX_DOCUMENT_EXPORTER_SIZE = config.THREAD_CNT.THREAD_DOCUMENT; //1;
		var customerKeywordArr = [];

		async.waterfall([
			//Step#1 CUSTOMER 폴더 목록 가져오기
			function (callback) {
				logger.info('[documents] Step#1 CUSTOMER 폴더 목록 가져오기');

				fs.readdir(sourcePath, function (err, customers) {
					if (err) {
						callback(err);
					} else {
						if (customers.length === 0) {
							callback('ERR_NO_CUSTOMERS');
						} else {
							callback(null, customers);
						}
					}
				});
			},
			//Step#2 CUSTOMER 폴더 처리
			function (customers, callback) {
				logger.info('[documents] Step#2 CUSTOMER 폴더 처리 ' + customers.length);

				async.eachSeries(customers, function (customer, callback) {
					async.waterfall([
						//Step#3 KEYWORD 목록 가져오기
						function (callback) {
							logger.info('[documents] Step#3 KEYWORD 목록 가져오기');
							// './topics/' + customer
							fs.readdir(sourcePath + customer, function (err, keywords) {
								if (err) {
									callback(err);
								} else {
									if (keywords.length === 0) {
										callback('ERR_NO_KEYWORDS');
									} else {
										callback(null, keywords);
									}
								}
							}); //readdir (CUSTOMER)
						},
						//Step#4 KEYWORD 폴더 처리
						function (keywords, callback) {
							logger.info('[documents] Step#2-2 CUSTOMER-KEYWORD 관계 리스트 생성 ' + keywords.length);
							async.eachSeries(keywords, function (keyword, callback) {
								customerKeywordArr.push({
									customer: customer,
									keyword: keyword
								});
								callback(null);
							}, function (err) {
								if (err) {
									callback(err);
								} else {
									callback(null);
								}
							}); //eachSeries (KEYWORD)
						}
					], function (err) {
						if (err) {
							if (err === 'ERR_NO_KEYWORDS') {
								logger.warn('[documents] ' + err);
								callback(null);
							} else {
								callback(err);
							}
						} else {
							callback(null);
						}
					}); //waterfall (CUSTOMER)
				}, function (err) {
					if (err) {
						callback(err);
					} else {
						callback(null);
					}
				}); //eachSeries (CUSTOMER)
			},
			//Step#3 MAX_DOCUMENT_EXPORTER_SIZE 만큼 documentsSaver 생성 및 수행 시작
			function (callback) {
				const customerKeywords = init(customerKeywordArr, MAX_DOCUMENT_EXPORTER_SIZE);
				if (customerKeywords.length != 0) {
					logger.info('[documents] Step#3 documentsSaver 생성 및 수행 시작 50documentsSaver size : ' + MAX_DOCUMENT_EXPORTER_SIZE);
					async.eachOfLimit(customerKeywords, MAX_DOCUMENT_EXPORTER_SIZE, function (customerKeyword, ret_num, callback) {
						var documentsSaver = require('./50documentsSaver');
						documentsSaver.process(sourcePath, customerKeyword, ret_num, callback); //save	
					}, function (err) {
						if (err) {
							callback(err);
						} else {
							logger.debug('[documents] End of documentsSaver');
							callback(null);
						}
					}); //eachOfLimit (customerKeywords);
				} else {
					callback(null);
				}
			}
		], function (err) {
			if (err) {
				if (err === 'ERR_NO_CUSTOMERS') {
					logger.warn('[documents] ' + err);
					callback(null);
				} else {

					// slack alert
					var slackMessage = {
						color: 'danger',
						title: 'documents',
						value: '[documents] documents process failed : ' + err
					};

					slack.sendMessage(slackMessage, function (err) {
						if (err) {
							logger.error('[documents] ' + err);
							callback(err);
						} else {
							logger.info('[documents] Successfully push message to Slack');
							callback(err);
						}
					});
				}
			} else {
				callback(null);
			}
		}); //waterfall
	}; //process

	var process = function (sourcePath, callback) {
		var MAX_DOCUMENT_EXPORTER_SIZE = 4;
		var customerKeywordArr = [];

		async.waterfall([
			//Step#1 CUSTOMER 폴더 목록 가져오기
			function (callback) {
				logger.info('[documents] Step#1 CUSTOMER 폴더 목록 가져오기');

				fs.readdir(sourcePath, function (err, customers) {
					if (err) {
						callback(err);
					} else {
						if (customers.length === 0) {
							callback('ERR_NO_CUSTOMERS');
						} else {
							callback(null, customers);
						}
					}
				});
			},
			//Step#2 CUSTOMER 폴더 처리
			function (customers, callback) {
				logger.info('[documents] Step#2 CUSTOMER 폴더 처리 ' + customers.length);

				async.eachSeries(customers, function (customer, callback) {
					async.waterfall([
						//Step#3 KEYWORD 목록 가져오기
						function (callback) {
							logger.info('[documents] Step#3 KEYWORD 목록 가져오기');
							// './topics/' + customer
							fs.readdir(sourcePath + customer, function (err, keywords) {
								if (err) {
									callback(err);
								} else {
									if (keywords.length === 0 || customer === 'nongshim' || customer === 'wisenut' || customer === 'Ipsos') {
										callback('ERR_NO_KEYWORDS');
									} else {
										callback(null, keywords);
									}
								}
							}); //readdir (CUSTOMER)
						},
						//Step#4 KEYWORD 폴더 처리
						function (keywords, callback) {
							logger.info('[documents] Step#2-2 CUSTOMER-KEYWORD 관계 리스트 생성 ' + keywords.length);
							async.eachSeries(keywords, function (keyword, callback) {
								customerKeywordArr.push({
									customer: customer,
									keyword: keyword
								});
								callback(null);
							}, function (err) {
								if (err) {
									callback(err);
								} else {
									callback(null);
								}
							}); //eachSeries (KEYWORD)
						}
					], function (err) {
						if (err) {
							if (err === 'ERR_NO_KEYWORDS') {
								logger.warn('[documents] ' + err);
								callback(null);
							} else {
								callback(err);
							}
						} else {
							callback(null);
						}
					}); //waterfall (CUSTOMER)
				}, function (err) {
					if (err) {
						callback(err);
					} else {
						callback(null);
					}
				}); //eachSeries (CUSTOMER)
			},
			//Step#3 MAX_DOCUMENT_EXPORTER_SIZE 만큼 documentsSaver 생성 및 수행 시작
			function (callback) {
				var customerKeywords = init(customerKeywordArr, MAX_DOCUMENT_EXPORTER_SIZE);
				if (customerKeywords.length != 0) {
					logger.info('[documents] Step#3 documentsSaver 생성 및 수행 시작 size : ' + MAX_DOCUMENT_EXPORTER_SIZE);
					async.eachOfLimit(customerKeywords, MAX_DOCUMENT_EXPORTER_SIZE, function (customerKeyword, ret_num, callback) {
						var documentsSaver = require('./documentsSaver');
						documentsSaver.process(sourcePath, customerKeyword, ret_num, callback); //save	
					}, function (err) {
						if (err) {
							var esDao = require('../dao/esDao');
							esDao.refreshIndexes('documents', function (err2) {
								logger.debug('[documents] refreshIndexes');
								if (err2) {
									callback(err + err2);
								} else {
									callback(err);
								}
							});

						} else {
							logger.debug('[documents] End of documentsSaver');
							var esDao = require('../dao/esDao');
							esDao.refreshIndexes('documents', function (err2) {
								logger.debug('[documents] refreshIndexes');
								if (err2) {
									callback(err2);
								} else {
									callback(null);
								}
							});
							//-----------refresh
							//callback(null);//callback(customerKeywords);
						}
						if (err) {
							callback(err);
						} else {
							callback(null);
						}
					}); //eachOfLimit (customerKeywords);
				} else {
					callback(null);
				}
			}
		], function (err) {
			if (err) {
				if (err === 'ERR_NO_CUSTOMERS') {
					logger.warn('[documents] ' + err);
					callback(null);
				} else {

					// slack alert
					var slackMessage = {
						color: 'danger',
						title: 'documents',
						value: '[documents] documents process failed : ' + err
					};

					slack.sendMessage(slackMessage, function (err) {
						if (err) {
							logger.error('[documents] ' + err);
							callback(err);
						} else {
							logger.info('[documents] Successfully push message to Slack');
							callback(err);
						}
					});
				}
			} else {
				callback(null);
			}
		}); //waterfall
	}; //process

	/*
	화제어 처리모듈 갯수만큼 배열을 생성, 처리할 리스트들을 분배
	*/
	var init = function (customerKeywords, MAX_TOPIC_EXPORTER_SIZE) {
		var tempArr = [];

		if (customerKeywords.length != 0) {
			for (var i = 0; i < MAX_TOPIC_EXPORTER_SIZE; i++) {
				tempArr.push([]);
			}

			for (var i = 0; i < customerKeywords.length; i++) {
				var number = i % MAX_TOPIC_EXPORTER_SIZE;
				tempArr[number].push(customerKeywords[i]);
			}
		}
		return tempArr;
	}

	return {
		process_50: process_50,
		process: process
	};

})();

if (exports) {
	module.exports = documents;
}