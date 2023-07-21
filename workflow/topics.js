const async = require('async');
const fs = require('fs-extra'); 
const logger = require('../lib/logger');
const slack = require('../lib/slack');
const config = require('../lib/config');

var topics = (function(){

	var process_50 = function(sourcePath, callback){
		const MAX_TOPIC_EXPORTER_SIZE = config.THREAD_CNT.THREAD_TOPIC; //1;
		var customerKeywordArr = [];
		async.waterfall([
			//Step#1 CUSTOMER 폴더 목록 가져오기
			function(callback) {
				logger.info('[topics] Step#1 CUSTOMER 폴더 목록 가져오기');

				fs.readdir(sourcePath, function(err, customers) {
                    if (err) {
                        callback(err);
                    } else {
                        if (customers.length === 0){
                        	callback('ERR_NO_CUSTOMERS');
                        } else {
							callback(null, customers);
						}
                    }
                });
			}, 
			//Step#2 CUSTOMER 폴더 처리
			function(customers, callback) {		
				logger.info('[topics] Step#2 CUSTOMER 폴더 처리 ' + customers.length);

				async.eachSeries(customers, function(customer, callback) {
					async.waterfall([
						//Step#3 KEYWORD 목록 가져오기
						function(callback) {      
								logger.info('[topics] Step#2-1 KEYWORD 목록 가져오기');
								// './topics/' + customer
								fs.readdir(sourcePath + customer, function(err, keywords) {
									if (err) {
										callback(err);
									} else {
										if (keywords.length === 0 ){
											callback('ERR_NO_KEYWORDS');
										} else {
											callback(null, keywords);
										}
									}
								}); //readdir (CUSTOMER)
						},
						//Step#4 KEYWORD 폴더 처리
						function(keywords, callback) {
							logger.info('[topics] Step#2-2 CUSTOMER-KEYWORD 관계 리스트 생성 ' + keywords.length);
							async.eachSeries(keywords, function(keyword, callback) {
								customerKeywordArr.push(
									{customer : customer, keyword : keyword}
								);
								callback(null);
							}, function(err) {
								if (err) {
									callback(err);
								} else {
									callback(null);
								}
							}); //eachSeries (KEYWORD)
						}
					], function(err) {
						if(err) {
							if(err === 'ERR_NO_KEYWORDS') {
								logger.warn('[topics] ' + err);
								callback(null);
							} else {
								callback(err);
							}							
						} else {
							callback(null);
						}
					}); //waterfall (CUSTOMER)
				}, function(err) {
					if(err) {
						callback(err);
					} else {
						callback(null);
					}
				}); //eachSeries (CUSTOMER)
			},
			//Step#3 MAX_TOPIC_EXPORTER_SIZE 만큼 topicExporter 생성 및 수행 시작
			function(callback){
				var customerKeywords = init(customerKeywordArr, MAX_TOPIC_EXPORTER_SIZE);
				if (customerKeywords.length != 0){
					logger.info('[topics] Step#3 50topicExporter 생성 및 수행 시작 size : ' + MAX_TOPIC_EXPORTER_SIZE);
					async.eachOfLimit(customerKeywords, MAX_TOPIC_EXPORTER_SIZE, function(customerKeyword, ret_num, callback){
						var topicsSaver = require('./50topicsSaver');
						topicsSaver.process(sourcePath, customerKeyword, ret_num, callback); //save	
					}, function(err) {
						if(err) {
							callback(err);
						} else {
							logger.debug('[topicMaker] End TopicsSaver');
							callback(null);//callback(customerKeywords);
						}
					}); //eachOfLimit (customerKeywords);
				}else{
					callback(null);
				}
			}
		], function(err) {
			if(err) {
				if(err === 'ERR_NO_CUSTOMERS') {
					logger.warn('[topics] ' + err);
					callback(null);						
				} else {

					// slack alert
					var slackMessage = {
						color: 'danger',
						title: 'topics',
						value: '[topics] topics process failed : ' + err
					};
					
					slack.sendMessage(slackMessage, function(err) {
						if (err) {
							logger.error('[topics] ' + err);
							callback(err);
						} else {
							logger.info('[topics] Successfully push message to Slack');
							callback(err);
						}
					});
				} 
			} else {
				callback(null);
			}
		}); //waterfall
	}; //process_50
	
	var process = function(sourcePath, callback){
var MAX_TOPIC_EXPORTER_SIZE = 5;
		var customerKeywordArr = [];
		async.waterfall([
			//Step#1 CUSTOMER 폴더 목록 가져오기
			function(callback) {
				logger.info('[topics] Step#1 CUSTOMER 폴더 목록 가져오기');

				fs.readdir(sourcePath, function(err, customers) {
                    if (err) {
                        callback(err);
                    } else {
                        if (customers.length === 0){
                        	callback('ERR_NO_CUSTOMERS');
                        } else {
							callback(null, customers);
						}
                    }
                });
			}, 
			//Step#2 CUSTOMER 폴더 처리
			function(customers, callback) {		
				logger.info('[topics] Step#2 CUSTOMER 폴더 처리 ' + customers.length);

				async.eachSeries(customers, function(customer, callback) {
					async.waterfall([
						//Step#3 KEYWORD 목록 가져오기
						function(callback) {      
/*							if (customer != "kdic"){
								logger.debug('예금보험공사 외 스킵');
								callback('ERR_NO_KEYWORDS');
							}else{*/
								logger.info('[topics] Step#2-1 KEYWORD 목록 가져오기');
								// './topics/' + customer
								fs.readdir(sourcePath + customer, function(err, keywords) {
									if (err) {
										callback(err);
									} else {
//										if (keywords.length === 0 || (customer !== "kdic" && customer !== 'smc_dongwon') ){
	//if (keywords.length === 0 || (customer !== 'smc_dongwon' && customer !== 'ICT') ){
	if (keywords.length === 0 || ( customer !== 'ICT' && customer !=='smc_dongwon') ){
											callback('ERR_NO_KEYWORDS');
										} else {
											callback(null, keywords);
										}
									}
								}); //readdir (CUSTOMER)
//							}
						},
						//Step#4 KEYWORD 폴더 처리
						function(keywords, callback) {
							logger.info('[topics] Step#2-2 CUSTOMER-KEYWORD 관계 리스트 생성 ' + keywords.length);
							async.eachSeries(keywords, function(keyword, callback) {
								customerKeywordArr.push(
									{customer : customer, keyword : keyword}
								);
								callback(null);
							}, function(err) {
								if (err) {
									callback(err);
								} else {
									callback(null);
								}
							}); //eachSeries (KEYWORD)
						}
					], function(err) {
						if(err) {
							if(err === 'ERR_NO_KEYWORDS') {
								logger.warn('[topics] ' + err);
								callback(null);
							} else {
								callback(err);
							}							
						} else {
							callback(null);
						}
					}); //waterfall (CUSTOMER)
				}, function(err) {
					if(err) {
						callback(err);
					} else {
						callback(null);
					}
				}); //eachSeries (CUSTOMER)
			},
			//Step#3 MAX_TOPIC_EXPORTER_SIZE 만큼 topicExporter 생성 및 수행 시작
			function(callback){
				var customerKeywords = init(customerKeywordArr, MAX_TOPIC_EXPORTER_SIZE);
				if (customerKeywords.length != 0){
					logger.info('[topics] Step#3 topicExporter 생성 및 수행 시작 size : ' + MAX_TOPIC_EXPORTER_SIZE);
					async.eachOfLimit(customerKeywords, MAX_TOPIC_EXPORTER_SIZE, function(customerKeyword, ret_num, callback){
						//var topicsSaver = require('./topicsSaver_test');
						var topicsSaver = require('./topicsSaver');
						topicsSaver.process(sourcePath, customerKeyword, ret_num, callback); //save	
					}, function(err) {
						if(err) {
							callback(err);
						} else {
							logger.debug('[topicMaker] End TopicsSaver');
							callback(null);//callback(customerKeywords);
						}
					}); //eachOfLimit (customerKeywords);
				}else{
					callback(null);
				}
				/*cluster
				var cluster = require('cluster');
				if (cluster.isMaster){
					// 클러스터 워커 프로세스 포크
					for (var idx = 0; idx < MAX_TOPIC_EXPORTER_SIZE; idx++) {
						cluster.fork();
					}
					cluster.on('message', function(message) {//워커가 보내는 메세지 처리
						logger.debug('[topics] worker ' + worker.process.pid + ' message : ' + message);//마스터가 받는 메세지
					});
					cluster.on('exit', function(worker, code, signal) {
						logger.error('[topics] worker ' + worker.process.pid + ' died - code : ' + code + ', signal : ' + signal);
					});
				}else{
					var datas = customerKeywords[cluster.worker.id - 1];
					topicsSaver.process(datas,function(err){
						if (err){
							process.send("[topics] worker " + process.pid + " has err :" + err);
							callback(err);
						}else{
							process.send("[topics] worker " + process.pid + " finished a job normally." + datas.length);
							process.exist(0);
							callback(null);
						}
					});
				}
				//cluster*/
			}
		], function(err) {
			if(err) {
				if(err === 'ERR_NO_CUSTOMERS') {
					logger.warn('[topics] ' + err);
					callback(null);						
				} else {

					// slack alert
					var slackMessage = {
						color: 'danger',
						title: 'topics',
						value: '[topics] topics process failed : ' + err
					};
					
					slack.sendMessage(slackMessage, function(err) {
						if (err) {
							logger.error('[topics] ' + err);
							callback(err);
						} else {
							logger.info('[topics] Successfully push message to Slack');
							callback(err);
						}
					});
				} 
			} else {
				callback(null);
			}
		}); //waterfall
	}; //process
	
	var getFiles = function(path, files) {
		fs.readdirSync(path).forEach(function(file) {
			var subpath = path + '/' + file;
			if(fs.lstatSync(subpath).isDirectory()){
				getFiles(subpath, files);
			} else {
				files.push(path + '/' + file);
			}
		}); //readdirSync
	}; //getFiles

	var removeByKey =	function (array, params, num){
		logger.debug('[removeByKey]' + params + 'm' + num);  
		logger.debug(array.length);  
		for (var i=0; i<num; i++){
			array.some(function(item, index) {
			  //return (array[index][params.key] === params.value) ? !!(array.splice(index, 1)) : false;
//			logger.debug(array[index][params.key]);  
//			logger.debug(array[index][params.key] === params.value);  
				return (array[index][params.key] === params.value) ? !!(array.splice(index, 1)) : false;
			});
		};
		/*array.some(function(item, index) {
			return (array[index][params.key] === params.value) ? !!(array.splice(index, 1)) : false;
		});*/
//		logger.debug(array);
//		logger.debug(array.length);  
		logger.debug('[removeByKey]');  
	  return array;
	}//removeByKey

	/*
	화제어 처리모듈 갯수만큼 배열을 생성, 처리할 리스트들을 분배
	*/
	var init = function(customerKeywords, MAX_TOPIC_EXPORTER_SIZE){
		var tempArr = [];

		if (customerKeywords.length != 0){
			for (var i=0; i<MAX_TOPIC_EXPORTER_SIZE; i++ ){
				tempArr.push([]);
			}

			for (var i=0; i<customerKeywords.length; i++ ){
				var number = i % MAX_TOPIC_EXPORTER_SIZE;
				tempArr[number].push(customerKeywords[i]);
			}
		}
		return tempArr;
	}
	
	return {
		process_50 : process_50,
    	process: process
    };
    
})();

if (exports) {
	module.exports = topics;
}
