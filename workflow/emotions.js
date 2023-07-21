var async = require('async');
var fs = require('fs-extra');
var logger = require('../lib/logger');
var slack = require('../lib/slack');
var config = require('../lib/config');



var emotions = (function() {
  //------emotions 폴더 내의 파일에서 감성분석 탈지 안탈지를 결정
  var sourceProcessingPath = './emotions_processing/';
  var sourceEmotionsDonePath = './emotions_done/';

  var process_50 = function(sourcePath, callback) {
    var MAX_EMOTION_EXPORTER_SIZE = config.THREAD_CNT.THREAD_EMOTION; //1;	//현재 감정분석 모듈 문제로 1개만 수
    var callback_cnt = 0;
    var customerKeywordArr = [];

    async.waterfall([
      //Step#1 CUSTOMER 폴더 목록 가져오기
      function(callback) {
        logger.info('[emotions] Step#1 CUSTOMER 폴더 목록 가져오기');

        fs.readdir(sourcePath, function(err, customers) {
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
      function(customers, callback) {
        logger.info('[emotions] Step#2 CUSTOMER 폴더 처리 ' + customers.length);

        async.eachSeries(customers, function(customer, callback) {
          async.waterfall([

            //Step#3 KEYWORD 목록 가져오기
            function(step2callback) {
              logger.debug('[emotions] Step#3 KEYWORD 목록 가져오기');
              // './emotions/' + customer
              fs.readdir(sourcePath + customer, function(err, keywords) {
                if (err) {
                  step2callback(err);
                } else {
                  if (keywords.length === 0) {
                    step2callback('ERR_NO_KEYWORDS');
                  } else {
                    step2callback(null, keywords);
                  }
                }
              }); //readdir (CUSTOMER)
            },
            //Step#4 KEYWORD 폴더 처리
            function(keywords, step2callback) {
              logger.info('[emotions] Step#2-2 CUSTOMER-KEYWORD 관계 리스트 생성 ' + keywords.length);
              async.eachSeries(keywords, function(keyword, callback) {
                customerKeywordArr.push({
                  customer: customer,
                  keyword: keyword
                });

                logger.info('[emotions] 빈 폴더 삭제 ' + keywords.length);

                fs.ensureDir(sourcePath + customer + '/' + keyword, (err) =>{

                  if(err){
                    callback(err);
                  }else{
                    fs.readdir(sourcePath + customer + '/' + keyword, function(err1, scdFolders)  {

                      for(var i = 0; i<scdFolders.length; i++) {
                          //if(scdFolders[i].length === 0)
                        fs.rmdir(sourcePath + customer + '/' + keyword +'/'+scdFolders[i], (err) =>{
                          if(err){
                          }
                        });
                        //logger.info(keyword+" : "+scdFolders[i]);
                      }
                    });

                  }
                });

                callback(null);
              }, function(err) {
                if (err) {
                  step2callback(err);
                } else {
                  step2callback(null);
                }
              }); //eachSeries (KEYWORD)
            }
          ], function(err) {
            if (err) {
              if (err === 'ERR_NO_KEYWORDS') {
                logger.warn('[emotions] ' + err);
                callback(null);
              } else {
                callback(err);
              }
            } else {
              callback(null);
            }
          }); //waterfall (CUSTOMER)
        }, function(err) {
          if (err) {
            callback(err);
          } else {
            callback(null);
          }
        }); //eachSeries (CUSTOMER)
      },
      //Step#3 MAX_EMOTION_EXPORTER_SIZE 만큼 emotionExporter 생성 및 수행 시작
      function(callback) {
        var customerKeywords = init(customerKeywordArr, MAX_EMOTION_EXPORTER_SIZE);
        if (customerKeywords.length != 0) {
          if (MAX_EMOTION_EXPORTER_SIZE <= config.SMARTCRUNCHER_CONFIG.PORT.length) {
            logger.info('[emotions] Step#3 emotionExporter 생성 및 수행 시작 size : ' + MAX_EMOTION_EXPORTER_SIZE);
            async.eachOfLimit(customerKeywords, MAX_EMOTION_EXPORTER_SIZE, function(customerKeyword, ret_num, callback) {
              var emotionsSaver = require('./50emotionsSaver');
              emotionsSaver.process(sourcePath, customerKeyword, ret_num, callback); //save
            }, function(err) {
              callback(err); //callback(customerKeywords);
            }); //eachOfLimit (customerKeywords);
          } else {
            logger.error('[emotions] MAX_EMOTION_EXPORTER_SIZE > config.SMARTCRUNCHER_CONFIG.PORT.length');
            logger.error('[emotions] config 확인 필요');
            callback('ERR_SMART_CRUNCHER_PORT_COUNT');
          }
        } else {
          callback(null);
        }
      }
    ], function(err) {
      if (err) {
        if (err === 'ERR_NO_CUSTOMERS') {
          logger.warn('[emotions] ' + err);
          callback(null);
        } else if (err === 'ERR_NO_DOCUMENTS') {
          logger.warn('[emotions] ' + err);
          callback(null);
        } else {
          callback(err);
        }
      } else {
        callback(null);
      }
    }); //waterfall
  }; //process_50


  var process = function(sourcePath, callback) {
    var MAX_EMOTION_EXPORTER_SIZE = 5; //현재 감정분석 모듈 문제로 1개만 수
    var callback_cnt = 0;
    var customerKeywordArr = [];

    async.waterfall([

      //Step#1 CUSTOMER 폴더 목록 가져오기
      function(callback) {
        logger.info('[emotions] Step#1 CUSTOMER 폴더 목록 가져오기');

        fs.readdir(sourcePath, function(err, customers) {
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
      function(customers, callback) {
        logger.info('[emotions] Step#2 CUSTOMER 폴더 처리 ' + customers.length);

        async.eachSeries(customers, function(customer, callback) {
          async.waterfall([

            //Step#3 KEYWORD 목록 가져오기
            function(callback) {
              logger.debug('[emotions] Step#3 KEYWORD 목록 가져오기');
              // './emotions/' + customer
              fs.readdir(sourcePath + customer, function(err, keywords) {
                if (err) {
                  callback(err);
                } else {
                  if (keywords.length === 0 || customer !== 'nongshim') {
                    callback('ERR_NO_KEYWORDS');
                  } else {
                    callback(null, keywords);
                  }
                }
              }); //readdir (CUSTOMER)
            },
            //Step#4 KEYWORD 폴더 처리
            function(keywords, callback) {
              logger.info('[emotions] Step#2-2 CUSTOMER-KEYWORD 관계 리스트 생성 ' + keywords.length);
              async.eachSeries(keywords, function(keyword, callback) {
                customerKeywordArr.push({
                  customer: customer,
                  keyword: keyword
                });
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
            if (err) {
              if (err === 'ERR_NO_KEYWORDS') {
                logger.warn('[emotions] ' + err);
                callback(null);
              } else {
                callback(err);
              }
            } else {
              callback(null);
            }
          }); //waterfall (CUSTOMER)
        }, function(err) {
          if (err) {
            callback(err);
          } else {
            callback(null);
          }
        }); //eachSeries (CUSTOMER)
      },
      //Step#3 MAX_EMOTION_EXPORTER_SIZE 만큼 emotionExporter 생성 및 수행 시작
      function(callback) {
        var customerKeywords = init(customerKeywordArr, MAX_EMOTION_EXPORTER_SIZE);
        if (customerKeywords.length != 0) {
          logger.info('[emotions] Step#3 emotionExporter 생성 및 수행 시작 size : ' + MAX_EMOTION_EXPORTER_SIZE);
          async.eachOfLimit(customerKeywords, MAX_EMOTION_EXPORTER_SIZE, function(customerKeyword, ret_num, callback) {
            var emotionsSaver = require('./emotionsSaver');
            emotionsSaver.process(sourcePath, customerKeyword, ret_num, callback); //save
          }, function(err) {
            var esDao = require('../dao/esDao');
            if (err) {
              esDao.refreshIndexes('emotions', function(err2) {
                logger.debug('[emotions] refreshIndexes');
                if (err2) {
                  callback(err + err2);
                } else {
                  callback(err);
                }
              });
            } else {
              logger.debug('[emotions] End emotionsSaver');
              esDao.refreshIndexes('emotions', function(err2) {
                logger.debug('[emotions] refreshIndexes');
                if (err2) {
                  callback(err + err2);
                } else {
                  callback(err);
                }
              });
              callback(null); //callback(customerKeywords);
            }
          }); //eachOfLimit (customerKeywords);
        } else {
          callback(null);
        }
      }
    ], function(err) {
      if (err) {
        if (err === 'ERR_NO_CUSTOMERS') {
          logger.warn('[emotions] ' + err);
          callback(null);
        } else if (err === 'ERR_NO_DOCUMENTS') {
          logger.warn('[emotions] ' + err);
          callback(null);
        } else {
          callback(err);
        }
      } else {
        callback(null);
      }
    }); //waterfall
  }; //process

  var getFiles = function(path, files) {
    fs.readdirSync(path).forEach(function(file) {
      var subpath = path + '/' + file;
      if (fs.lstatSync(subpath).isDirectory()) {
        getFiles(subpath, files);
      } else {
        files.push(path + '/' + file);
      }
    }); //readdirSync
  }; //getFiles

  var removeByKey = function(array, params, num) {
    logger.debug('[removeByKey]' + params + 'm' + num);
    //		logger.debug(array.length);
    for (var i = 0; i < num; i++) {
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
    logger.debug(array.length);
    logger.debug('[removeByKey]');
    return array;
  } //removeByKey

  /*
  화제어 처리모듈 갯수만큼 배열을 생성, 처리할 리스트들을 분배
  */
  var init = function(customerKeywords, MAX_EMOTION_EXPORTER_SIZE) {
    var tempArr = [];

    if (customerKeywords.length != 0) {
      for (var i = 0; i < MAX_EMOTION_EXPORTER_SIZE; i++) {
        tempArr.push([]);
      }

      for (var i = 0; i < customerKeywords.length; i++) {
        var number = i % MAX_EMOTION_EXPORTER_SIZE;
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
  module.exports = emotions;
}
