var bicAnalyzer = (function() {
  const fs = require('fs-extra');
  const logger = require('./logger');
  const slack = require('../lib/slack');
  const config = require('../lib/config');
  const async = require('async');
  const http = require('http');
  const querystring = require('querystring');
  const xml = require('xml');
  const xml2js = require('xml2js');
  const mariaDBDao = require('../dao/mariaDBDao');
  require('string-format-js');

  var TIMEOUT = config.BICA_CONFIG.ALWAYS.TIMEOUT;

  var RETRY_TIME = config.BICA_CONFIG.ALWAYS.RETRY_TIME; // async.retry
  var RETRY_INTERVAL = config.BICA_CONFIG.ALWAYS.RETRY_INTERVAL;
  var BUF_SIZE = -1;
  var NUMBER_OF_TMF = -1;

  var bicHost = {}; //var bicHost = null; //s[0];
  var TYPE = "";

  var setAnalyzerHost = function(doc, callback) {
    //원문에서 project_seq 가 [] 형태로 넘어온다!
    mariaDBDao.selectBICAInfo(doc, function(err, db_results) {
      if (err) {
        callback(err);
      } else {
        if (db_results == null || typeof db_results[0] === "undefined" || db_results.length === 0) {
          err = "NO_BICA_INFO"; //--- bica_ip 가 null 혹은 지정되지 않음
          callback(err);
        } else {

          var rows_clone = []; //결과값 복사
          db_results.forEach(function(v, i) {
            rows_clone.push(v);
          });
          for (var i = 0; i < db_results.length; i++) {
            var test_info = rows_clone[i].bic_info; //bica_ip,'|',bica_port,'|',bica_concept_id
            var filtered_rows = rows_clone.filter(function(row) {
              return row.bic_info == test_info
            });
            //						console.log('filtered_rows'); console.log(filtered_rows);
            var arr_project_seqs = [];
            for (var j = 0; j < filtered_rows.length; j++) {
              arr_project_seqs.push(filtered_rows[j].project_seq)
            }
            //						console.log('arr_project_seqs'); console.log(arr_project_seqs);
            db_results[i] = {
              "project_seq": arr_project_seqs,
              "bic_info": test_info
            }
          }
          logger.debug(db_results);
          //같은 bica 정보를 가진 project_seq 끼리 array 로 묶여진 array로 변경(중복된 데이터 있음);

          var rets = db_results.filter(
            (thing, index, self) => self.findIndex((t) => {
              return t.bic_info === thing.bic_info && t.project_seq.join() === thing.project_seq.join();
            }) === index
          );


          logger.debug(rets);
          callback(null, rets);
        }
      }
    });
  }

  var restApi_RE = function(bicHost, doc, callback) {

    //logger.debug('<doc> : ' +doc.doc_title + ' <customer> : ' +customer+' <keyword> : '+keyword )
    var sourceProcessingPath = './emotions_processing/';
    var sourcehang = './hang/';
    //var restApi = function(bicHost, title, content, callback){ //var restApi = function(title, content, callback){
    //http://[Interface Server IP]:[InterfaceServer Port]/request.is?data=[sentence]&conceptID=[최상위컨셉아이디]
    /*분석대상 문서를 API를 통해 전송하고 분석 결과를 바로 확인가능한 API이다.
    결과는 JSON으로 응답(Response)을 받으며 8.2 REST Like API 결과 에서 이에 대한 설명을 확인 가능하다*/
    var title = (typeof doc.doc_title === "undefined" ? "" : doc.doc_title).replace('/\r\n/gi', ' ');
    var content = (typeof doc.doc_content === "undefined" ? "" : doc.doc_content).replace('/\r\n/gi', ' ');


    var task = function(callback, result) {
      //logger.debug('<TITLE>' +title + ' <CONTENT>' +content );
      var postData = querystring.stringify({
        data: title + ' ' + content,
        conceptID: bicHost.concept_id == 0 ? '' : bicHost.concept_id
      });

      logger.info('host : '+bicHost.host +' post : '+bicHost.port);
      var byteLength = Buffer.byteLength(postData);
      var options = {
        host: '',
        port: '',
        path: '/request.is',
        method: 'POST',
        //path: '/request.is?data=' +content + "&conceptID=" + bicHost.concept_id, method:'GET',
        headers: {
          "Content-Type": "text/html",
          "Content-Length": byteLength
        }
      };

      var req = http.request(options, function(res) {
        res.setEncoding('utf8');
        var ret = "";
        res.on('data', function(chunk) {
          ret += chunk;
        });
        res.on('end', function() {
          if (res.statusCode === 200) {
            logger.debug('try');
            try { //
              ret = ret.replace(/\n/g, "")
                .replace(/\'/g, "\'")
                .replace(/\&/g, "\&")
                .replace(/\r/g, "")
                .replace(/\t/g, "")
                .replace(/\b/g, "")
                .replace(/\f/g, "")
                .replace(/\",\"index\"/g, ',\"index\"')
                .replace(/\"/g, '\"')
                .replace(/[\u0000-\u0019]+/g, "")
                .replace(/[\u001A]+/g, "");

              var result = JSON.parse(ret);
              var stautsCode = result.stauts.code;
              if (stautsCode === 200 || stautsCode === "200") { //서버 요청이 성공하면 200 반환
                callback(null, result.result); //json 반환
              } else {
                // ------result.stauts.code 607 : All TMF daemon busy //------result.stauts.code 900 : Unknown error occured
                logger.debug(result.stauts.msg);
                logger.debug('retry...');
                callback(result.stauts.msg, null);
              }
            } catch (e) {
              logger.error("[bicAnalyzer] json parse error : " + e);
              // logger.debug(ret);
              callback('ERR_JSON_PARSE_FAILED ' + doc._id);
            }
          } else {
            logger.debug("[bicAnalyzer] BICA status error : " + res.statusCode);
            callback(res.statusCode, null);
          }
        });
      });

      req.write(postData);
      // req.write(postData, 'utf8');
      req.setTimeout(TIMEOUT, function() {
        logger.debug('request timed out');
        // callback('request timed out');
      }); //ms
      req.on('error', function(err) {
        logger.error('[bicAnalyzer] problem with request : ' + err);
        logger.debug(doc.scdN);
        // if(err == 'Error: socket hang up') {
        //   //hang up json파일 hang파일로 옮기는 작업
        //
        //     fs.move(sourceProcessingPath + customer+'/'+ keyword +'/'+doc.scdN, sourcehang + keyword + '/'+doc.scdN);
        // }
        logger.error(err.stack);
        // ECONNRESET 이전의 연결 데이터를 잃어버렸고 클라이언트는 여전히 socket을 연결하고 있을 때 클라이언트가 서버에 연결을 시도했을 때 발생?
        // err = "SKIP_THIS_ERR";
        callback(err);
      });
      req.end();
    };

    async.retry({
      times: RETRY_TIME,
      interval: RETRY_INTERVAL
    }, task, function(err, result) {
      //logger.debug(err);			logger.debug(result);
      if (err) {
        /*				// slack alert
        				var slackMessage = {
        					color: 'danger',
        					title: 'bicAnalyzer',
        					value: '[bicAnalyzer](project_seq:'+bicHost.project_seq+') [host:'+bicHost.host+'] [port:' +bicHost.port+'] [concept_id:' +bicHost.concept_id+ '] bicAnalyzer restApi failed.' + err
        				};

        				slack.sendMessage(slackMessage, function(err) {
        					if (err) {
        						logger.error(err);
        					} else {
        						logger.info('[bicAnalyzer] Successfully push message to Slack');
        					}
        				});

        				//console.trace(err.message);
        				logger.error("[bicAnalyzer] " + err);
        				logger.error(err.indexOf('ERR_JSON_PARSE_FAILED') );


        				if (err == "SKIP_THIS_ERR" || err.indexOf('ERR_JSON_PARSE_FAILED') >=0 ){
        					callback(null, []);//callback(null, {});
        				}else{
        					callback(err);
        				}
        */
        callback(err);

      } else {
        //callback(null);
        callback(null, result); //-----callback was already called...?error
      }
    });

  };
  var isEmpty = function(value) {
    if (value == "" || value == null || value == undefined || (value != null && typeof value == "object" && !Object.keys(value).length)) {
      return true
    } else {
      return false
    }
  };

  var restApi = function(threadIdx, bicinfo, doc, analType, callback) {
    if(analType == 'P'){ //상시
      TIMEOUT = config.BICA_CONFIG.ALWAYS.TIMEOUT;
      RETRY_TIME = config.BICA_CONFIG.ALWAYS.RETRY_TIME; // async.retry
      RETRY_INTERVAL = config.BICA_CONFIG.ALWAYS.RETRY_INTERVAL;
      BUF_SIZE = config.BICA_CONFIG.ALWAYS.MAX_CONTENT_SIZE;
      NUMBER_OF_TMF = config.BICA_CONFIG.ALWAYS.NUM_OF_TMF;
      bicHost = {
        host: bicinfo.host,
        port: bicinfo.port
      }
    }else{ // 소급
      TIMEOUT = config.BICA_CONFIG.RETROACTIVE.TIMEOUT;
      RETRY_TIME = config.BICA_CONFIG.RETROACTIVE.RETRY_TIME; // async.retry
      RETRY_INTERVAL = config.BICA_CONFIG.RETROACTIVE.RETRY_INTERVAL;
      BUF_SIZE = config.BICA_CONFIG.RETROACTIVE.MAX_CONTENT_SIZE;
      NUMBER_OF_TMF = config.BICA_CONFIG.RETROACTIVE.NUM_OF_TMF;
      bicHost = {
        host: config.BICA_CONFIG.RETROACTIVE.BICA_IP,
        port: config.BICA_CONFIG.RETROACTIVE.BICA_PORT
      }
    }
    //http://[Interface Server IP]:[InterfaceServer Port]/request.is?data=[sentence]&conceptID=[최상위컨셉아이디]
    var attachs = "";
    if (typeof doc.attachs !== "undefined") {
      attachs = doc.attachs[0].content;
      doc.attachs.forEach(function(element, index, array) {
        // attachs += element.content.replace('/\\n/gi',' ') + ' ';
        attachs += element.content + ' ';
      });
    }
    var content = (typeof doc.doc_content === "undefined" ? "" : doc.doc_content);
    content = content.replace(/\n/g, "")
      .replace(/\'/g, "\'")
      .replace(/\&/g, "\&")
      .replace(/\r/g, "")
      .replace(/\t/g, "")
      .replace(/\b/g, "")
      .replace(/\f/g, "")
      .replace(/\"/g, '\"')
      .replace(/\b/g, "")
      .replace(/[\u0003]+/g, "")
      .replace(/[\u0005]+/g, "")
      .replace(/[\u007F]+/g, "")
      .replace(/[\u001a]+/g, "")
      .replace(/[\u001e]+/g, "")
      .replace(/[\u0000-\u0019]+/g, "")
      .replace(/[\u001A]+/g, "");

    //const bufferex = Buffer.alloc(1,attachs + ' ' + content);
    const buffer = Buffer.from(attachs + ' ' + content);
    // 예를 들어, BUF_SIZE가 256이고, 전체 buffer 길이가 1082인 경우
    // bufNumber, 즉 bufferArr의 길이는 1082/256의 몫을 올림한 5다.
    var bufNumber = buffer.length / BUF_SIZE;
    var bufferArr = [];
    for (var i = 0; i < Math.ceil(bufNumber); i++) {
      bufferArr.push({
        "callback": callback,
        "concept_id": bicinfo.concept_id,
        "doc": doc,
        "index": i,
        "cont": buffer.slice(i * BUF_SIZE, i * BUF_SIZE + BUF_SIZE),
        "threadIdx": threadIdx
      });
    }

    var taskMapLimit = function(threadIdx, callback) {
      async.mapLimit(bufferArr, NUMBER_OF_TMF, task, function(err, resolvedValues) {
        if (err) {
          callback(err);
        } else {
          logger.debug("["+threadIdx+"] All done!!!");
          logger.debug("["+threadIdx+"] ------------------------------------------------------------------------------");
          for (var i = 0; i < resolvedValues.length; i++) {
            logger.debug("["+threadIdx+"] " + resolvedValues[i].doc_id + "'s " + i + "번째 buffer 결과")
            for (var j = 0; j < resolvedValues[i].result.length; j++) {
              logger.debug("\t\tsentence.index: " + resolvedValues[i].result[j].sentence.index);
              logger.debug("\t\tsentence.string: " + resolvedValues[i].result[j].sentence.string);
            }
          }
          logger.debug("------------------------------------------------------------------------------");

          if (isEmpty(resolvedValues[0])) {
            callback("DOC_ID is null.", resolvedValues);
          } else {
            var _docId = '';
            var _results = [];
            for (var i = 0; i < resolvedValues.length; i++) {
              if (i === 0) {
                _docId = resolvedValues[i].doc_id;
              }
              for (var j = 0; j < resolvedValues[i].result.length; j++) {
                _results.push(resolvedValues[i].result[j]);
              }
            }

            callback(null,
              _results
            );
          }
        }

      });
    };


    /*var promisedTask = function(subBufferArr, callback) {
      logger.debug("promisedTask starts!!!");
      Promise.all(subBufferArr.map(task)).then(function(resolvedValues) {
        logger.debug("All done!!!");
        logger.debug("------------------------------------------------------------------------------");
        for(var i=0; i<resolvedValues.length; i++){
          logger.debug(resolvedValues[i].doc_id + "'s " + i +"번째 buffer 결과")
          for (var j = 0; j < resolvedValues[i].result.length; j++) {
            logger.debug("\t\tsentence.index: "+resolvedValues[i].result[j].sentence.index);
            logger.debug("\t\tsentence.string: "+resolvedValues[i].result[j].sentence.string);
          }
        }
        logger.debug("------------------------------------------------------------------------------");

        if (isEmpty(resolvedValues[0])) {
          callback("DOC_ID is null.", resolvedValues);
        } else {
          var _docId = '';
          var _results = [];
          for (var i = 0; i < resolvedValues.length; i++) {
            if (i === 0) {
              _docId = resolvedValues[i].doc_id;
            }
            for (var j = 0; j < resolvedValues[i].result.length; j++) {
              _results.push(resolvedValues[i].result[j]);
            }
          }

          callback(null, {
            doc_id: _docId,
            result: _results
          });
        }

      }).catch(function(rejectedValues) {
        callback(rejectedValues, null);
      });
    }*/

    async.retry({
      times: RETRY_TIME,
      interval: RETRY_INTERVAL
    }, async.apply(taskMapLimit, threadIdx), function(err, result) {
      //logger.debug(err);			logger.debug(result);
      if (err) {
        logger.error("["+threadIdx+"][bicAnalyzer] " + err);
        callback(config.LOG_MESSAGE.ERR_BICA_RESTAPI);

      } else {
        logger.debug("["+threadIdx+"][bicAnalyzer] ");
        callback(null, result); //-----callback was already called...?error
      }
    });
  };

  var task = function(obj, callback) {
    var threadIdx = obj.threadIdx;
    //return new Promise(function(resolved, rejected) {
    if (isEmpty(obj.cont)) {
      callback(null, {
        'doc_id': obj.doc._id,
        'result': []
      });
    } else {
      var formData = "data=" + obj.cont.toString() + "&conceptID=" + obj.concept_id;
      var byteLength = Buffer.byteLength(formData);

      logger.debug("["+threadIdx+"][bicAnalyzer] formData >>>>>>> " + formData);
      logger.debug("["+threadIdx+"][bicAnalyzer] Content-length >>>>>>> " + byteLength);

      var options = {
        host:  bicHost.host,
        port:  bicHost.port,
        path: '/request.is',
        method: 'POST',
        //path: '/request.is?data=' +content + "&conceptID=" + bicHost.concept_id, method:'GET',
        headers: {
          "User-Agent": "Wget/1.12 (linux-gnu)",
          "Content-Type": "application/x-www-form-urlencoded",
          "Content-Length": byteLength
          //'Transfer-Encoding': 'chunked'
        }
      };

      var req = new http.ClientRequest(options, function(res) {
        res.setEncoding('utf8');
        var ret = "";
        res.on('data', function(chunk) {
          ret += chunk;
        });
        res.on('end', function() {
          if (res.statusCode === 200) {
            try { //
              ret = ret.replace(/\n/g, "")
                .replace(/\'/g, "\'")
                .replace(/\&/g, "\&")
                .replace(/\r/g, "")
                .replace(/\t/g, "")
                .replace(/\b/g, "")
                .replace(/\f/g, "")
                .replace(/\",\"index\"/g, ',\"index\"')
                .replace(/\"/g, '\"')
                .replace(/[\u0000-\u0019]+/g, "")
                .replace(/[\u001A]+/g, "");

              var result = JSON.parse(ret);
              for (var idx = 0; idx < result.result.length; idx++) {
                result.result[idx].doc_datetime = obj.doc.doc_datetime;
                result.result[idx].sentence.original = obj.index + "_" + result.result[idx].sentence.index;
                result.result[idx].sentence.index += obj.index;
              }
              var statusCode = result.stauts.code;
              if (statusCode === 200 || statusCode === "200") {
                callback(null, {
                  'doc_id': obj.doc._id,
                  'result': result.result
                });
              } else {
                // ------result.stauts.code 607 : All TMF daemon busy //------result.stauts.code 900 : Unknown error occured
                logger.debug("["+threadIdx+"] "+result.stauts.msg);
                logger.debug("["+threadIdx+"][bicAnalyzer] retry...");
                //callback(result.stauts.msg, null);
                callback(result.stauts.msg);
              }
            } catch (e) {
              logger.error("["+threadIdx+"][bicAnalyzer] " + obj.doc._id + " json parse error : " + e);
              // logger.debug(ret);
              //callback('ERR_JSON_PARSE_FAILED ' + obj.doc.doc_id);
              callback('ERR_JSON_PARSE_FAILED ' + obj.doc._id);
            }
          } else {
            logger.debug("["+threadIdx+"][bicAnalyzer] BICA status error : " + res.statusCode);
            //callback(res.statusCode, null);
            callback(res.statusCode);
          }
        });
      });

      req.write(formData);
      // req.write(postData, 'utf8');
      req.setTimeout(TIMEOUT, function() {
        logger.debug("["+threadIdx+"] request timed out");
        // callback('request timed out');
        callback('request timed out');
      }); //ms
      req.on('error', function(err) {
        callback("["+threadIdx+"][bicAnalyzer] problem with request : " + err);

      });
      req.end();
    }
    //});
  };



  //분석설정 상태 조회
  //getAnalyzerSettingStatus
  //SELECT id, path_name FROM wn_tmf_tbl_concept; cid 조회 쿼리
  var getAnalyzerSettingStatus = function(bicHost, callback) {
    //<Request><RequestType>getAnalyzerSettingStatus</RequestType><RequestId>1</RequestId><Params><Param><analyzerName>hyundai_1</analyzerName></Param></Params></Request>
    var xmlObject = {
      Request: [{
          RequestType: 'getAnalyzerSettingStatus'
        },
        //{RequestId: (auto)},
        {
          Params: [{
            analyzerName: [bicHost.analyzerName]
          }]
        }
      ]
    };
    var xmlString = xml(xmlObject, {
      steam: true
    });

    var options = {
      host: bicHost.host,
      port: bicHost.port,
      //path: '/getAnalyzerSettingStatus.is',
      path: '/getAnalyzerSettingStatus.is?format=xml&query=' + xmlString,
      method: 'POST'
    };

    //		logger.debug("xmlString : " + xmlString);
    var postData = querystring.stringify({
      format: 'xml',
      query: xmlString
    });

    var ret = "";
    var req = http.request(options, function(res) {
      /*logger.debug("===========================");
      console.log('STATUS: ' + res.statusCode);
      console.log('HEADERS: ' + JSON.stringify(res.headers));
      console.log('STATUS: ' + JSON.stringify(res));*/
      res.setEncoding('utf8');
      res.on('data', function(chunk) {
        console.log('BODY: ' + chunk);
        ret += chunk;
      });
      res.on('end', function() {
        logger.debug('No more data in response');
        console.log(ret);
        //callback(null,ret);
      });
    });
    //req.write(postData);
    req.on('error', function(err) {
      logger.debug('problem with request : ');
      logger.debug(err);
    });
    req.end();
    //req.end(postData, 'utf8', function(){callback(null, ret);});
    /*req.end(postData, 'utf8', function(res){
    	console.log(res);
    	callback(null, res);
    });*/

  };


  //등록한 분석 설정 수행 : 해당 분석설정명을 시작 혹은 정지하는 API
  //runAnalyzer
  //http://[Interface Server IP]:[InterfaceServer Port]/runAnalyzer.is?format=xml&query=[query]
  var runAnalyzer = function(bicHost, status) {
    var xmlObject = {
      request: [{
          RequestType: 'runAnalyzer'
        },
        //{RequestId: (auto)},
        {
          params: [{
              analyzerName: [bicHost.analyzerName]
            },
            {
              setting: [status]
            } //status : start / stop
          ]
        }
      ]
    };
    var xmlString = xml(xmlObject, {
      declaration: true,
      indent: '\t'
    });
  };


  //매칭 문서 조회(문서 기준)
  //getMatchedDocumentByConcept
  //http://[Interface Server IP]:[InterfaceServer Port]/getMatchedDocumentByConcept.is?format=xml&query=[query]
  var getMatchedDocumentByConcept = function(bicHost, title, content, start_dt, end_dt) {
    var xmlObject = {
      request: [{
          RequestType: 'getMatchedDocumentByConcept'
        },
        //{RequestId: (auto)},
        {
          params: [
            //{param: [{_attr: {name: 'collection_id'}}, 'sample_terms']},
            {
              param: [{
                _attr: {
                  name: 'cid'
                }
              }, bicHost.cid]
            }, //number
            {
              param: [{
                _attr: {
                  name: 'analyzerName'
                }
              }, bicHost.analyzerName]
            },
            {
              param: [{
                _attr: {
                  name: 'isWholePeriod'
                }
              }, 'N']
            },
            {
              param: [{
                _attr: {
                  name: 'startTime'
                }
              }, start_dt]
            },
            {
              param: [{
                _attr: {
                  name: 'endTime'
                }
              }, end_dt]
            },
            {
              param: [{
                _attr: {
                  name: 'sort'
                }
              }, 'N']
            }, //0기본/1오름차순/2내림차순
            {
              param: [{
                _attr: {
                  name: 'rowCnt'
                }
              }, 'N']
            }, //10/30/50
            {
              param: [{
                _attr: {
                  name: 'pageNum'
                }
              }, 'N']
            }, //num
          ]
        }
      ]
    };
    var xmlString = xml(xmlObject, {
      declaration: true,
      indent: '\t'
    });
  };




  return {
    getAnalyzerSettingStatus: getAnalyzerSettingStatus,
    runAnalyzer: runAnalyzer,
    restApi: restApi,
    setAnalyzerHost: setAnalyzerHost
  };
})();

if (exports) {
  module.exports = bicAnalyzer;
}
