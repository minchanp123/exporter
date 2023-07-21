const logger = require('../lib/logger');
const config = require('../lib/config');
const async = require('async');

const bicAnalyzer = require('../lib/bicAnalyzer');
const MAX_SMART_CRUNCHER_SIZE = 9;

//var crawlDocEmotionMaker = (function(docs){
var crawlDocEmotionMaker = (function(docs, ret_num) {
  //var process = function(docs, callback){
  var process = function(docs, ret_num, processCallback) {
    var jsonMaker = require('../lib/50jsonMaker');
    var esDao = require('../dao/50esDao');
    var callback_cnt = 0;

    logger.debug('[emotionMaker] Start Emotion Maker ' + docs.length);
    var returnArray = [];

    async.eachSeries(docs, function(doc, eachSeriesCallback) {
      // async.eachOfLimit(docs, 3, function(doc, index,callback) {
      async.waterfall([
        function(toStep2) {
          logger.debug('[emotionMaker]Step#1 get bicaHost');
          // 프로젝트 seq에 따른 bica정보 리스트를 가져옴
          bicAnalyzer.setAnalyzerHost(doc, function(err, bicInfos) {
            if (err) {
              toStep2(err);
            } else {
              var rets = [];
              for (var idx = 0; idx < bicInfos.length; idx++) {
                var this_infos = bicInfos[idx].bic_info.split('|');
                rets.push({
                  'project_seq': bicInfos[idx].project_seq,
                  'host': this_infos[0],
                  'port': this_infos[1],
                  'concept_id': this_infos[2]
                });
              }
              toStep2(null, rets);
            }
          });
        },
        function(bicInfos, toStep3) {
          // 복수개의 bica에 각각 요청
          //복수개의 BICA 에서 전달받은 결과를 CONCAT 하는 배열
          var result = [];
          /*
          result = [
          	{
          		bicInfo : {}
          		, emotions : []
          	},
          	{
          		bicInfo : {}
          		, emotions : []
          	}
          ]
          */
          async.each(bicInfos, function(bicInfo, toStep3) {
            logger.debug('[emotionMaker]Step#2 BICA CONNECT(' + bicInfo.project_seq + ' / ' + bicInfo.host + ':' + bicInfo.port + '/conceptId=' + bicInfo.concept_id + ') : doc_id (' + doc._id + ')');
            var starttime = new Date();
            // logger.debug('(#BICA#) start : '  + starttime); //3KB -> 5min //11KB-> ?
            bicAnalyzer.restApi(ret_num, bicInfo, doc,'P', function(err, bica_ret) { //bica_ret : array
              if (err) {
                logger.debug('[emotionMaker]Error occurs while analyzing document : doc_id (' + doc._id + ')');
                if (err == "SKIP_THIS_ERR") {
                  result = result.concat({
                    bicInfo: bicInfo,
                    emotions: bica_ret
                  });
                  // result = result.concat(bica_ret);
                  toStep3(null);
                } else {
                  toStep3(err);
                }
              } else {
                var endtime = new Date();
                // logger.debug('(#BICA#) end : '  + endtime);
                logger.debug('[emotionMaker]BICA elapsed time [' + (endtime - starttime) / 1000 + ' second] : doc_id (' + doc._id + ')');
                // logger.debug(bica_ret);
                if (bica_ret.length === 0) {
                  logger.debug('[emotionMaker]NO_ANALYZED_RESULT : doc_id (' + doc._id + ')');
                  toStep3(null);
                } else {
                  result = result.concat({
                    bicInfo: bicInfo,
                    emotions: bica_ret
                  });
                  toStep3(null);
                }
              }
            });
          }, function(err) {
            if (err) {
              toStep3(err);
            } else {
              toStep3(null, result);
            }
          }); //async.each
        },
        function(results, toStep4) {
          logger.debug('[emotionMaker] Step#3 get emotion_type [size:' + results.length + '] : doc_id (' + doc._id + ')');
          if (results.length == 0) {
            err = 'NO_ANALYZED_RESULT';
            toStep4(err);
          } else {
            //logger.debug(results);
            async.eachSeries(results, function(result, cbOfStep3) {
              logger.debug('[emotionMaker] Step#3 get emotion_type [size:' + result.emotions.length + '] : doc_id (' + doc._id + ')');
              var rets = result.emotions;
              var retResult = [];
              async.waterfall([
                function(step3_1) {
                  logger.debug('[emotionMaker] Step#3-1 start SmartCruncher(' + MAX_SMART_CRUNCHER_SIZE + ')');
                  async.forEachOfLimit(rets, MAX_SMART_CRUNCHER_SIZE, function(ret, ret_num2, step3_1) {
                    var smartCruncher = require('../lib/smartCruncher');
                    smartCruncher.setSmartCruncherNumber(ret_num, ret_num2, MAX_SMART_CRUNCHER_SIZE); // smartCruncher.setSmartCruncherNumber(ret_num, MAX_SMART_CRUNCHER_SIZE);
                    var sentence = ret.matched_text.string;
                    var starttime = new Date();

                    ret.emotion_type = "중립";
                    retResult.push(ret);
                    step3_1(null);
                  }, function(err) {
                    if (err) {
                      logger.error('[emotionMaker] forEachOfLimit > callback ' + err);
                      if (err === "SKIP_THIS_ERR") {
                        step3_1(null);
                      } else {
                        step3_1(err);
                      }
                    } else {
                      step3_1(null);
                    }
                  }); //async.forEachOfLimit
                },
                function(step3_2) {
                  logger.debug('[emotionMaker] End getAnalzyed result [size:' + retResult.length + ']');
                  result.emotions = retResult;
                  step3_2(null);
                  //callback(null, doc, retResult);
                }
              ], cbOfStep3);
            }, function(err) {
              if (err) {
                toStep4(err);
              } else {
                logger.debug('[emotionMaker]  End getAnalzyed result ');
                toStep4(null, doc, results);
              }
            });

          }
        },
        function(doc, retResult, toStep5) {
          logger.debug('[emotionMaker]Step#4 get getEmotionJson');
          var starttime = new Date();
          jsonMaker.getEmotionJson(doc, retResult, function(err, returnJsons) {
            if (err) {
              logger.error(err);
              toStep5(err);
            } else {
              var endtime = new Date();
              logger.debug('[emotionMaker]getEmotionJson elapsed time [' + (endtime - starttime) / 1000 + ' second]');
              logger.debug(returnJsons);
              jsonMaker.makeJsonOutput(returnJsons, config.DOC_TYPE.TYPE_EMOTIONS, function(err, ret) {
                if (err) {
                  toStep5(err);
                } else {
                  logger.debug('[topicMaker] CallbackCnt=' + (++callback_cnt));
                  returnArray.push(ret);
                  toStep5(null);
                }
              }); // save2ES
            }
          }); //getEmotionJson
        }
      ], function(err) {
        if (err) {
          if (err === "NO_ANALYZED_RESULT") {
            logger.warn('[emotionMaker] ' + err + " : no analyzed result");
            //++callback_cnt;	//BICA 분석결과 없어도 카운팅??
            eachSeriesCallback(null);
          } else if (err === "NO_BICA_INFO") { // 감정분석 실행 X
            logger.debug('[emotionMaker] ' + err + " : get next documents...");
            eachSeriesCallback(null);
          } else {
            eachSeriesCallback(err);
          }
        } else {
          eachSeriesCallback(null);
        }
      });
      // END of waterfall
    }, function(err) {
      if (err) {
        processCallback(err);
      } else {
        logger.debug('[emotionMaker] End emotionMaker ');
        processCallback(null, returnArray);
      }
    });
    // end of eachSeries
  };
  return {
    process: process
  };
})();

if (exports) {
  module.exports = crawlDocEmotionMaker;
}
