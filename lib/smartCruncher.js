var smartCruncher = (function(){
	const logger = require('./logger');
	const slack = require('../lib/slack');
	const config = require('../lib/config');
	const async = require('async');
	const http = require('http');
	const querystring = require('querystring');

	const RETRY_TIME = config.SMARTCRUNCHER_CONFIG.RETRY_TIME;
	const RETRY_INTERVAL = config.SMARTCRUNCHER_CONFIG.RETRY_INTERVAL;
		
	var smartCruncherHost = {
		host : config.SMARTCRUNCHER_CONFIG.HOST,
		port : config.SMARTCRUNCHER_CONFIG.PORT,//9098,
		authinit : config.SMARTCRUNCHER_CONFIG.AUTHINIT,
		service : config.SMARTCRUNCHER_CONFIG.SERVICE,
		num : '00'
	};

	var setSmartCruncherNumber = function (ret_num, thread_num, MAX_SMART_CRUNCHER_SIZE){
		//smartCruncherHost.port = config.SMARTCRUNCHER_CONFIG.PORT[ret_num];

		var number = thread_num % MAX_SMART_CRUNCHER_SIZE;
		//01~09 까지 사용 가능
		if ( number < 10){
			number = '0' + number;
		}else{
			number = '' + number;
		}
		if (number === '00'){
			number = (MAX_SMART_CRUNCHER_SIZE < 10 ? '0'+MAX_SMART_CRUNCHER_SIZE: ''+MAX_SMART_CRUNCHER_SIZE);
		}
		smartCruncherHost.num = number;
	}

	/*
	스마트크런처 호출 url 지정 emotions 에서 지정한 MAX_SMART_CRUNCHER_SIZE 만큼
	
	var setSmartCruncherNumber = function (thread_num, MAX_SMART_CRUNCHER_SIZE){
		var number = thread_num % MAX_SMART_CRUNCHER_SIZE;
		//01~09 까지 사용 가능
		if ( number < 10){
			number = '0' + number;
		}else{
			number = '' + number;
		}
		if (number === '00'){
			number = (MAX_SMART_CRUNCHER_SIZE < 10 ? '0'+MAX_SMART_CRUNCHER_SIZE: ''+MAX_SMART_CRUNCHER_SIZE);
		}
		smartCruncherHost.num = number;
	}*/
		
	var restApi = function(sentence, callback){
		sentence = (typeof sentence === "undefined" ? "":sentence.replace(/\!/g,''));//.replace('/\r\n/gi',' ');
		
		var task = function(callback,result){
			var postData = querystring.stringify({authinit:smartCruncherHost.authinit, sentence : sentence});
			var byteLength = Buffer.byteLength(postData);
			
			var options = {
	                                host : smartCruncherHost.host,
					port : smartCruncherHost.port,
					path: '/' + smartCruncherHost.service+smartCruncherHost.num+'?' + postData, 
					method: 'GET',
					headers : {
						"Content-Type":"application/json; charset=utf-8"
//						, "Content-Length" : byteLength
					}
			};
//			console.log(options);
			var req = http.request(options, function(res) {
//				  console.log('STATUS: ' + res.statusCode);
//				  console.log('HEADERS: ' + JSON.stringify(res.headers));
				  res.setEncoding('utf8');
				  var ret = "";
				  res.on('data', function (chunk) {
				    ret += chunk;
				  });
				  res.on('end', function(){
					  /* smartcruncher return 값들
					  START|00|PC001_MC01_123456|POSITIVE|END
						START|00|PC001_MC01_123456|NEGATIVE|END
						 START|00|PC001_MC01_123456|ETC|END
						START|91|||END*/
					  var resArr = ret.split('|');
					  if (resArr[1] == "00"){	//처리 성공
						  if (resArr[3] == "POSITIVE"){
							  callback(null, "긍정");
						  }else if (resArr[3] == "NEGATIVE"){
							  callback(null, "부정");
  						  }else if (resArr[3] == "ETC"){
							  callback(null, "중립");	//POSITIVE / NEGATIVE / ETC
						  }else{
							  callback("ERR_SMART_CRUNCHER",null);
						  }
					  }else{
						  var err = resArr[1];
						  if(err == "91"){//parameter 누락
							  logger.error('[smartCruncher]parameter 누락');
						  }else if(err == "92"){	//timeout
							  logger.error('[smartCruncher]timeout');
						  }else if(err == "93"){	//db 에러
							  logger.error('[smartCruncher]db error');
						  }else{
							  logger.error("[smartCruncher]("+smartCruncherHost.num+") status error : " + ret);
						  }
						  callback(err, null);
					  }
				  });
			});

			req.setTimeout(60000, function(err){
				logger.warn('warn : [smartCruncher]('+smartCruncherHost.num+') timed out 60000');
				callback(err);
			});
			
//			req.write(postData);		//req.write(postData, 'utf8');
			req.on('error', function(err){
				logger.error('error : [smartCruncher]('+smartCruncherHost.num+') problem with request : ');
				logger.error(err.stack);
				//ECONNRESET 이전의 연결 데이터를 잃어버렸고 클라이언트는 여전히 socket을 연결하고 있을 때 클라이언트가 서버에 연걸을 시도했을 때 발생?
				callback(err);
			});
			req.end();
		};
		
		async.retry({times:RETRY_TIME, interval:RETRY_INTERVAL}
		, task
		, function(err, result){
			if (err){
				/*// slack alert
				var slackMessage = {
					color: 'danger',
					title: 'smartCruncher',
					value: '[smartCruncher: '+smartCruncherHost.host+'] [port:' +smartCruncherHost.port+ ']'+' [num:' +smartCruncherHost.num+ '] smartCruncher failed. ' + err
				};
				
				slack.sendMessage(slackMessage, function(err) {
					if (err) {
						logger.error(err);
					} else {
						logger.info('[smartCruncher] Successfully push message to Slack');
					}
				});
				*/
				logger.error("[smartCruncher] " + err.message);
				callback(err);
				
			}else{
				//callback(null);
				callback(null, result);
			}
		});
	};

	
	return {
		restApi: restApi,
		setSmartCruncherNumber : setSmartCruncherNumber
	};
})();

if (exports) {
	module.exports = smartCruncher;
}
