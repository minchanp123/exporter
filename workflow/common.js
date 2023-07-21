/*
get docType, docs, id then analyzer start
*/
var logger = require('../lib/logger');
var async = require('async');
var esDao = require('../dao/esDao');
var mariaDBDao = require('../dao/mariaDBDao');
var crawlDocTopicMaker = require('../routes/crawlDocTopicMakerParallel');
var crawlDocEmotionMaker = require('../routes/crawlDocEmotionMaker');

var dmapAnalyzer = (function(doc_type, docs){
	
	var process = function(doc_type, docs, callback){
		 if(doc_type ==="D"){//원문일 경우
    		 logger.debug('=============================================');
    		 logger.debug('Step #3-5-1(D). 각 Object Array 분석 시작');
    		 logger.debug('=============================================');
    		 async.parallelLimit([
                 function(callback) {
                	 async.waterfall([
    	                  function(callback){
    	                	  logger.debug('Step #3-5-1(D)-1. TopicAnalzyer');
    	                	  
    	                	  //async.eachLimit();
	                	 		crawlDocTopicMaker.process(docs, function(err, resultCnt, result){
           	                		 if (err){
           	                			 callback(err);
           	                		 }else{
           	                			 //logger.debug(result);
           	                			 logger.debug('get topic results = ' + resultCnt);
           	                			 //console.log(result);
           	                			 callback(null, result);
           	                		 }
           	                	 });
    	                  },
    	                  function(result, callback){
							  if (result.length > 0){
								  //result 수가 500보다 작은 경우 처리
								  logger.debug('Step #3-5-1(D)-2. TopicAnalzyer : save2ES');
								  //callback(null);
									esDao.save2ES(result, 'topics', function(err){
										 if (err){
											 callback(err);
										 }else{
											 callback(null);//callback(null,doc_type, docs);
										 }
									} );
							  }else{
								  callback(null);
							  }
    	                  }
	                  ]
                	 ,function(err){
                		 if(err){
                			 callback(err);
                		 }else{
                			 callback(null, 'topics_finish');
                		 }
                	 });
                 }/*,
                 function(callback) {
                	 async.waterfall([
						 function(callback){
    	                	  logger.debug('Step #3-5-1(D)-1. get BICA host');
        	                	 mariaDBDao.selectBICAInfo(function(err, bicaHosts){
        	                		 if (err){
        	                			 callback(err);
        	                		 }else{
										 logger.debug('bicaHosts : ' + bicaHosts.length);
        	                			 callback(null, bicaHosts);
        	                		 }
        	                	 });
    	                  },
    	                  function(bicaHosts, callback){
    	                	  logger.debug('Step #3-5-1(D)-2. BICAnalzyer');
        	                	 crawlDocEmotionMaker.process(docs, bicaHosts, function(err, result){
        	                		 if (err){
        	                			 callback(err);
        	                		 }else{
        	                			 logger.debug('get bica results = ' + result.length);
										 //logger.debug(result);
        	                			 callback(null, result);
        	                		 }
        	                	 });
    	                  },
    	                  function(result, callback){
    	                	  logger.debug('Step #3-5-1(D)-3. BICAnalzyer : save2ES');
    	                	  //callback(null);
							  if (result.length == 0){
								  callback("no_bica_results");
							  }else{
								  //result 수가 500보다 작은 경우 처리
								   esDao.save2ES(result, 'emotions', function(err){
									 if (err){
										 callback(err);
									 }else{
										 callback(null);//callback(null,doc_type, docs);
									 }
								 } );
							  }
    	                  }*/
	                  ]
                	 ,function(err){
                		 if(err){
							 if (err == "no_bica_results"){
								 logger.debug(err);
								 callback(null, 'emotions_finish');
							 }else{
	                 			 callback(err);
							 }
                		 }else{
                			 callback(null, 'emotions_finish');
                		 }
                	 });
                 }
             ],
             //limit
            1, //2,
             // optional callback
             function(err, results) {
                 // the results array will equal ['one','two'] even though
				 logger.debug('Step #3-5-2(D). 완료');
        		 if (err){
        			 callback(err);
        		 }else{
        			 logger.debug(results);
        			 if (results[0] === 'topics_finish' )//if (results[0] === 'topics_finish' && results[1]==='emotions_finish')
        				 callback(null);
        			 else{
        				 callback("unknown error");
        			 }
        		 }
             });
    	 }else if(doc_type==="C"){//댓글일 경우
    		 logger.debug('Step #3-5-1(C). 각 댓글 Object Array 분석 시작');
    		 callback(null);
    		//TODO saveESObject 여기에 결과들을 담아 Save2ES 호출?
    	 }
		 
	};
	
	return {
    	process: process
    };
    
})();

if (exports) {
	module.exports = dmapAnalyzer;
}