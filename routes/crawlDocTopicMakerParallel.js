const logger = require('../lib/logger');
const config = require('../lib/config');
const async = require('async');
const PythonShell = require('python-shell');
const sleep = require('sleep');
const teaAnalyzer = require('../lib/teaAnalyzer');
var teaSocket = teaAnalyzer.teaSocket;

var crawlDocTopicMakerParallel = (function(){
	/** get Toics using TEA and make topics type json format
	* @ param : docs
	* @ param : relatedWordsY
	* @ return : topics type jsons array
	**/
	// var process = function(docs, callback) {
	var process = function(docs, relatedWordsY, callback) {
		var jsonMaker = require('../lib/50jsonMaker');
		var doc_cnt = 0;
		var callback_cnt = 0;

		var term_yn = 'y';
		var verb_yn = 'y';

		logger.debug('[topicMaker] Start Topic Maker ' + docs.length + '::'+relatedWordsY);
		var returnArray = [];

		async.eachSeries(docs, function(doc, callback) {
			logger.info('[topicMaker] Step#1 문서 화제어 처리');
			logger.debug('[topicMaker] docCnt=' + (++doc_cnt));
			async.waterfall([
				// Step#1-1 TEA 화제어 추출
				function(callback) {
					logger.info('[topicMaker] Step#1-1 TEA 화제어 추출');
					
					if (typeof doc.doc_title === 'undefined') { 
						doc.doc_title = '';
					}

					teaSocket(doc.doc_title, doc.doc_content, term_yn, verb_yn, doc.customer_id, function(err, terms) {
						if(err) {
							callback(err);
						} else {				
							//logger.debug('[topicMaker] Terms=' + terms);
							logger.debug(terms);
							doc.topics = terms.join('').split('');
							callback(null, terms);
						}
					});
				},
				// Step#1-2 명사형 화제어 부가정보 설정
				function(terms, callback) {
					logger.info('[topicMaker] Step#1-2 명사형 화제어 부가정보 설정');
					var retTerms = [];

					async.each(terms, function(term, callback) {
						var topicObj = term;
						if (topicObj.topic == '' || topicObj.topic =='대화동빌라' || topicObj.topic=='신축빌라방' || topicObj.topic=='월드에이스탑' || topicObj.topic=='한국어교원'){
							callback(null);	
						}else if (topicObj.topic_class === 'NN') { //명사
							// 연관어 분석 진행할 경우
							if (relatedWordsY){
								async.waterfall([
									// Step#1-2-1 연관어 추출 (문서:화제어)
									function(callback){
										logger.info('[topicMaker] Step#1-2-1 연관어 추출 (' + doc._id + ':' + topicObj.topic + ')');
										// ------------python 호출시 retry 처리
										var RETRY_TIME = 3;
										var RETRY_INTERVAL = 5000;

										var result = getCloseWordsStr(doc.doc_title, doc.doc_content, topicObj.topic);

										if(result.length > 2000){
											var start = result.substring(0,1000)
											var end = result.slice(-1000)
											result = start + end
										}
										var pyOptions = {
											mode : 'text',
											pythonPath : config.RELATED_CONFIG.PYTHON_PATH, // '/home/wisenut/anaconda3/bin/python',
											scriptPath :  config.RELATED_CONFIG.SCRIPT_PATH, // '/data/related-word-extractor',
											args : [result]
										};
										// ---------------------------------------------파이썬 호출 retry함수 설정
										function getRelatedWords(callback, results){
											PythonShell.run(config.RELATED_CONFIG.RUNFILE_NM2, pyOptions, function(err, rets) {
												logger.debug(result)
												if (result.length < 2000) {
													if(err) {// ----- OSError 발생시 ES 전반적인 커넥션 문제 발생 60초 sleep
														if (err.errno == 99){// Error: OSError: [Errno 99] Cannot assign requested address
															logger.error('[topicMaker] getRelatedWords ' + err);
															sleep.sleep(60);
														}
														callback(err, null);
													} else {
														logger.debug(rets);
														callback(null, rets);
													}
												} else {
													callback(null);
												}
											});
										
										};

										async.retry({times:RETRY_TIME, interval:RETRY_INTERVAL}, getRelatedWords, function(err, result){
											if (err){
												logger.error('[topicMaker] getRelatedWords ' + err);
												callback(err);
											}else{
												if(result != null) {
													var relatedWords = result[0].replace(/\'/g,'\"');
													topicObj.related_words = JSON.parse(relatedWords);
													if (topicObj.related_words.length == 0){
														async.retry({times:RETRY_TIME, interval:RETRY_INTERVAL}, getRelatedWords, function(err2, result2){
															if (err2){
																logger.error('[topicMaker] getRelatedWords ' + err2);
																callback(err2);
															}else{
																if(result2 != null) {
																	var relatedWords = result2[0].replace(/\'/g,'\"');
																	topicObj.related_words = JSON.parse(relatedWords);
																	logger.debug('[topicMaker] relatedWord=' + topicObj.related_words);
																	callback(null);
																} else {
																	logger.warn('[topicMaker] NO_RELATED_WORDS');
																	topicObj.related_words = "";
																	callback(null);
																}
															}
														});
													}else{
														logger.debug('[topicMaker] relatedWord=' + topicObj.related_words);
														callback(null);
													}											
												} else {
													logger.warn('[topicMaker] NO_RELATED_WORDS');
													topicObj.related_words = "";
													callback(null);
												}
											}
										});

									 }
									 , function(callback){
										 retTerms.push(topicObj);
										 callback(null);
									}
								], function(err) {
									if(err) {
										callback(err);
									} else { 
										callback(null);
									}
								}); 
								//waterfall (TERM)
							// 연관어 분석 진행하지 않을 경우
							}else{
								topicObj.related_words = [];
								retTerms.push(topicObj);
								callback(null);
							}
							/*
							async.waterfall([
								// Step#1-2-1 연관어 추출 (문서:화제어)
								function(callback){
									logger.info('[topicMaker] Step#1-2-1 연관어 추출 (' + doc._id + ':' + topicObj.topic + ')');
									// ------------python 호출시 retry 처리
									var RETRY_TIME = 3;
									var RETRY_INTERVAL = 5000;
									var es_path = '/documents-' + doc.doc_datetime.substring(0,"yyyy-mm-dd".length).replace(/\-/g, '.')+'/_search';

									var pyOptions = {
										mode : 'text',
										pythonPath : config.RELATED_CONFIG.PYTHON_PATH, // '/home/wisenut/anaconda3/bin/python',
										scriptPath :  config.RELATED_CONFIG.SCRIPT_PATH, // '/data/related-word-extractor',
										args : [config.ES_CONFIG.HOST, es_path, doc._id, topicObj.topic, 'False']
									};
									// ---------------------------------------------파이썬 호출 retry함수 설정
									function getRelatedWords(callback, results){
										PythonShell.run(config.RELATED_CONFIG.RUNFILE_NM, pyOptions, function(err, rets) {
											if(err) {// ----- OSError 발생시 ES 전반적인 커넥션 문제 발생 60초 sleep
												if (err.errno == 99){// Error: OSError: [Errno 99] Cannot assign requested address
													logger.error('[topicMaker] getRelatedWords ' + err);
													sleep.sleep(60);
												}
												callback(err, null);
											} else {
												// logger.debug(rets);
												callback(null, rets);
											}
										});
									
									};

									async.retry({times:RETRY_TIME, interval:RETRY_INTERVAL}, getRelatedWords, function(err, result){
										if (err){
											logger.error('[topicMaker] getRelatedWords ' + err);
											callback(err);
										}else{
											if(result != null) {
												var relatedWords = result[0].replace(/\'/g,'\"');
												topicObj.related_words = JSON.parse(relatedWords);
												if (topicObj.related_words.length == 0){
													async.retry({times:RETRY_TIME, interval:RETRY_INTERVAL}, getRelatedWords, function(err2, result2){
														if (err2){
															logger.error('[topicMaker] getRelatedWords ' + err2);
															callback(err2);
														}else{
															if(result2 != null) {
																var relatedWords = result2[0].replace(/\'/g,'\"');
																topicObj.related_words = JSON.parse(relatedWords);
																logger.debug('[topicMaker] relatedWord=' + topicObj.related_words);
																callback(null);
															} else {
																logger.warn('[topicMaker] NO_RELATED_WORDS');
																topicObj.related_words = "";
																callback(null);
															}
														}
													});
												}else{
													logger.debug('[topicMaker] relatedWord=' + topicObj.related_words);
													callback(null);
												}											
											} else {
												logger.warn('[topicMaker] NO_RELATED_WORDS');
												topicObj.related_words = "";
												callback(null);
											}
										}
									});

								 }
								 , function(callback){
									 retTerms.push(topicObj);
									 callback(null);
								}
							], function(err) {
								if(err) {
									callback(err);
								} else { 
									callback(null);
								}
							}); 
							//waterfall (TERM)
							*/
						} else {
							// logger.info('[topicMaker] Step#2 동사형 화제어 PUSH');
							retTerms.push(topicObj);
							callback(null);
						}
					}, function(err) {
						if(err) {
							callback(err);
						} else {
							// ---------------------------- 화제어 중복 제거
							var uniq = retTerms.reduce(function(a,b){
								var isSameTopic = false;
								for(var i=0; i<a.length; i++){
									if (a[i].topic === b.topic){
										if (a[i].topic_class === 'VV'){
											a[i] = b;
										}
										isSameTopic = true;
										break;
									}
								}
								if (!isSameTopic){a.push(b);}		
								return a;
							},[]);
							// ----------------------------
							callback(null, uniq);
						}
					});
					// each (TERMS)
				}
			], function(err, retTerms) {
				if(err) {
					callback(err);
				} else {		
					// Step#2 화제어 저장
					logger.info('[topicMaker] Step#3 화제어 저장');
					logger.debug('[topicMaker] projectSeq=' + doc.project_seq + ' / url ' + doc.doc_url);
					jsonMaker.getTopicJson(doc, retTerms, function(err, returnJsons) {
						if(err) {
							callback(err);
						} else {
							jsonMaker.makeJsonOutput(returnJsons, config.DOC_TYPE.TYPE_TOPICS, function(err, ret) {
								if (err){
									callback(err);
								} else {
									logger.debug('[topicMaker] CallbackCnt=' + (++callback_cnt));
									returnArray.push(ret);
									callback(null);
								}
							}); // save2ES
						}
					}); // getTopicJson
				}
			}); 
			// waterfall (DOC)
		}, function(err) {
			if(err) {
				callback(err);
			} else {
				logger.debug('[topicMaker] End Topic Maker ' + doc_cnt);
				// logger.debug(returnArray);
				callback(null, returnArray);
			}
		}); 
		// eachLimit (DOCS)
	}; 

	function getUniq(obj){
		return obj.reduce(function(a,b){
			var isSameTopic = false;
			for(var i=0; i<a.length; i++){
				if (a[i] === b){
					a[i] = b;
					isSameTopic = true;
					break;
				}
			}
			if (!isSameTopic){a.push(b);}		
			return a;
		},[]);
	}

	/**
	* getCloseWords
	* @param : start_arr 시작태그를 기준으로 나눈 배열
	* @param : end_arr 종료태그를 기준으로 나눈 배열
	* @return : Array
	*/
	function getCloseWords(start_arr, end_arr){
		var this_close_arr = [];

		for (var i=0; i<end_arr.length; i++){
			// 종료태그 기준으로 나눈 배열에서 시작태그가 있는 오브젝트만 진행
			if (end_arr[i].includes(config.RELATED_CONFIG.START_TAG)){
				// console.log(end_arr[i]);
				end_arr[i] = end_arr[i] + config.RELATED_CONFIG.END_TAG;
				// 어절로 WINDOW_SIZE 만큼 가져오기
				var this_words_arr = end_arr[i].trim().split(' ');
				var split_size = config.RELATED_CONFIG.START_WINDOW_SIZE;
				if (this_words_arr.length < config.RELATED_CONFIG.START_WINDOW_SIZE){	
					split_size = this_words_arr.length;
				}
				for (var j=this_words_arr.length-1; j>=this_words_arr.length-split_size; j--){
					// console.log('end_arr['+i+']['+j+']>>>'+this_words_arr[j]);
					this_close_arr.push(this_words_arr[j]);
				}
			}
			for (var k=i; k<start_arr.length; k++){
				// 시작태그 기준으로 나눈 배열에서 종료태그가 있는 오브젝트만 진행
				if (start_arr[k].includes(config.RELATED_CONFIG.END_TAG)){
					start_arr[k] = config.RELATED_CONFIG.START_TAG + start_arr[k];
					var this_words_arr = start_arr[k].trim().split(' ');
					var split_size = config.RELATED_CONFIG.END_WINDOW_SIZE;
					if (this_words_arr.length < config.RELATED_CONFIG.END_WINDOW_SIZE){	
						split_size = this_words_arr.length;
					}
					for (var l=0; l<split_size; l++){
						// console.log('start_arr['+k+']['+l+']>>>'+this_words_arr[l]);
						this_close_arr.push(this_words_arr[l]);
					}
				}
				break;
			}
		}
		 delete start_arr;	delete end_arr;
		// start_arr = undefined;
		// end_arr = undefined;
		return this_close_arr;
	}

	/**
	* getCloseWordsStr
	* @param : title
	* @param : content
	* @param : topic 단일 화제어
	* @return : String
	*/
	function getCloseWordsStr (title, content, topic) {
		//	화제어를 기준 앞 뒤로 띄어쓰기 되어있는 경우만 추출한다.
		// var regex = new RegExp("\\s?"+topic+"\\s|\\s"+topic+"\\s?", "g");
		var regex = new RegExp("\\s"+topic+"\\s", "g");
		// 화제어 highlight 처리
		var replace_topic_str = config.RELATED_CONFIG.START_TAG + topic + config.RELATED_CONFIG.END_TAG;
		title = title.replace(regex, replace_topic_str);
		content = content.replace(regex, replace_topic_str);
		var repRegex = new RegExp("\<em\>"+topic+"\<\/em\>", "g");
		var loop_num =0;
		// 근접 단어 배열
		var this_close_arr = [];
		if (title.match(repRegex) != null){
			// 앞에서 부터 FRAGMENT 갯수만큼만 연관분석 대상으로 함
			loop_num = title.match(repRegex).length;
			if (config.RELATED_CONFIG.TITLE_NUM_OF_FRAGMENTS < loop_num){
				loop_num = loop_num - config.RELATED_CONFIG.TITLE_NUM_OF_FRAGMENTS;
				for (var i=0; i<loop_num; i++){
					title = title.substring(0,title.lastIndexOf(replace_topic_str));
				}
			}
			var start_title_arr = title.split(config.RELATED_CONFIG.START_TAG);
			var end_title_arr = title.split(config.RELATED_CONFIG.END_TAG);
			// console.log(start_title_arr);
			// console.log(end_title_arr);
			// 제목에서 찾기
			this_close_arr = getCloseWords(start_title_arr, end_title_arr);
			start_title_arr = undefined;
			delete end_title_arr;
		}

		if (content.match(repRegex) != null){
			loop_num = content.match(repRegex).length;
			if (config.RELATED_CONFIG.CONTENT_NUM_OF_FRAGMENTS < loop_num){
				loop_num = loop_num - config.RELATED_CONFIG.CONTENT_NUM_OF_FRAGMENTS;
				for (var i=0; i<loop_num; i++){
					content = content.substring(0,content.lastIndexOf(replace_topic_str));
				}
			}
			var start_content_arr = content.split(config.RELATED_CONFIG.START_TAG);
			var end_content_arr = content.split(config.RELATED_CONFIG.END_TAG);
			// 내용에서 찾기
			this_close_arr = this_close_arr.concat(getCloseWords(start_content_arr, end_content_arr));
			start_content_arr = undefined;
			delete end_content_arr;
		}
		
		loop_num = undefined; // delete loop_num;
		return getUniq(this_close_arr).join(' ');
	}

	
	return {
		process : process 
	};
	
})();

if (exports) {
    module.exports = crawlDocTopicMakerParallel;
}
