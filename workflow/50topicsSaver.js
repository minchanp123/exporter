const async = require('async');
const fs = require('fs-extra');
const config = require('../lib/config');
const logger = require('../lib/logger');
const slack = require('../lib/slack');
const mariaDBDao = require('../dao/mariaDBDao');
const jsonMaker = require('../lib/50jsonMaker');
const md5 = require('md5');
const crawlDocTopicMaker = require('../routes/crawlDocTopicMakerParallel');
const crawlDocCommentTopicMaker = require('../routes/crawlDocCommentTopicMakerParallel');
var topicsSaver = (function(){

	var process = function(sourcePath, customerKeywords, ret_num, callback){
		var sourceProcessingPath = './topics_processing/';
		var sourceEmotionsPath = './emotions/'; //감성 분석을 하는 경우
		var sourceDonePath = './topics_done/'; //화제어 분석만을 하는 경우

		async.eachSeries(customerKeywords, function(customerKeyword, callback) {
			var customer = customerKeyword.customer;
			var keyword = customerKeyword.keyword;
			var destPath = '';
			var relatedWordsY = false;
			var project_seq = [];

			logger.info(attachLoggerHeader(ret_num,'customer : ' + customer + '/ keyword : ' + keyword));
			async.waterfall([
				// Step#4-1 PROCESSING 폴더로 이동
				function(callback) {
					logger.info(attachLoggerHeader(ret_num,'Step#4-1 PROCESSING 폴더로 이동'));
					// './topics/' + customer + '/' + keyword
					// fs.move(sourcePath + customer + '/' + keyword, sourceProcessingPath + customer + '/' + keyword, function(err) {
					fs.pathExists(sourceProcessingPath + customer + '/' + keyword, function(err, exists) {
						if(err) {
							logger.error(attachLoggerHeader(ret_num,'ERR_PATH_EXIST_FAILED ' + err));
							callback(err);
						} else {
							// 키워드 폴더가 존재한다면
							if (exists){
								logger.debug(attachLoggerHeader(ret_num,'키워드 폴더가 존재한다면'));
								fs.readdir(sourcePath + customer + '/' + keyword, function(err, scdFolders){
									if (scdFolders.length === 0){
										// TOPICS 폴더 아래에 SCD가 없다면
										logger.warn(attachLoggerHeader(ret_num,'NO_SCDFOLDERS '));
										callback('ERR_NO_SCDFOLDERS');
									} else {
										async.eachSeries(scdFolders, function(scdFolder, callback){
											// TOPICS 폴더의 SCDFOLDER를 PROCESSING으로 이동
											logger.debug(attachLoggerHeader(ret_num, sourcePath + customer + '/' + keyword + '/' + scdFolder));
											fs.move(sourcePath + customer + '/' + keyword + '/' + scdFolder, sourceProcessingPath + customer + '/' + keyword + '/' + scdFolder, function(err2){
												if(err2) {
													logger.error(attachLoggerHeader(ret_num,'ERR_SCDFOLDER_MOVE_FAILED ' + err2));
													callback(err2);
												} else {
													callback(null);
												}
											});
										},function(err3){
											if (err3){
												callback(err3);
											}else{
												logger.debug(attachLoggerHeader(ret_num,'키워드 폴더 이동 후'));
												fs.readdir(sourcePath + customer + '/' + keyword, function(err4, scdFolders){
													if (err4){
														callback(err4);
													}else{
														logger.debug(attachLoggerHeader(ret_num, sourcePath + customer + '/' + keyword + '/' ));
														logger.debug(scdFolders);
														// keyword 폴더 아래에 남아있는 폴더 개수가 0이 아닌경우 <- move가 제대로 되지 않음 혹은 새 파일이 이동 됨
														if (scdFolders.length != 0){
															callback(null);
															// err = 'ERR_JSON_FOLDER_MOVE_FAILED';
															// callback(err);
														}else{
															logger.debug(attachLoggerHeader(ret_num,'키워드 폴더 삭제'));
															fs.remove(sourcePath + customer + '/' + keyword, callback);
														}
													}
												});
											}
										});
									}
								});
							}else{
								// 키워드 폴더가 존재하지 않는다면
								// logger.debug(attachLoggerHeader(ret_num,'키워드 폴더가 존재하지 않는다면'));
								fs.move(sourcePath + customer + '/' + keyword, sourceProcessingPath + customer + '/' + keyword, callback);
							}
						}
					}); //move
				},
				function(callback){
					logger.info(attachLoggerHeader(ret_num,'Step#4-2 DestPath 및 프로젝트seq 지정'));
					mariaDBDao.selectSearchKeywordInfo({customer_id : customer, search_keyword_text : keyword}, function(err, res){
						if (err){
							callback(err);
						}else{
							if(res.length == 0){
								 err = "ERR_NO_PROJECT_SEQ";
								 callback(err);
							}else {
								var isEmotionsY = false;
								if (res.length > 1){
									for (var i=0; i<res.length; i++){
										if (res[i].emotions_yn == 'Y'){isEmotionsY = true;}
										if (res[i].related_words_yn == 'Y'){relatedWordsY = true;}
										project_seq.push(res[i].project_seq);
									}
								}else{
									if (res[0].emotions_yn == 'Y'){isEmotionsY = true;}
									if (res[0].related_words_yn == 'Y'){relatedWordsY = true;}
									project_seq.push(res[0].project_seq);
								}
								// 파일 처리후 이동할 폴더 지정
								if (isEmotionsY){	//감정분석 진행시
									destPath = sourceEmotionsPath;
								}else{	//화제어만 진행시(원문 & 화제어)
									destPath = sourceDonePath;
								}
								callback(null);
							}
						}
					});
					// mariaDBDao.selectSearchKeywordInfo
				},
				// Step#4-2 JSON 목록 가져오기
				function(callback) {
					logger.info(attachLoggerHeader(ret_num,'Step#4-3 JSON 목록 가져오기'));
					try {
						var jsons = [];
						getFiles(sourceProcessingPath + customer + '/' + keyword, jsons);
						callback(null, jsons);
					} catch(err) {
						callback(err);
					}
				},
				// Step#5 JSON 파일 처리
				function(jsons, callback) {
					logger.info(attachLoggerHeader(ret_num,'Step#5 JSON 파일 처리 ' + jsons.length));

					async.eachSeries(jsons, function(json, callback) {
						const folderNm = json.split("/")[2];
						const fileNm = json.substring(json.lastIndexOf('/')+1, json.length);
						async.waterfall([
							// Step#5-1 JSON 파일 객체로 변경
							function(callback) {
								logger.debug(attachLoggerHeader(ret_num, '' + json));
								fs.readJson(json, function(err, docs) {
									if (err){
										callback('ERR_INVALID_JSON');
									}else{
										var docArray = [];
										if (docs.length === undefined) {
											docArray.push(docs);
										} else {
											docArray = docs;
										}
										logger.info(attachLoggerHeader(ret_num,'Step#5-1 JSON 파일 객체로 변경 ' + docArray.length));
										var skipIdxArr = [];
										var skipIdx = 0;
										// callback(null, docs);

										async.eachSeries(docArray, function(doc, callback) {
											// 2017.09.12 신규수집기(인스타그램) 수집시 키워드 대신 uuid 가 들어가므로 uuid 여부를 판단해야함..
											// ex) ./topics_processing/kdnavien/6567aa20-355e-11e7-afc0-9141438e508a/linkDocCollector/20170511/*.json
											// doc.uuidStr = json.split("/")[2];
											doc.uuidStr = folderNm;
											// 기본정보(project_Seq, datetime, doc_id) 셋팅
											doc.project_seq = project_seq;
											logger.debug(doc.project_seq + ' / ' +doc.doc_url);
											// SCD->json 변경 시 확인
											//if (typeof doc.depth1_nm !== 'undefined' && doc.depth1_nm === '포털' ){
												//doc.doc_url = jsonMaker.urlChanger(doc);	// 일반 포털 검색시 url 포맷 변경
											//}
											// 신규수집기 수집시 확인
											//if (typeof doc.source !== 'undefined' && (doc.source === '지식 검색' || doc.source === '카페' )){
												//doc.doc_url = jsonMaker.urlChanger(doc);	// 일반 포털 검색시 url 포맷 변경
											//}

											doc.doc_datetime = jsonMaker.datetimeMapper(doc);
											doc._id = md5(jsonMaker.getDocPkStr(doc));
											// 기본정보(project_Seq, datetime, doc_id) 셋팅
											logger.debug(doc.doc_url);
											callback(null);
											//doc.doc_id = md5(getDocPkStr(doc));
										}, function(err) {
											if(err) {
												callback(err);
											} else {
												callback(null, removeByKey(docArray, {key : 'doc_datetime', value : "NaN-NaN-NaNTNa:NN:aN"}, skipIdxArr.length));
											}
										});
										//eachSeries (DOCS)
									}
								});
								// readJson (JSON)
							},
							// Step#5-2 topics array 생성
							function(docs, callback) {
								logger.info(attachLoggerHeader(ret_num,'Step#5-2 topics array 생성 ' + docs.length));
								// crawlDocTopicMaker.process(docs, function(err, topicsArray){
								logger.info("fileNm : "+fileNm);		
								if(fileNm.substring(0,1) == "C"){
									crawlDocCommentTopicMaker.process(docs, relatedWordsY, function(err, topicsArray){
										if (err){
											callback(err);
										} else {
											// json 파일 생성 @destPath, jsonArray, docType, callback
											jsonMaker.writeJsonFile(
												config.JSON_PATH.OUTPUT_JSON + '/' + customer + '/' + keyword + '/' + json.split("/")[4]
												, topicsArray
												, config.DOC_TYPE.TYPE_TOPICS
												, function (err, returnName){
													if (err){ callback(err);
													}else{
														fs.move(returnName, returnName.replace('json.swap', 'json'), {overwrite:true}, callback);
													}
											});
										}
									});
								}else{
									crawlDocTopicMaker.process(docs, relatedWordsY, function(err, topicsArray){
										if (err){
											callback(err);
										} else {
											// json 파일 생성 @destPath, jsonArray, docType, callback
											jsonMaker.writeJsonFile(
												config.JSON_PATH.OUTPUT_JSON + '/' + customer + '/' + keyword + '/' + json.split("/")[4]
												, topicsArray
												, config.DOC_TYPE.TYPE_TOPICS
												, function (err, returnName){
													if (err){ callback(err);
													}else{
														fs.move(returnName, returnName.replace('json.swap', 'json'), {overwrite:true}, callback);
													}
											});
										}
									});
								}
								
								// process
							},
							// =========Step#5-3 폴더 이동
							function(callback) {
								logger.info(attachLoggerHeader(ret_num,'Step#5-3 화제어 저장 완료 json 폴더 이동 destPath : ' + destPath));
								fs.ensureDir(destPath + customer + '/' + keyword, function(err){
									if (err){
										callback(err);
									}else{
										//fs.move(json, json.replace(sourceProcessingPath,destPath), function(err2) {
										fs.move(json, json.replace(sourceProcessingPath,destPath), {overwrite:true}, function(err2) {
											if(err2) {
												logger.error(attachLoggerHeader(ret_num,'ERR_JSON_MOVE_FAILED ' + err2));
												callback(err2);
											} else {
												var folderNm = json.split("/")[4];
//												fs.readdir(sourceProcessingPath + '/' + customer + '/' + keyword + '/' + folderNm, function(files, err){
												fs.readdir(sourceProcessingPath + customer + '/' + keyword + '/' + folderNm, function(err3, files){
													if (err3){
														logger.error(attachLoggerHeader(ret_num,'ERR_READDIR '+ sourceProcessingPath + customer + '/' + keyword + '/' + folderNm + ' : ' + err3 ));
														callback(err3);
													}else{
														logger.debug(files);
														if (typeof(files) == 'undefined' || typeof(files) == 'null' || files.length == 0 ){//folderNm 폴더 아래에 남아있는 폴더 개수가 0이 아닌경우 <- move가 제대로 되지 않음 혹은 남은 json이 있음
															logger.debug(attachLoggerHeader(ret_num,'processing scd folder 삭제'));
//															fs.remove(sourceProcessingPath + '/' + customer + '/' + keyword + '/' + folderNm, callback);
															fs.remove(sourceProcessingPath + customer + '/' + keyword + '/' + folderNm, function(err4){
																if (err4){
																	logger.error(err4); callback(err4);
																}else{
																	logger.debug(attachLoggerHeader(ret_num,'remove done!!!!'));
																	callback(null);
																}
															});
														}else{
															callback(null);
														}
													}
												});
											}//else err2
										}); //move
									}
								});//ensureDir
							}
						// ======================================================================
						], function(err) {
							if(err) {
								if(err === 'ERR_NO_CHANNEL_LIST') {
									logger.warn(attachLoggerHeader(ret_num,'ERR_NO_CHANNEL_LIST ' + json));
									callback(null);
								} else if(err === 'ERR_NO_PROJECT_SEQ') {
									logger.warn(attachLoggerHeader(ret_num,'ERR_NO_PROJECT_SEQ ' + json));
									callback(null);
								} else if(err === 'ERR_UNKONWN_FILE_TYPE') {
									logger.warn(attachLoggerHeader(ret_num,'ERR_UNKONWN_FILE_TYPE ' + json));
									callback(null);
								} else {
									logger.error(attachLoggerHeader(ret_num, err + ' ' + json));
									callback(err);
								}
							} else {
								callback(null);
							}
						}); // waterfall(JSON)
					}, callback);
					// eachSeries(JSON)
				}
			], function(err) {
				if(err) {
					logger.debug(attachLoggerHeader(ret_num,'Step#4-3 KEYWORD 폴더 처리중 에러 발생'));
					//sourceProcessingPath -> sourcePath 로 이동
					fs.pathExists(sourcePath + customer + '/' + keyword, function(err, exists) {
						if(err) {
							logger.error(attachLoggerHeader(ret_num,'ERR_PATH_EXIST_FAILED ' + err));
							callback(err);
						} else {
							if (exists){	//존재한다면
								logger.debug(attachLoggerHeader(ret_num,'키워드 폴더가 존재한다면'));
								fs.readdir(sourceProcessingPath + customer + '/' + keyword, function(err1, scdFolders){
									if (scdFolders.length === 0){
										fs.remove(sourceProcessingPath + customer + '/' + keyword, callback);
										//callback('ERR_NO_SCDFOLDERS');
									} else {
										async.eachSeries(scdFolders, function(scdFolder, callback){
											fs.move(sourceProcessingPath + customer + '/' + keyword + '/' + scdFolder, sourcePath + customer + '/' + keyword + '/' + scdFolder, function(err2){
												if(err2) {
													logger.error(attachLoggerHeader(ret_num,'ERR_SCDFOLDER_MOVE_FAILED ' + err2));
													callback(err2);
												} else {
													logger.debug(attachLoggerHeader(ret_num,'scd 폴더 이동' +  customer + '/' + keyword + '/' + scdFolder + '>>' + sourcePath + customer + '/' + keyword + '/' + scdFolder));
													callback(null);
												}
											});
										},function(err3){
											if (err3){
												callback(err3);
											}else{
												logger.debug(attachLoggerHeader(ret_num,'PROCESSING/keyword 아래 확인'));
												fs.readdir(sourceProcessingPath + customer + '/' + keyword, function(err4, scdFolders){
													if (err4){
														callback(err4);
													}else{
														if (scdFolders.length != 0){//keyword 폴더 아래에 남아있는 폴더 개수가 0이 아닌경우 <- move가 제대로 되지 않음 혹은 새 파일이 이동 됨
															callback('ERR_JSON_FOLDER_MOVE_FAILED');//callback(null);
														}else{
															logger.debug(attachLoggerHeader(ret_num,'삭제'));
															fs.remove(sourceProcessingPath + customer + '/' + keyword, callback);
														}
													}
												});
											}
										});
									}
								});
							}else{
								// 존재하지 않는다면
								fs.move(sourceProcessingPath + customer + '/' + keyword, sourcePath + customer + '/' + keyword, function(err2){
									if (err2){
										logger.error(err2);
										callback(err2);
									}else{
										logger.error(err);
										callback(err);
									}
								});
							}
						}
					});
					// move
				} else {
					// Step#4-4 화제어 저장 완료 KEYWORD 폴더 이동
					logger.debug(attachLoggerHeader(ret_num,' Step#4-5 PROCESSING 빈 폴더 삭제'));
					fs.readdir(sourceProcessingPath + customer + '/' + keyword, function(err, scdFolders){
						if (err){
							callback(err);
						}else{
							if (scdFolders.length != 0){//keyword 폴더 아래에 남아있는 폴더 개수가 0이 아닌경우
								async.eachSeries(scdFolders, function(scdFolder, callback){
									fs.readdir(sourceProcessingPath + customer + '/' + keyword + '/' + scdFolder, function(err, files){
										if (files.length == 0){//SCD폴더 안에 있는 파일 개수가 0개인 경우
											fs.remove(sourceProcessingPath + customer + '/' + keyword + '/' + scdFolder,callback);
										}
									});
								},function(err){
									if (err){
										callback(err);
									}else{
										callback(null);
									}
								});
							}else{
								fs.remove(sourceProcessingPath + customer + '/' + keyword, callback);
								callback(null);
							}
						}
					});
				}
			}); //waterfall (KEYWORD)
		}, function(err) {
			if (err) {
				var slackMessage = {
						color: 'warning',
						title: 'topics',
						value: '[topics] topics process failed : ' + err
					};

					slack.sendMessage(slackMessage, function(slack_err) {
						if (slack_err) {
							logger.warn(attachLoggerHeader(ret_num, slack_err));
							callback(slack_err);
						} else {
							logger.info(attachLoggerHeader(ret_num,'Successfully push message to Slack'));
							callback(err);
						}
					});
//				callback(err);
			} else {
				//logger.debug(attachLoggerHeader(ret_num, customerKeyword.customer + ' / ' + customerKeyword.keyword + ' Finished!!!'));
				logger.debug(attachLoggerHeader(ret_num, ' Finished!!!'));
				callback(null);
			}
		}); //eachLimit (customerKeywords)
	}; //process

	var attachLoggerHeader = function(ret_num, str){
		return ('[topicsSaver][' + ret_num + '] '+ str);
	}

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
		for (var i=0; i<num; i++){
			array.some(function(item, index) {
				return (array[index][params.key] === params.value) ? !!(array.splice(index, 1)) : false;
			});
		};
		logger.debug('[removeByKey]');
	  return array;
	}//removeByKey


	return {
    	process: process
    };

})();

if (exports) {
	module.exports = topicsSaver;
}
