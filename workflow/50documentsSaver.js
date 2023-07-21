const async = require('async');
const fs = require('fs-extra');
const config = require('../lib/config');
const logger = require('../lib/logger');
const slack = require('../lib/slack');
const jsonMaker = require('../lib/50jsonMaker');

var documentsSaver = (function () {
	var process = function (sourcePath, customerKeywords, ret_num, callback) {
		var mariaDBDao = require('../dao/mariaDBDao');
		var sourceProcessingPath = './documents_processing/';
		var sourceTopicsPath = './topics/'; //화제어 분석을 하는 경우
		var sourceEmotionsPath = './emotions/'; //화제어 분석을 하는 경우
		var sourceDonePath = './documents_done/'; //원문 insert만 하는 경우

		async.eachSeries(customerKeywords, function (customerKeyword, callback) {
			var customer = customerKeyword.customer;
			var keyword = customerKeyword.keyword;
			var destPath = '';
			var project_seq = [];

			logger.info(attachLoggerHeader(ret_num, 'customer : ' + customer + '/ keyword : ' + keyword));
			async.waterfall([
				// Step#4-1 PROCESSING 폴더로 이동
				function (callback) {
					logger.info(attachLoggerHeader(ret_num, 'Step#4-1 PROCESSING 폴더로 이동'));
					fs.pathExists(sourceProcessingPath + customer + '/' + keyword, function (err, exists) {
						if (err) {
							logger.error(attachLoggerHeader(ret_num, 'ERR_PATH_EXIST_FAILED ' + err));
							callback(err);
						} else {
							// 키워드 폴더가 존재한다면
							if (exists) {
								fs.readdir(sourcePath + customer + '/' + keyword, function (err1, scdFolders) {
									if (scdFolders.length === 0) {
										// documents 폴더 아래에 SCD가 없다면
										logger.warn(attachLoggerHeader(ret_num, 'NO_SCDFOLDERS '));
										callback('ERR_NO_SCDFOLDERS');
									} else {
										async.eachSeries(scdFolders, function (scdFolder, callback) {
											// documents 폴더의 SCDFOLDER를 PROCESSING으로 이동
											logger.debug(attachLoggerHeader(ret_num, sourcePath + customer + '/' + keyword + '/' + scdFolder));
											fs.move(sourcePath + customer + '/' + keyword + '/' + scdFolder, sourceProcessingPath + customer + '/' + keyword + '/' + scdFolder, function (err2) {
												if (err2) {
													logger.error(attachLoggerHeader(ret_num, 'ERR_SCDFOLDER_MOVE_FAILED ' + err2));
													callback(err2);
												} else {
													callback(null);
												}
											});
										}, function (err3) {
											if (err3) {
												callback(err3);
											} else {
												logger.debug(attachLoggerHeader(ret_num, '키워드 폴더 이동 후'));
												fs.readdir(sourcePath + customer + '/' + keyword, function (err4, scdFolders) {
													if (err4) {
														callback(err4);
													} else {
														logger.debug(attachLoggerHeader(ret_num, sourcePath + customer + '/' + keyword + '/'));
														logger.debug(scdFolders);
														if (scdFolders.length != 0) {
															// keyword 폴더 아래에 남아있는 폴더 개수가 0이 아닌경우 <- move가 제대로 되지 않음 혹은 새 파일이 이동 됨
															callback(null);
															// err = 'ERR_JSON_FOLDER_MOVE_FAILED';
															// callback(err);
														} else {
															logger.debug(attachLoggerHeader(ret_num, '키워드 폴더 삭제'));
															fs.remove(sourcePath + customer + '/' + keyword, callback);
														}
													}
												});
											}
										});
									}
								});
							} else {
								// 존재하지 않는다면
								fs.move(sourcePath + customer + '/' + keyword, sourceProcessingPath + customer + '/' + keyword, callback);
							}
						}
					}); //move
				},
				function (callback) {
					logger.info(attachLoggerHeader(ret_num, 'Step#4-2 DestPath 및 프로젝트seq 지정'));
					logger.info("keyword : "+keyword)
					mariaDBDao.selectSearchKeywordInfo({
						customer_id: customer,
						search_keyword_text: keyword
					}, function (err, res) {
						if (err) {
							callback(err);
						} else {
							if (res.length == 0) {
								err = "ERR_NO_PROJECT_SEQ";
								callback(err);
							} else {
								var isTopicsY = false;
								var isEmotionsY = false;
								if (res.length > 1) {
									for (var i = 0; i < res.length; i++) {
										if (res[i].topics_yn == 'Y') {
											isTopicsY = true;
										}
										if (res[i].emotions_yn == 'Y') {
											isEmotionsY = true;
										}
										project_seq.push(res[i].project_seq);
									}
								} else {
									if (res[0].topics_yn == 'Y') {
										isTopicsY = true;
									}
									if (res[0].emotions_yn == 'Y') {
										isEmotionsY = true;
									}
									project_seq.push(res[0].project_seq);
								}
								// 파일 처리후 이동할 폴더 지정
								if (isTopicsY) {
									// 화제어분석 진행시
									destPath = sourceTopicsPath;
								} else if (!isTopicsY && isEmotionsY) {
									// 감성분석만 진행시 파일 이동
									destPath = sourceEmotionsPath;
								} else {
									// 원문 insert 만 진행시(화제어 / 감성분석 진행 X)
									destPath = sourceDonePath;
								}
								callback(null);
							}
						}
					});
					// mariaDBDao.selectSearchKeywordInfo
				},
				// Step#4-2 JSON 목록 가져오기
				function (callback) {
					logger.info(attachLoggerHeader(ret_num, 'Step#4-3 JSON 목록 가져오기'));
					try {
						var jsons = [];
						getFiles(sourceProcessingPath + customer + '/' + keyword, jsons);
						callback(null, jsons);
					} catch (err) {
						callback(err);
					}
				},
				// Step#5 JSON 파일 처리
				function (jsons, callback) {
					logger.info(attachLoggerHeader(ret_num, 'Step#5 JSON 파일 처리 ' + jsons.length));
				
					async.eachSeries(jsons, function (json, callback) {
						const folderNm = json.split("/")[2];
						const scdNM = json.split("/")[4];
						const fileNm = json.substring(json.lastIndexOf('/') + 1, json.length);
						async.waterfall([
							// Step#5-1 JSON 파일 객체로 변경
							function (callback) {
								logger.debug(attachLoggerHeader(ret_num, json));
								fs.readJson(json, function (err, docs) {
									if (err) { 
 										logger.debug("fileNM : "+fileNm);
										//logger.debug('json : '+sourceProcessingPath + customer + '/' + keyword + '/' + scdNM);
										fs.move(json,'/data/dmap-exporter-04/documents_faild/'+fileNm)
									
										callback('ERR_INVALID_JSON');
									} else {
										var docArray = [];
										if (docs.length === undefined) {
											docArray.push(docs);
										} else {
											docArray = docs;
										}
										logger.info(attachLoggerHeader(ret_num, 'Step#5-1 JSON 파일 객체로 변경 ' + docArray.length));
										var skipIdxArr = [];
										var skipIdx = 0;

										async.eachSeries(docArray, function (doc, callback) {
											//2017.09.12 신규수집기(인스타그램) 수집시 키워드 대신 uuid 가 들어가므로 uuid 여부를 판단해야함..
											//ex) ./topics_processing/kdnavien/6567aa20-355e-11e7-afc0-9141438e508a/linkDocCollector/20170511/*.json
											//doc.uuidStr = json.split("/")[2];							
											doc.uuidStr = folderNm;
											//2017.12.19 신규수집기 수집분 구조
											//ex) ./topics_processing/kdnavien/requestSeq/*.json
											if (project_seq.length != 0) { //>>>>>>>>>>>>>>>>>>>>>>>projectSeq 지정
												doc.project_seq = project_seq;
											}

											if (fileNm.startsWith('D') > -1) {
												logger.info(attachLoggerHeader(ret_num, 'Step#5-1-1 원문 관련 메타 조회'));

												docsType = 'D';
												jsonMaker.collectJson2crawlJson_v2(doc, function (err, res) {
													if (err) {
														callback(err);
													} else {
														if (res != null) {
															logger.debug(doc._id);
															doc = res;
														} else {
															logger.debug('res == null');
															skipIdxArr.push(skipIdx);
														}
														skipIdx++;
														callback(null);
													}
												}); //collectJson2crawlJson_v2
											} else if (fileNm.startsWith('C') > -1) {
												logger.info(attachLoggerHeader(ret_num, 'Step#5-1-2 댓글 관련 메타 조회'));

												docsType = 'C';
												jsonMaker.collectJson2crawlJson_v2(doc, function (err, res) {
													if (err) {
														callback(err);
													} else {
														doc = res;
														callback(null);
													}
												}); //collectJson2crawlJson_v2
											}
										}, function (err) {
											if (err) {
												callback(err);
											} else {
												callback(null, removeByKey(docArray, {
													key: 'doc_datetime',
													value: "NaN-NaN-NaNTNa:NN:aN"
												}, skipIdxArr.length));
											}
										}); //eachSeries (DOCS)
									}
								}); //readJson (JSON)	

							},
							//Step#5-2 documents array 생성
							function (docs, callback) {
								logger.info(attachLoggerHeader(ret_num, 'Step#5-2 documents array 생성 ' + docs.length));
								jsonMaker.makeJsonOutput(docs, config.DOC_TYPE.TYPE_DOCUMENTS, function (err, rets) {
									if (err) {
										callback(err);
									} else {
										// json 파일 생성 @destPath, jsonArray, docType, callback
										jsonMaker.writeJsonFile(
											config.JSON_PATH.OUTPUT_JSON + '/' + customer + '/' + keyword + '/' + json.split("/")[4], rets, config.DOC_TYPE.TYPE_DOCUMENTS,
											function (err, returnName) {
												if (err) {
													callback(err);
												} else {
													fs.move(returnName, returnName.replace('json.swap', 'json'), {
														overwrite: true
													}, callback);
												}
											});
									}
								});
							},
							//=========Step#5-3 폴더 이동
							function (callback) {
								logger.info(attachLoggerHeader(ret_num, 'Step#5-3 원문 저장 완료 json 폴더 이동 destPath : ' + destPath));
								fs.ensureDir(destPath + customer + '/' + keyword, function (err) {
									if (err) {
										callback(err);
									} else {
										//fs.move(json, json.replace(sourceProcessingPath,destPath), function(err2) {
										fs.move(json, json.replace(sourceProcessingPath, destPath), {
											overwrite: true
										}, function (err2) {
											if (err2) {
												logger.error(attachLoggerHeader(ret_num, 'ERR_JSON_MOVE_FAILED ' + err2));
												callback(err2);
											} else {
												var folderNm = json.split("/")[4];
												//												fs.readdir(sourceProcessingPath + '/' + customer + '/' + keyword + '/' + folderNm, function(files, err){
												fs.readdir(sourceProcessingPath + customer + '/' + keyword + '/' + folderNm, function (err3, files) {
													if (err3) {
														logger.error(attachLoggerHeader(ret_num, 'ERR_READDIR ' + sourceProcessingPath + customer + '/' + keyword + '/' + folderNm + ' : ' + err3));
														callback(err3);
													} else {
														//logger.debug(files);
														if (typeof (files) == 'undefined' || typeof (files) == 'null' || files.length == 0) { //folderNm 폴더 아래에 남아있는 폴더 개수가 0이 아닌경우 <- move가 제대로 되지 않음 혹은 남은 json이 있음
															logger.debug(attachLoggerHeader(ret_num, 'processing scd folder 삭제'));
															//															fs.remove(sourceProcessingPath + '/' + customer + '/' + keyword + '/' + folderNm, callback);
															fs.remove(sourceProcessingPath + customer + '/' + keyword + '/' + folderNm, function (err4) {
																if (err4) {
																	logger.error(err4);
																	callback(err4);
																} else {
																	logger.debug(attachLoggerHeader(ret_num, 'remove done!!!!'));
																	callback(null);
																}
															});
														} else {
															callback(null);
														}
													}
												});
											} //else err2
										});
										// move
									}
								});
								// ensureDir						
							}
							// ======================================================================
						], function (err) {
							docsType = '';

							if (err) {
								if (err === 'ERR_NO_CHANNEL_LIST') {
									logger.warn(attachLoggerHeader(ret_num, 'ERR_NO_CHANNEL_LIST ' + json));
									callback(null);
								} else if (err === 'ERR_NO_PROJECT_SEQ') {
									logger.warn(attachLoggerHeader(ret_num, 'ERR_NO_PROJECT_SEQ ' + json));
									callback(null);
								} else if (err === 'ERR_UNKONWN_FILE_TYPE') {
									logger.warn(attachLoggerHeader(ret_num, 'ERR_UNKONWN_FILE_TYPE ' + json));
									callback(null);
								} else {
									logger.error(attachLoggerHeader(ret_num, err + ' ' + json));
									callback(err);
									/*logger.debug(attachLoggerHeader(ret_num, '오류 발생 sourcePath로 이동'));
									fs.move(json, json.replace(sourceProcessingPath,sourcePath), function(err2) {//오류 발생시 sourcePath로 이동
										if(err2) {
											logger.error(attachLoggerHeader(ret_num,'ERR_JSON_MOVE_FAILED ' + err2));
											callback(err2);
										} else {
											//fs.remove
											var folderNm = json.split("/")[2];
											fs.readdir(sourceProcessingPath + '/' + customer + '/' + keyword + '/' + folderNm, function(files, err3){
												if (err3){
													callback(err3);
												}else{
													if (files.length != 0){//folderNm 폴더 아래에 남아있는 폴더 개수가 0이 아닌경우 <- move가 제대로 되지 않음 혹은 남은 json이 있음
														callback(err);
													}else{
														fs.remove(sourceProcessingPath + '/' + customer + '/' + keyword + '/' + folderNm, function(err4){
															if (err4) {callback(err4);} else{callback(err);}
														});
													}
												}
											});
										}
									}); //move*/
								}
							} else {
								callback(null);
							}
						}); //waterfall(JSON)
					}, function (err) {
						if (err) {
							callback(err);
						} else {
							callback(null);
						}
					}); //eachSeries(JSON)
				}
			], function (err) {
				if (err) {
					logger.debug(attachLoggerHeader(ret_num, 'Step#4-3 KEYWORD 폴더 처리중 에러 발생'));
					logger.debug(attachLoggerHeader(ret_num, err));
					// sourceProcessingPath -> sourcePath 로 이동
					fs.pathExists(sourcePath + customer + '/' + keyword, function (err, exists) {
						if (err) {
							logger.error(attachLoggerHeader(ret_num, 'ERR_PATH_EXIST_FAILED ' + err));
							callback(err);
						} else {
							if (exists) {
								// 키워드 폴더가 존재한다면
								fs.readdir(sourceProcessingPath + customer + '/' + keyword, function (err1, scdFolders) {
									if (scdFolders.length === 0) {
										fs.remove(sourceProcessingPath + customer + '/' + keyword, callback);
									} else {
										async.eachSeries(scdFolders, function (scdFolder, callback) {
											fs.move(sourceProcessingPath + customer + '/' + keyword + '/' + scdFolder, sourcePath + customer + '/' + keyword + '/' + scdFolder, function (err2) {
												if (err2) {
													logger.error(attachLoggerHeader(ret_num, 'ERR_SCDFOLDER_MOVE_FAILED ' + err2));
													callback(err2);
												} else {
													logger.debug(attachLoggerHeader(ret_num, 'scd 폴더 이동' + customer + '/' + keyword + '/' + scdFolder + '>>' + sourcePath + customer + '/' + keyword + '/' + scdFolder));
													callback(null);
												}
											});
										}, function (err3) {
											if (err3) {
												callback(err3);
											} else {
												logger.debug(attachLoggerHeader(ret_num, 'PROCESSING/keyword 아래 확인'));
												fs.readdir(sourceProcessingPath + customer + '/' + keyword, function (err4, scdFolders) {
													if (err4) {
														callback(err4);
													} else {
														if (scdFolders.length != 0) { //keyword 폴더 아래에 남아있는 폴더 개수가 0이 아닌경우 <- move가 제대로 되지 않음 혹은 새 파일이 이동 됨
															callback('ERR_JSON_FOLDER_MOVE_FAILED'); //callback(null);
														} else {
															logger.debug(attachLoggerHeader(ret_num, '삭제'));
															fs.remove(sourceProcessingPath + customer + '/' + keyword, callback);
														}
													}
												});
											}
										});
									}
								});
							} else { //존재하지 않는다면
								fs.move(sourceProcessingPath + customer + '/' + keyword, sourcePath + customer + '/' + keyword, function (err2) {
									if (err2) {
										logger.error(err2);
										callback(err2);
									} else {
										logger.error(err);
										callback(err);
									}
								});
							}
						}
					}); //move

					/*var slackMessage = {
						color: 'warning',
						title: 'topics',
						value: '[documents] [keyword:' + keyword + '] topics process failed : ' + err
					};
					
					slack.sendMessage(slackMessage, function(slack_err) {
						if (slack_err) {
							logger.warn('[documents] ' + slack_err);
							callback(err);
						} else {
							logger.info('[documents] Successfully push message to Slack');
							callback(err);
						}										
					});*/

					/*fs.move(sourceProcessingPath + customer + '/' + keyword, sourcePath + customer + '/' + keyword, function(err2) {
						if(err2) {
							logger.error('[documents] ERR_KEYWORD_FOLDER_MOVE_FAILED ' + err2);
							callback(err);
						} else {

							var slackMessage = {
								color: 'warning',
								title: 'topics',
								value: '[documents] [keyword:' + keyword + '] topics process failed : ' + err
							};
							
							slack.sendMessage(slackMessage, function(slack_err) {
								if (slack_err) {
									logger.warn('[documents] ' + slack_err);
									callback(err);
								} else {
									logger.info('[documents] Successfully push message to Slack');
									callback(err);
								}										
							});
						}
					}); //move*/
				} else {
					//Step#4-4 원문 저장 완료 KEYWORD 폴더 이동
					logger.debug(attachLoggerHeader(ret_num, ' Step#4-5 PROCESSING 빈 폴더 삭제'));
					fs.readdir(sourceProcessingPath + customer + '/' + keyword, function (err, scdFolders) {
						if (err) {
							callback(err);
						} else {
							if (scdFolders.length != 0) { //keyword 폴더 아래에 남아있는 폴더 개수가 0이 아닌경우
								logger.debug(attachLoggerHeader(ret_num, ' /keyword 폴더 아래에 남아있는 폴더 개수가 0이 아닌경우'));
								async.eachSeries(scdFolders, function (scdFolder, callback) {
									fs.readdir(sourceProcessingPath + customer + '/' + keyword + '/' + scdFolder, function (err, files) {
										if (files.length == 0) { //SCD폴더 안에 있는 파일 개수가 0개인 경우
											logger.debug(attachLoggerHeader(ret_num, ' /SCD폴더 안에 있는 파일 개수가 0개인 경우'));
											fs.remove(sourceProcessingPath + customer + '/' + keyword + '/' + scdFolder, callback);
										}
									});
								}, function (err) {
									if (err) {
										callback(err);
									} else {
										callback(null);
									}
								});
							} else {
								fs.remove(sourceProcessingPath + customer + '/' + keyword, callback);
							}
						}
					});
					/*
												logger.debug(attachLoggerHeader(ret_num,' Step#4-5 KEYWORD 폴더 이동'));
												fs.ensureDir(destPath + customer + '/' + keyword, function(err){
													if (err){
														logger.error(attachLoggerHeader(ret_num,'ERR_ENSURE_DIR_FAILED ' + err));
													}else{
														var jsons = [];
														getFiles(sourceProcessingPath + customer + '/' + keyword, jsons);

														async.eachSeries(jsons, function(json, callback) {
															fs.move(json, json.replace(sourceProcessingPath,destPath), function(err) {
																if(err) {
																	logger.error(attachLoggerHeader(ret_num,'ERR_JSON_MOVE_FAILED ' + err));
																	callback(err);
																} else {
																	//fs.remove
																	callback(null);
																}
															}); //move
														},function(err){
															if (err){
																logger.error(attachLoggerHeader(ret_num,'ERR_KEYWORD_FOLDER_MOVE_FAILED ' + err));
																callback(err);
															}else{
																fs.remove(sourceProcessingPath + customer + '/' + keyword, callback);
															}
														});
														
													}
												});*/


					/*fs.move(sourceProcessingPath + customer + '/' + keyword, sourceEmotionsPath + customer + '/' + keyword, function(err) {
						if(err) {
							logger.error('[documents] ERR_KEYWORD_FOLDER_MOVE_FAILED ' + err);
							callback(err);
						} else {
							callback(null);
						}
					}); //move*/
				}
			}); //waterfall (KEYWORD)
		}, function (err) {
			if (err) {
				callback(err);
			} else {
				//logger.debug(attachLoggerHeader(ret_num, customerKeyword.customer + ' / ' + customerKeyword.keyword + ' Finished!!!'));
				logger.debug(attachLoggerHeader(ret_num, ' Finished!!!'));
				callback(null);
			}
		}); //eachSeries (KEYWORD)
	}; //process

	var attachLoggerHeader = function (ret_num, str) {
		return ('[documentsSaver][' + ret_num + '] ' + str);
	} //attachLoggerHeader

	var getFiles = function (path, files) {
		fs.readdirSync(path).forEach(function (file) {
			var subpath = path + '/' + file;
			if (fs.lstatSync(subpath).isDirectory()) {
				getFiles(subpath, files);
			} else {
				files.push(path + '/' + file);
			}
		}); //readdirSync
	}; //getFiles

	var removeByKey = function (array, params, num) {
		for (var i = 0; i < num; i++) {
			array.some(function (item, index) {
				return (array[index][params.key] === params.value) ? !!(array.splice(index, 1)) : false;
			});
		};
		return array;
	}
	// removeByKey

	return {
		process: process
	};

})();

if (exports) {
	module.exports = documentsSaver;
}
