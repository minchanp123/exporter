const fs = require('fs-extra');
const async = require('async');
const moment = require('moment');
const md5 = require('md5');
const logger = require('./logger');
const config = require('./config');
const mariaDBDao = require('../dao/mariaDBDao');

/*
 * document object to json
 */
var jsonMaker = (function () {

	var urlChanger = function (doc) {
		var ret = doc.doc_url;
		var pattern = /.*/g;
		if ((typeof doc.depth1_nm !== 'undefined' && doc.depth1_nm === '포털') || (typeof doc.source !== 'undefined' && (doc.source === '지식 검색' || doc.source === '카페'))) {
			// 일반 포털 검색시 url 포맷 변경
			if (doc.doc_url.startsWith('http://section.cafe.naver.com')) { //네이버 카페 (.*section\.cafe\.naver\.com.*)\&query
				//pattern = /(.*section\.cafe\.naver\.com.*)\&query/g;
				pattern = /(.*section\.cafe\.naver\.com.*\&query)/g;
			} else if (doc.doc_url.startsWith('http://cafe.daum.net')) { //다음 카페 (.*cafe\.daum\.net.*)\?
				//pattern = /(.*cafe\.daum\.net.*\d+)\?q/g;
				pattern = /(.*cafe\.daum\.net.*\d+\?q)/g;
			} else if (doc.doc_url.startsWith('http://kin.naver.com')) { // 네이버 지식인 (.*kin\.naver\.com\/qna\/detail\.nhn\?.*)\&qb
				//pattern = /(.*kin\.naver\.com\/qna\/detail\.nhn\?.*)\&qb/g;
				//pattern = /(.*kin\.naver\.com\/qna\/detail\.nhn\?.*\&qb)/g;
				pattern = /(.*kin\.naver\.com\/.*\/detail\.nhn\?.*\&qb)/g;

			} else if (doc.doc_url.startsWith('http://tip.daum.net')) { // 다음 지식검색 (.*tip\.daum\.net.*)\?q
				//pattern = /(.*tip\.daum\.net.*)\?q/g;
				pattern = /(.*tip\.daum\.net.*\?q)/g;
			}
			ret = pattern.exec(doc.doc_url);
		}

		return (typeof ret[1] === 'undefined' ? doc.doc_url : ret[1]);
	}


	/*
	시간 포맷 변경
	*/
	function datetimeMapper(doc) {
		//Mon May 04 2015 10:05:38 GMT+0900 (대한민국 표준시) => "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
		var date = "";
		if (typeof doc.doc_datetime !== "undefined" && typeof doc.pub_year =="undefined") {
			date = Date.parse(doc.doc_datetime);
			date = moment(date).format('YYYY-MM-DDTHH:mm:ss'); //yyyy-MM-dd HH:mm:ss
			logger.debug('[datetime]2' + date);
		} /*else if (typeof doc.doc_datetime !== "undefined" && doc.doc_datetime.includes('T')) {
			date = doc.doc_datetime;
			logger.debug('[datetime]' + date);
		} */else if (typeof doc.cmt_datetime !== "undefined") { //댓글
			date = Date.parse(doc.cmt_datetime);
			date = moment(date).format('YYYY-MM-DDTHH:mm:ss'); //yyyy-MM-dd HH:mm:ss
		} else { //SCD 일 때
			date = doc.pub_year + '-' + (doc.pub_month.length < 2 ? '0' + doc.pub_month : doc.pub_month) + '-' + (doc.pub_day.length < 2 ? '0' + doc.pub_day : doc.pub_day) + 'T' + doc.pub_time.substring(0, 2) + ":" + doc.pub_time.substring(2, 4) + ':' + doc.pub_time.substring(4, 6);
			/*//TODO cassandra2ES 의 경우 시간 정보 무시, scd2es  사용시 주석 해제?
			 * if (doc.pub_time !== '000000'){
				date += ' ' + doc.pub_time.substring(0,2) + ":"+doc.pub_time.substring(2,4)+':'+doc.pub_time.substring(4,6);
			}*/
		}
		//console.log(date);
		return date;
	}

	//"프리미엄|NN|브랜드" or "안서다|VV"	=> topic Analyzer return 타입 정의 필요
	var IDX_TOPIC_NM = 0;
	var IDX_TOPIC_CLASS = 1;
	var IDX_TOPIC_ATTR = 2;

	/** Elastic Search crawl_doc type의 PK를 생성
	 * @param 수집원문 obj
	 * @return String
	 **/
	var getDocPkStr = function (doc) { //function getDocPkStr(doc){

		var docPkStr = "";
		var doc_datetime = doc.doc_datetime;
		//logger.debug('doc_datetime ' + doc_datetime);

		if (typeof doc_datetime === "undefined" || typeof doc.pub_year !=="undefined") {
			docPkStr = doc.pub_year + doc.pub_month + doc.pub_day + doc.doc_url;
		} else if (!doc_datetime.includes(" ") && doc_datetime.length === 14) { //yyyyMMddhhmiss // TODO 확인
			docPkStr = doc_datetime.substring(0, 4) + doc_datetime.substring(4, 6) + doc_datetime.substring(6, 8) + doc.doc_url;
		} else {
			//var dateArr =doc.doc_datetime.split(" ")[0].split("-"); 
			var dateArr = doc.doc_datetime.split("T")[0].split("-");
			docPkStr = dateArr[0] + dateArr[1] + dateArr[2] + doc.doc_url; //"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
			//logger.debug('[jsonMaker] docPkStr(else) ' + dateArr[0] +'/'+ dateArr[1]+'/'+ dateArr[2] );//2017/07/20T00:00:00
		}
		return docPkStr;
	}

	function getCmtPkStr(cmt) {
		var cmtPkStr = "";
		//_id : "",//( doc_datetime + doc_url + cmt_datetime(yyyyMMddHHmmss) + cmt_writer -> md5 )

	}

	/** Elastic Search custom type의 PK를 생성
	 * emotions, topics 의 PK 는 부모의md5 값(원본string 아님) + 고유값 을 md5 한다.
	 * @param 수집원문 obj, 추가할 pk요소 String
	 * @return String
	 **/
	function getCustomPkStr(doc, str) {
		var custom = "";
		for (var idx = 0; idx < str.length; idx++) {
			custom += str[idx];
		}
		/*		topics
		"topics"+doc_id(as md5)
		emotions
		"emotions"+(ip_port_conceptID as md5)+doc_id(as md5)
		comments
		 (doc_id(as md5)+"comments" -> md5 )*/
		//return md5(getDocPkStr(doc)) + custom;
		return custom + doc.cmt_uuid;
	}


	//TODO
	var scd2collectJson = function (doc, callback) {
		var crawlJson = {
			doc_title: "",
			doc_content: "",
			//doc_datetime : "",
			doc_writer: "",
			doc_url: "",
			img_url: [],
			view_count: 0,
			comment_count: 0,
			like_count: 0,
			dislike_count: 0,
			share_count: 0,
			locations: "",
			source: "",
			uuid: "",
			customer_id: ""
		};

		crawlJson.search_keyword_text = doc.search_keyword_text;

		crawlJson.doc_title = doc.doc_title;
		crawlJson.doc_content = doc.doc_content;
		crawlJson.doc_writer = doc.doc_writer;
		crawlJson.doc_url = doc.doc_url;
		crawlJson.customer_id = doc.customer_id;
		crawlJson.source = doc.source;

		crawlJson.pub_year = doc.pub_year;
		crawlJson.pub_month = doc.pub_month;
		crawlJson.pub_day = doc.pub_day;
		crawlJson.pub_time = doc.pub_time;

		crawlJson.depth1_seq = doc.depth1_seq;
		crawlJson.depth2_seq = doc.depth2_seq;
		crawlJson.depth3_seq = doc.depth3_seq;
		crawlJson.depth1_nm = doc.depth1_nm;
		crawlJson.depth2_nm = doc.depth2_nm;
		crawlJson.depth3_nm = doc.depth3_nm;

		if (typeof doc.comment_count !== "undefined") {
			if (typeof doc.comment_count === 'string')
				doc.comment_count = doc.comment_count.replace(/\,/g, '');
			crawlJson.comment_count = doc.comment_count;
		}
		if (typeof doc.view_count !== "undefined") {
			if (typeof doc.view_count === 'string')
				doc.view_count = doc.view_count.replace(/\,/g, '');
			crawlJson.view_count = doc.view_count;
		}
		if (typeof doc.like_count !== "undefined") {
			if (typeof doc.like_count === 'string')
				doc.like_count = doc.like_count.replace(/\,/g, '');
			crawlJson.like_count = doc.like_count;
		}
		if (typeof doc.dislike_count !== "undefined") {
			if (typeof doc.dislike_count === 'string')
				doc.dislike_count = doc.dislike_count.replace(/\,/g, '');
			crawlJson.dislike_count = doc.dislike_count;
		}
		if (typeof doc.share_count !== "undefined") {
			if (typeof doc.share_count === 'string')
				doc.share_count = doc.share_count.replace(/\,/g, '');
			crawlJson.share_count = doc.share_count;
		}
		if (typeof doc.locations !== "undefined") {
			crawlJson.place = doc.locations; //returnJson.locations = doc.locations;
		}
		if (typeof doc.place !== "undefined") {
			crawlJson.place = doc.place; //returnJson.locations = doc.locations;
		}
		if (typeof doc.doc_second_url !== "undefined") {
			crawlJson.doc_second_url = doc.doc_second_url; //returnJson.locations = doc.locations;
		}

		if (doc.depth2_nm == "트위터") { //트위터의 경우
			crawlJson.twit_id = doc.twit_id;
			crawlJson.twit_writer_id = doc.twit_writer_id;
			crawlJson.twit_rpl_to_statusid = doc.twit_rpl_to_statusid;
			crawlJson.twit_rpl_to_usrid = doc.twit_rpl_to_usrid;
			crawlJson.twit_rpl_to_scr = doc.twit_rpl_to_scr;
		}

		callback(null, crawlJson);

	};


	/** BICA 분석 결과를 Elastic Search crawl_doc_emotion type 으로 변경
	 * @param 수집원문 obj, BICA 결과 ->  
					result = [
						{ 
							bicInfo : {
							}
							, emotions : []
						},
						{ 
							bicInfo : {
							}
							, emotions : []
						}
					]
					
	 * @return String
	 **/
	var getEmotionJson = function (doc, rets, callback) {
		logger.debug('[jsonMaker]  rets.length ' + rets.length);
		var returnJson = {};

		for (var idx = 0; idx < rets.length; idx++) {
			var bicInfo = rets[idx].bicInfo;
			logger.debug(bicInfo);
			var ret = rets[idx].emotions;
			//var ret = rets[idx];
			var emotions = [];
			for (var idx2 = 0; idx2 < ret.length; idx2++) {
				var emotion = {
					emotion_type: "",
					conceptlabel: "",
					lsp: "",
					sentence: {
						string: "",
						offset: 0,
						index: 0
					},
					matched_text: {
						string: "",
						begin: 0,
						end: 0
					},
					variables: [{
						name: "",
						value: ""
					}],
					categories: [{
						label: "",
						entries: ""
					}],
					conceptlevel1: "",
					conceptlevel2: "",
					conceptlevel3: ""
				};

				emotion.emotion_type = ret[idx2].emotion_type;
				emotion.lsp = ret[idx2].lsp;

				var conceptlabel = ret[idx2].info.conceptlabel;
				emotion.conceptlabel = conceptlabel;
				var conceptlabelArr = conceptlabel.replace(/ /g, '').split('>');

				if (conceptlabelArr.length > 1)
					emotion.conceptlevel1 = conceptlabelArr[1].trim();
				if (conceptlabelArr.length > 2)
					emotion.conceptlevel2 = conceptlabelArr[2].trim();
				if (conceptlabelArr.length > 3)
					emotion.conceptlevel3 = conceptlabelArr[3].trim();

				emotion.sentence = ret[idx2].sentence;
				emotion.sentence.string = emotion.sentence.string.replace(/[^\/\+=_!@#\$%\^\-\.\,\;a-zA-Z0-9ㄱ-ㅎ가-힣\s]/gi, "")
											.replace(/(<([^>]+)>)/g, "") // html 태그  제거
											.replace(/[\u200B-\u200D\uFEFF]/g, '')
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
				emotion.matched_text = ret[idx2].matched_text;
				emotion.matched_text.string = emotion.matched_text.string.replace(/[^\/\+=_!@#\$%\^\-\.\,\;a-zA-Z0-9ㄱ-ㅎ가-힣\s]/gi, "")
											.replace(/(<([^>]+)>)/g, "") // html 태그  제거
											.replace(/[\u200B-\u200D\uFEFF]/g, '')
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

				emotion.variables = ret[idx2].variables;
				emotion.categories = ret[idx2].categories;

				emotions.push(emotion);
			}
			var date = datetimeMapper(doc);
			doc.doc_date = date;
			returnJson.doc_datetime = date;
			returnJson.doc_url = doc.doc_url;
			returnJson.doc_id = doc.cmt_uuid;
			returnJson.uuid = doc.uuid;
			returnJson.emotions = emotions;
			returnJson.project_seq = bicInfo.project_seq; ////returnJson.project_seq = rets.project_seq;	 17.08.10 수정		
			returnJson._id = getCustomPkStr(doc, ["emotions_"]) + "_" + bicInfo.project_seq;
			returnJson.emotion_id = returnJson._id;
			returnJson.doc_content = doc.doc_content;
                        returnJson.doc_title = doc.doc_title;
                        returnJson.doc_writer = doc.doc_writer;
                        returnJson.doc_datetime = doc.doc_datetime;
                        returnJson.doc_url = doc.doc_url;
			returnJson.source = doc.source;
		}
		logger.debug("[jsonMaker] Analyzed result Length : " + rets.length + ', datetime' + returnJson.doc_datetime);
		callback(null, returnJson);
	}

	//TODO 댓글 파싱 및 댓글 분석 처리
	/** 댓글 수집분을 Elastic Search comment type으로 변경
	 * @param 수집댓글 obj//, 추가할 pk요소 String
	 * @return object
	 * */
	function getChildren(cmt) {
		var docs = cmt.children;
		var pcmt_id = cmt._id;
	}
	var getCommentJson = function (cmt, callback) {
		var result = [];
		var returnJson = {
			_id: "", //( doc_datetime + doc_url + cmt_datetime(yyyyMMddHHmmss) + cmt_writer -> md5 )
			cmt_content: "",
			cmt_datetime: "", //"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
			cmt_writer: "",
			cmt_url: "",
			emotion_type: "",
			emotion_score: "", //_parent : ""	//doc_id
			//pcmt_id
		};

		var parent = {
			doc_url: cmt.doc_url,
			doc_datetime: cmt.doc_datetime
		};
		returnJson._parent = md5(getDocPkStr(parent));
		cmt.doc_url = undefined;
		cmt.doc_datetime = undefined;

		var date = datetimeMapper(cmt);
		returnJson.cmt_datetime = date;
		returnJson.cmt_writer = cmt.cmt_writer;
		cmt._id = md5(getCustomPkStr(parent, [date, cmt.cmt_writer]));

		//--- TODO 대댓글 처리 재귀함수 호출
		if (cmt.comment_count > 0) {
			getChildren(cmt);
		}
		//returnJson._parent = md5(getDocPkStr(cmt));

		var children_cmt = cmt.children;
		for (var idx in children_cmt) {
			children_cmt[idx]
		}
	}


	/** 수집원문 하나를 Elastic Search crawl_doc type 으로 변경(채널정보 파싱)
	 * 원문 하나를 project_seq 별로 추가하여 object로 변경
	 * @param 수집원문 obj//, 추가할 pk요소 String
	 * @return returnJsonArr
	 **/
	var collectJson2crawlJson = function (doc, callback) {

		var returnJsonArr = [];

		var returnJson = {
			_id: "",
			customer_id: "",
			doc_datetime: "", //"yyyy-MM-ddTHH:mm:ss||yyyy-MM-dd"
			doc_writer: "",
			doc_url: "",
			doc_title: "",
			doc_content: "",
			view_count: 0,
			comment_count: 0,
			like_count: 0,
			dislike_count: 0,
			share_count: 0,
			locations: null, //geo_point
			place: null, //geo_point
			depth1_nm: "",
			depth1_seq: 0,
			depth2_nm: "",
			depth2_seq: 0,
			depth3_nm: "",
			depth3_seq: 0,
			project_seq: 0,
			item_grp_nm: "",
			item_grp_seq: 0,
			item_nm: "",
			item_seq: 0 //,				topics : []
		};

		async.waterfall([
			function (callback) {
				//logger.debug('		collectJson2crawlJson : Step1 채널정보 파싱');
				returnJson.search_keyword_text = doc.search_keyword_text; // 검색어

				if (doc.depth1_seq !== 0 && typeof doc.depth1_seq !== "undefined") { //SCD로부터 파싱 받는 경우 채널선택 스킵
					returnJson.depth1_seq = doc.depth1_seq;
					returnJson.depth2_seq = doc.depth2_seq;
					returnJson.depth3_seq = doc.depth3_seq;
					returnJson.depth1_nm = doc.depth1_nm;
					returnJson.depth2_nm = doc.depth2_nm;
					returnJson.depth3_nm = doc.depth3_nm;
					callback(null, returnJson);
				} else {
					//logger.debug("# 채널정보 파싱");
					mariaDBDao.selectChannelInfo(doc, function (err, res) {
						if (err) {
							if (err === "no_channel_list") {
								logger.error("[jsonMaker] channel parsing error");
								//TODO 테스트 코드 삭제해야함
								callback(null, returnJson);
								//TODO 테스트 코드 삭제해야함
							} else {
								callback(err);
							}
						} else {
							returnJson.depth1_seq = res.depth1_seq;
							returnJson.depth2_seq = res.depth2_seq;
							returnJson.depth3_seq = res.depth3_seq;
							returnJson.depth1_nm = res.depth1_nm;
							returnJson.depth2_nm = res.depth2_nm;
							returnJson.depth3_nm = res.depth3_nm;
							//                      		logger.debug("# 채널정보 파싱 끝");
							callback(null, returnJson);
						}
					});
				}
			},
			function (returnJson, callback) {
				//logger.debug('		collectJson2crawlJson : Step2 원문 정보 채우기');
				var date = datetimeMapper(doc);
				returnJson.doc_datetime = date;
				doc.doc_date = date;
				returnJson.doc_title = doc.doc_title;
				returnJson.doc_content = doc.doc_content;
				returnJson.doc_writer = doc.doc_writer;
				returnJson.doc_url = doc.doc_url;

				if (typeof doc.doc_second_url !== "undefined") { //댓글수집 위한 second_url정보
					returnJson.doc_second_url = doc.doc_second_url;
				}

				if (typeof doc.comment_count !== "undefined") {
					if (typeof doc.comment_count === 'string')
						doc.comment_count = doc.comment_count.replace(/\,/g, '');
					returnJson.comment_count = doc.comment_count;
				}
				if (typeof doc.view_count !== "undefined") {
					if (typeof doc.view_count === 'string')
						doc.view_count = doc.view_count.replace(/\,/g, '');
					returnJson.view_count = doc.view_count;
				}
				if (typeof doc.like_count !== "undefined") {
					if (typeof doc.like_count === 'string')
						doc.like_count = doc.like_count.replace(/\,/g, '');
					returnJson.like_count = doc.like_count;
				}
				if (typeof doc.dislike_count !== "undefined") {
					if (typeof doc.dislike_count === 'string')
						doc.dislike_count = doc.dislike_count.replace(/\,/g, '');
					returnJson.dislike_count = doc.dislike_count;
				}
				if (typeof doc.share_count !== "undefined") {
					if (typeof doc.share_count === 'string')
						doc.share_count = doc.share_count.replace(/\,/g, '');
					returnJson.share_count = doc.share_count;
				}
				if (typeof doc.locations !== "undefined") {
					returnJson.place = doc.locations; //returnJson.locations = doc.locations;
				}
				if (typeof doc.place !== "undefined") {
					returnJson.place = doc.place; //returnJson.locations = doc.locations;
				}
				//----------------------------------------------트윗트랜드 SCD용 정보 추가
				if (typeof doc.twit_writer_id !== "undefined") {
					if (typeof doc.twit_writer_id === 'string') {
						doc.twit_writer_id = (doc.twit_writer_id === 'null' ? null : parseInt(doc.twit_writer_id.replace(/\,/g, '')));
					}
					returnJson.twit_writer_id = doc.twit_writer_id;
				}
				if (typeof doc.twit_rpl_to_scr !== "undefined") {
					returnJson.twit_rpl_to_scr = (doc.twit_rpl_to_scr === 'null' ? null : doc.twit_rpl_to_scr);
				}
				if (typeof doc.twit_rpl_to_usrid !== "undefined") {
					if (typeof doc.twit_rpl_to_usrid === 'string') {
						doc.twit_rpl_to_usrid = (doc.twit_rpl_to_usrid === 'null' ? null : parseInt(doc.twit_rpl_to_usrid.replace(/\,/g, '')));
					}
					returnJson.twit_rpl_to_usrid = doc.twit_rpl_to_usrid;
				}
				if (typeof doc.twit_rpl_to_statusid !== "undefined") {
					if (typeof doc.twit_rpl_to_statusid === 'string') {
						doc.twit_rpl_to_statusid = (doc.twit_rpl_to_statusid === 'null' ? null : parseInt(doc.twit_rpl_to_statusid.replace(/\,/g, '')));
					}
					returnJson.twit_rpl_to_statusid = doc.twit_rpl_to_statusid;
				}
				if (typeof doc.twit_id !== "undefined") {
					if (typeof doc.twit_id === 'string') {
						doc.twit_id = (doc.twit_id === 'null' ? null : parseInt(doc.twit_id.replace(/\,/g, '')));
					}
					returnJson.twit_id = doc.twit_id;
				}

				returnJson._id = md5(getDocPkStr(doc));
				// TODO customer_id : 변경할 부분
				if (doc.customer_id === 'hyundai') {
					returnJson.customer_id = 'innocean';
				} else {
					returnJson.customer_id = doc.customer_id;
					//returnJson.customer_id = '';
				}
				callback(null, returnJson); //채널정보 포함 object 리턴
			},
			function (returnJson, callback) {
				//  logger.debug('		collectJson2crawlJson : Step3 projectSeq 가져오기');
				// mariaDBDao.selectProjectSeq(doc, function(err, res){
				mariaDBDao.selectSearchKeywordInfo(doc, function (err, res) {

					if (err) {
						/*	if(err === "no_project_seq"){
							 logger.debug("no_project_seq");
						 }*/
						callback(err);
					} else {
						if (res.length == 0) {
							err = "no_project_seq";
							callback(err);
							/* returnJson.project_seq = res[0].seq;
							 returnJsonArr.push(returnJson);
							 callback(null);*/
						} else {
							/*
							async.eachSeries(res, function(re, callback) {
								returnJson.project_seq = re.project_seq;
								returnJsonArr.push(returnJson);
								logger.debug(returnJsonArr.length);
								callback(null);
							}, function(err) {
								if(err) {
									callback(err);
								} else {
									logger.debug('		collectJson2crawlJson : Step3 projectSeq 가져오기 끝');
									callback(null, returnJsonArr);
								}
							});
							*/

							//같은 projectSeq 배열이 들어가버리는 현상
							for (var idx in res) {
								var returnJsonTemp = {
									_id: returnJson._id,
									customer_id: returnJson.customer_id,
									doc_datetime: returnJson.doc_datetime, //"yyyy-MM-ddTHH:mm:ss||yyyy-MM-dd"
									doc_writer: returnJson.doc_writer,
									doc_url: returnJson.doc_url,
									doc_second_url: returnJson.doc_second_url,
									doc_title: returnJson.doc_title,
									doc_content: returnJson.doc_content,
									view_count: returnJson.view_count,
									comment_count: returnJson.comment_count,
									like_count: returnJson.like_count,
									dislike_count: returnJson.dislike_count,
									share_count: returnJson.share_count,
									locations: returnJson.locations, //geo_point
									place: returnJson.place, //geo_point
									depth1_nm: returnJson.depth1_nm,
									depth1_seq: returnJson.depth1_seq,
									depth2_nm: returnJson.depth2_nm,
									depth2_seq: returnJson.depth2_seq,
									depth3_nm: returnJson.depth3_nm,
									depth3_seq: returnJson.depth3_seq,
									project_seq: res[idx].project_seq,
									item_grp_nm: returnJson.item_grp_nm,
									item_grp_seq: returnJson.item_grp_seq,
									item_nm: returnJson.item_nm,
									item_seq: returnJson.item_seq //,				topics : []
								};

								//twitter
								if (typeof returnJson.twit_writer_id !== "undefined") {
									returnJsonTemp.twit_writer_id = returnJson.twit_writer_id;
									returnJsonTemp.twit_rpl_to_scr = returnJson.twit_rpl_to_scr;
									returnJsonTemp.twit_rpl_to_usrid = returnJson.twit_rpl_to_usrid;
									returnJsonTemp.twit_rpl_to_statusid = returnJson.twit_rpl_to_statusid;
									returnJsonTemp.twit_id = returnJson.twit_id;
								}

								returnJsonArr.push(returnJsonTemp);
							}

							//logger.debug('		collectJson2crawlJson : Step3 projectSeq 가져오기 끝');
							callback(null);
						}
					}
				});
			}
		], function (err) {
			if (err) {
				callback(err);
			} else {
				callback(null, returnJsonArr);
			}
		});

	};

	//원문의 채널 정보 및 프로젝트 SEQ 정보 부여
	var collectJson2crawlJson_v2 = function (doc, callback) {
		//logger.debug('[jsonMaker] originalDoc=' + JSON.stringify(doc));
		async.waterfall([
			// Step#1 채널정보 파싱 (유효성 검사)
			function (callback) {
				// logger.info('[jsonMaker] Step#1 채널정보 파싱 (유효성 검사)');
				// 채널 정보 존재
				if (doc.depth1_seq !== 0 && typeof doc.depth1_seq !== 'undefined') {
					callback(null);
				} else {
					if (doc.source.includes(' > ')) {
						doc.source = doc.source.split(' > ')[0].trim();
						logger.debug('[jsonMaker]' + doc.source);
					}
					mariaDBDao.selectChannelInfo(doc, function (err, res) {
						if (err) {
							callback(err)
						} else {
							if (res.length === 0) {
								callback('ERR_NO_CHANNEL_LIST');
							} else {
								doc.depth1_seq = res.depth1_seq;
								doc.depth2_seq = res.depth2_seq;
								doc.depth3_seq = res.depth3_seq;
								doc.depth1_nm = res.depth1_nm;
								doc.depth2_nm = res.depth2_nm;
								doc.depth3_nm = res.depth3_nm;

								callback(null);
							}
						}
					});
				}
			},
			//Step#2 원문 정보 채우기
			function (callback) {
				// logger.info('[jsonMaker] Step#2 원문 정보 채우기');
				// 동호회의 경우
				if (doc.depth1_nm === '동호회') {
					// logger.debug('[jsonMaker] 동호회 문서');
					// -----------------전수 수집 포맷 변경에 따라 지정
					doc.search_keyword_text = doc.keyword;
				} else {
					delete doc.search_keyword_text;
				}

				if (doc.depth1_nm === '포털') {
					logger.debug('[jsonMaker] 포털 아닌 경우 url 변경');
					logger.debug('doc.doc_url ' + doc.doc_url);
					doc.doc_url = urlChanger(doc);
					logger.debug('>>>>>>>>>>>>>> doc_url ' + doc.doc_url);
				}

				var date = datetimeMapper(doc);

				if (date.includes('NaN')) {
					callback('ERR_MALFORM_DATETIME');
				} else {
					doc.doc_datetime = date;
				}
				if (typeof doc.project_seq === 'undefined') {
					callback('ERR_NO_PROJECT_SEQ');
				}

				if (typeof doc.comment_count !== 'undefined') {
					if (typeof doc.comment_count === 'string')
						doc.comment_count = doc.comment_count.replace(/\,/g, '');
				}
				if (typeof doc.view_count !== 'undefined') {
					if (typeof doc.view_count === 'string')
						doc.view_count = doc.view_count.replace(/\,/g, '');
				}
				if (typeof doc.like_count !== 'undefined') {
					if (typeof doc.like_count === 'string')
						doc.like_count = doc.like_count.replace(/\,/g, '');
				}
				if (typeof doc.dislike_count !== 'undefined') {
					if (typeof doc.dislike_count === 'string')
						doc.dislike_count = doc.dislike_count.replace(/\,/g, '');
				}
				if (typeof doc.share_count !== 'undefined') {
					if (typeof doc.share_count === 'string')
						doc.share_count = doc.share_count.replace(/\,/g, '');
				}

				if (typeof doc.locations !== 'undefined') {
					doc.place = doc.locations;
				} else {
					doc.locations = '';
					doc.place = '';
				}

				doc._id = md5(getDocPkStr(doc));
				doc.doc_id = doc._id;

				if (typeof doc.second_url !== "undefined") {
					if (typeof doc.second_url === 'string') {
						doc.second_url = (doc.second_url === 'null' ? null : second_url);
					}
				}

				// ----------------------------------------------트윗트랜드 SCD용 정보 추가
				if (typeof doc.twit_writer_id !== "undefined") {
					if (typeof doc.twit_writer_id === 'string') {
						doc.twit_writer_id = (doc.twit_writer_id === 'null' ? null : parseInt(doc.twit_writer_id.replace(/\,/g, '')));
					}
				}
				if (typeof doc.twit_rpl_to_scr !== "undefined") {
					doc.twit_rpl_to_scr = (doc.twit_rpl_to_scr === 'null' ? null : doc.twit_rpl_to_scr);
				}
				if (typeof doc.twit_rpl_to_usrid !== "undefined") {
					if (typeof doc.twit_rpl_to_usrid === 'string') {
						doc.twit_rpl_to_usrid = (doc.twit_rpl_to_usrid === 'null' ? null : parseInt(doc.twit_rpl_to_usrid.replace(/\,/g, '')));
					}
				}
				if (typeof doc.twit_rpl_to_statusid !== "undefined") {
					if (typeof doc.twit_rpl_to_statusid === 'string') {
						doc.twit_rpl_to_statusid = (doc.twit_rpl_to_statusid === 'null' ? null : parseInt(doc.twit_rpl_to_statusid.replace(/\,/g, '')));
					}
				}
				if (typeof doc.twit_id !== "undefined") {
					if (typeof doc.twit_id === 'string') {
						doc.twit_id = (doc.twit_id === 'null' ? null : parseInt(doc.twit_id.replace(/\,/g, '')));
					}
				}
				if (typeof doc.doc_title === "undefined") {
                                	doc.doc_title = doc.source;     
                                }

				// 2018.09.10 특수문자 제거 로직 한번더 추가.
				doc.doc_title = doc.doc_title.replace(/[^_!@#\$\^\-\.\,\;a-zA-Z0-9ㄱ-ㅎ가-힣\s]/gi, " ")
											.replace(/(<([^>]+)>)/g, "") // html 태그  제거
											.replace(/[\u200B-\u200D\uFEFF]/g, '')
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

				doc.doc_content = doc.doc_content.replace(/[^_!@#\$\^\-\.\,\;a-zA-Z0-9ㄱ-ㅎ가-힣\s]/gi, " ")
											.replace(/(<([^>]+)>)/g, "") // html 태그  제거
											.replace(/[\u200B-\u200D\uFEFF]/g, '')
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

				/*doc.doc_url = doc.doc_url.replace(/[^\/_@#\$\^\-\.a-zA-Z0-9ㄱ-ㅎ가-힣]/gi, "")
											.replace(/(<([^>]+)>)/g, "") // html 태그  제거
											.replace(/[\u200B-\u200D\uFEFF]/g, '')
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
											.replace(/[\u001A]+/g, "");*/

				callback(null);
			}
		], function (err) {
			if (err) {
				if (err == "ERR_MALFORM_DATETIME") {
					//datetime에 nan이 포함되어 있는 경우 null 처리
					logger.warn("ERR_MALFORM_DATETIME");
					callback(null, null);
				} else {
					callback(err);
				}
			} else {
				//logger.debug('[jsonMaker] processedDoc=' + JSON.stringify(doc));
				callback(null, doc);
			}
		});

	};
	//원문의 채널 정보 및 프로젝트 SEQ 정보 부여
	var collectJson2crawlJson_v3 = function (doc, callback) {
		//logger.debug('[jsonMaker] originalDoc=' + JSON.stringify(doc));
		async.waterfall([
			// Step#1 채널정보 파싱 (유효성 검사)
			function (callback) {
				// logger.info('[jsonMaker] Step#1 채널정보 파싱 (유효성 검사)');
				// 채널 정보 존재
				if (doc.depth1_seq !== 0 && typeof doc.depth1_seq !== 'undefined') {
					callback(null);
				} else {
					if (doc.source.includes(' > ')) {
						doc.source = doc.source.split(' > ')[0].trim();
						logger.debug('[jsonMaker]' + doc.source);
					}
					mariaDBDao.selectChannelInfo(doc, function (err, res) {
						if (err) {
							callback(err)
						} else {
							if (res.length === 0) {
								callback('ERR_NO_CHANNEL_LIST');
							} else {
								doc.depth1_seq = res.depth1_seq;
								doc.depth2_seq = res.depth2_seq;
								doc.depth3_seq = res.depth3_seq;
								doc.depth1_nm = res.depth1_nm;
								doc.depth2_nm = res.depth2_nm;
								doc.depth3_nm = res.depth3_nm;

								callback(null);
							}
						}
					});
				}
			},
			//Step#2 원문 정보 채우기
			function (callback) {
				// logger.info('[jsonMaker] Step#2 원문 정보 채우기');
				// 동호회의 경우
				if (doc.depth1_nm === '동호회') {
					// logger.debug('[jsonMaker] 동호회 문서');
					// -----------------전수 수집 포맷 변경에 따라 지정
					doc.search_keyword_text = doc.keyword;
				//} else {
				//	delete doc.search_keyword_text;
				//}
				}
				//if (doc.depth1_nm === '포털') {
					//logger.debug('[jsonMaker] 포털 아닌 경우 url 변경');
					//logger.debug('doc.doc_url ' + doc.doc_url);
					//doc.doc_url = urlChanger(doc);
					//logger.debug('>>>>>>>>>>>>>> doc_url ' + doc.doc_url);
				//}

				var date = datetimeMapper(doc);

				if (date.includes('NaN')) {
					callback('ERR_MALFORM_DATETIME');
				} else {
					doc.doc_datetime = date;
				}
				if (typeof doc.project_seq === 'undefined') {
					callback('ERR_NO_PROJECT_SEQ');
				}

				if (typeof doc.comment_count !== 'undefined') {
					if (typeof doc.comment_count === 'string')
						doc.comment_count = doc.comment_count.replace(/\,/g, '');
				}
				if (typeof doc.view_count !== 'undefined') {
					if (typeof doc.view_count === 'string')
						doc.view_count = doc.view_count.replace(/\,/g, '');
				}
				if (typeof doc.like_count !== 'undefined') {
					if (typeof doc.like_count === 'string')
						doc.like_count = doc.like_count.replace(/\,/g, '');
				}
				if (typeof doc.dislike_count !== 'undefined') {
					if (typeof doc.dislike_count === 'string')
						doc.dislike_count = doc.dislike_count.replace(/\,/g, '');
				}
				if (typeof doc.share_count !== 'undefined') {
					if (typeof doc.share_count === 'string')
						doc.share_count = doc.share_count.replace(/\,/g, '');
				}

				if (typeof doc.locations !== 'undefined') {
					doc.place = doc.locations;
				} else {
					doc.locations = '';
					doc.place = '';
				}

				doc._id = doc.cmt_uuid;
				doc.doc_id = doc._id;
				if (typeof doc.second_url !== "undefined") {
					if (typeof doc.second_url === 'string') {
						doc.second_url = (doc.second_url === 'null' ? null : second_url);
					}
				}

				// ----------------------------------------------트윗트랜드 SCD용 정보 추가
				if (typeof doc.twit_writer_id !== "undefined") {
					if (typeof doc.twit_writer_id === 'string') {
						doc.twit_writer_id = (doc.twit_writer_id === 'null' ? null : parseInt(doc.twit_writer_id.replace(/\,/g, '')));
					}
				}
				if (typeof doc.twit_rpl_to_scr !== "undefined") {
					doc.twit_rpl_to_scr = (doc.twit_rpl_to_scr === 'null' ? null : doc.twit_rpl_to_scr);
				}
				if (typeof doc.twit_rpl_to_usrid !== "undefined") {
					if (typeof doc.twit_rpl_to_usrid === 'string') {
						doc.twit_rpl_to_usrid = (doc.twit_rpl_to_usrid === 'null' ? null : parseInt(doc.twit_rpl_to_usrid.replace(/\,/g, '')));
					}
				}
				if (typeof doc.twit_rpl_to_statusid !== "undefined") {
					if (typeof doc.twit_rpl_to_statusid === 'string') {
						doc.twit_rpl_to_statusid = (doc.twit_rpl_to_statusid === 'null' ? null : parseInt(doc.twit_rpl_to_statusid.replace(/\,/g, '')));
					}
				}
				if (typeof doc.twit_id !== "undefined") {
					if (typeof doc.twit_id === 'string') {
						doc.twit_id = (doc.twit_id === 'null' ? null : parseInt(doc.twit_id.replace(/\,/g, '')));
					}
				}
				if (typeof doc.doc_title === "undefined") {
                                	doc.doc_title = doc.source;     
                                }

				// 2018.09.10 특수문자 제거 로직 한번더 추가.
				doc.doc_title = doc.doc_title.replace(/[^_!@#\$\^\-\.\,\;a-zA-Z0-9ㄱ-ㅎ가-힣\s]/gi, " ")
											.replace(/(<([^>]+)>)/g, "") // html 태그  제거
											.replace(/[\u200B-\u200D\uFEFF]/g, '')
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

				doc.doc_content = doc.doc_content.replace(/[^_!@#\$\^\-\.\,\;a-zA-Z0-9ㄱ-ㅎ가-힣\s]/gi, " ")
											.replace(/(<([^>]+)>)/g, "") // html 태그  제거
											.replace(/[\u200B-\u200D\uFEFF]/g, '')
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

				/*doc.doc_url = doc.doc_url.replace(/[^\/_@#\$\^\-\.a-zA-Z0-9ㄱ-ㅎ가-힣]/gi, "")
											.replace(/(<([^>]+)>)/g, "") // html 태그  제거
											.replace(/[\u200B-\u200D\uFEFF]/g, '')
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
											.replace(/[\u001A]+/g, "");*/

				callback(null);
			}
		], function (err) {
			if (err) {
				if (err == "ERR_MALFORM_DATETIME") {
					//datetime에 nan이 포함되어 있는 경우 null 처리
					logger.warn("ERR_MALFORM_DATETIME");
					callback(null, null);
				} else {
					callback(err);
				}
			} else {
				//logger.debug('[jsonMaker] processedDoc=' + JSON.stringify(doc));
				callback(null, doc);
			}
		});

	};
	var collectJson2cmntJson = function (cmt, callback) {
		var returnJson = {
			_id: "",
			cmt_content: "",
			cmt_datetime: "",
			cmt_writer: "",
			cmt_url: "",
			emotion_type: "",
			emotion_score: "" //,_parent:""
		};
		callback(null, returnJson);
	}

	//============================================================== Topic

	/** Elastic Search topics type의 json 생성 
	 * 하나의 object에 하나의 topic 포함 
	 * @param doc : ES crawl_doc type object
	 *@retrun retrurnJson Object array 
	 **/
	var getTopicJson = function (crawl_doc, terms, callback) {
		var returnJson = {};
		if (typeof crawl_doc.project_seq === 'undefined') {
			callback('ERR_NO_PROJECT_SEQ');
		}
		returnJson.project_seq = crawl_doc.project_seq;
		returnJson.topics = terms;
		var date = datetimeMapper(crawl_doc);
		if (date.includes('NaN')) {
			callback('ERR_MALFORM_DATETIME');
		} else {
			returnJson.doc_datetime = date;
		}
		returnJson.doc_url = crawl_doc.doc_url;
		returnJson.doc_id = crawl_doc.cmt_uuid;
		//returnJson._id = md5(getCustomPkStr(crawl_doc,[returnJson.topic]));
		returnJson._id = getCustomPkStr(crawl_doc, ["topics_"]);
		returnJson.topic_id = returnJson._id;
		returnJson.uuid = crawl_doc.uuid;
		//returnJson.doc_url = crawl_doc.doc_url;
 	 	//returnJson.source = crawl_doc.source;
		//returnJson.keyword = crawl_doc.search_keyword_text
		logger.debug(returnJson);

		logger.debug("[jsonMaker] Terms Length : " + terms.length);
		callback(null, returnJson);
	};

	//================================================= Topic

	/** Logstash 로 저장할 json 포맷 만들기
	 * 하나의 object에 하나의 topic 포함 
	 * @param docs : obj array (documents / topics/ emotions 과정을 통해 생성된 array 객체)
	 * @param doc type : 
	 * 	const TYPE_DOCUMENTS = 0;
	 * 	const TYPE_TOPICS = 1;
	 * 	const TYPE_EMOTIONS = 2;
	 * 	const TYPE_COMMENTS = 3;
	 * @return jsonArray
	 **/
	var makeJsonOutput = function (docs, doc_type, callback) {
		var jsonArray = [];
		var jsonObj = {};
		async.waterfall([
			function (callback) {
				if (doc_type === config.DOC_TYPE.TYPE_DOCUMENTS) {
					async.eachSeries(docs, function (doc, callback) {
						// doc_datetime이 없을경우 임의로 1970-01-01로 지정
						if (doc.doc_datetime === undefined) {
							doc.doc_datetime = '1970-01-01T00:00:00';
							logger.info("[jsonMaker] doc_datetime is undefined / " + doc.doc_datetime + " / " + doc.doc_url);
						}

						var objParams = {};
						// 원문일 경우 doc > 하나의 원문
						objParams = {
							index_nm: doc.index_nm = 'documents-' + doc.doc_datetime.substring(0, "yyyy-mm-dd".length).replace(/\-/g, '.'),
							doc_id: doc.doc_id,
							ori_id: doc.uuid,
							doc_datetime: doc.doc_datetime,
							upd_datetime: moment().format('YYYY-MM-DDTHH:mm:ss'), //업데이트 시간 추가
							doc_writer: doc.doc_writer,
							doc_url: doc.doc_url,
							doc_second_url: doc.doc_second_url,
							doc_title: doc.doc_title,
							doc_content: doc.doc_content,
							view_count: doc.view_count,
							comment_count: doc.comment_count,
							like_count: doc.like_count,
							dislike_count: doc.dislike_count,
							share_count: doc.share_count,
							locations: doc.locations,
							place: doc.place,
							depth1_nm: doc.depth1_nm,
							depth1_seq: doc.depth1_seq,
							depth2_nm: doc.depth2_nm,
							depth2_seq: doc.depth2_seq,
							depth3_nm: doc.depth3_nm,
							depth3_seq: doc.depth3_seq,
							project_seq: doc.project_seq,
							my_relations: {
								name: "documents"
							},
							relation_name: 'documents' //relation 특정
						}

						if (doc.depth2_nm === "트위터") { //트위터의 경우					
							objParams.twit_id = doc.twit_id;
							objParams.twit_writer_id = doc.twit_writer_id;
							objParams.twit_rpl_to_statusid = doc.twit_rpl_to_statusid;
							objParams.twit_rpl_to_usrid = doc.twit_rpl_to_usrid;
							objParams.twit_rpl_to_scr = doc.twit_rpl_to_scr;

						} else if (doc.depth1_nm === "동호회" && typeof doc.uuidStr !== 'undefind') { //신규수집기 통한 전수수집의 경우
							//inlineScript += " ctx._source.search_keyword?.addAll(params.search_keyword); Set hs2 = new HashSet(); hs2.addAll(ctx._source.search_keyword); ctx._source.search_keyword.clear(); ctx._source.search_keyword.addAll(hs2); ";
							objParams.search_keyword = [doc.search_keyword_text];
						}

						delete objParams.uuid;
						delete objParams.uuidStr;
						jsonArray.push(objParams);
						// logger.debug(objParams);
						callback(null);
					}, callback);
				} else {
					if (docs.doc_datetime === 'Invalid date') {
                        	                docs.doc_datetime = '1970-01-01T00:00:00';
						logger.info('[jsonMaker] doc_datetime is Invalid date ' + docs.doc_datetime);
                                        }

					var objParams = {};
					// 화제어일 경우
					if (doc_type === config.DOC_TYPE.TYPE_TOPICS) {
						objParams = {
							index_nm: docs.index_nm = 'documents-' + docs.doc_datetime.substring(0, "yyyy-mm-dd".length).replace(/\-/g, '.'),
							topic_id: docs.topic_id,
 							ori_id: docs.uuid,
							routing: docs.doc_id,
							topic_id: docs.topic_id,
							topics: docs.topics,
							doc_id: docs.doc_id,
							doc_url: docs.doc_url,
							source: docs.source,
							keyword: docs.keyword,
							upd_datetime: moment().format('YYYY-MM-DDTHH:mm:ss'), //업데이트 시간 추가
							my_relations: {
								name: "topics",
								parent: docs.doc_id
							},
							project_seq: docs.project_seq,
							relation_name: 'topics'
						}
						// 감정일 경우
					} else if (doc_type === config.DOC_TYPE.TYPE_EMOTIONS) {
						logger.debug('[jsonMaker/makeJsonOutput] TYPE_EMOTIONS');
						objParams = {
							index_nm: docs.index_nm = 'documents-' + docs.doc_datetime.substring(0, "yyyy-mm-dd".length).replace(/\-/g, '.'),
							routing: docs.doc_id,
							ori_id: docs.uuid,
							emotion_id: docs.emotion_id,
							emotions: docs.emotions,
							my_relations: {
								name: "emotions",
								parent: docs.doc_id
							},
							project_seq: docs.project_seq,
							relation_name: 'emotions',
							doc_content: docs.doc_content,
							doc_title: docs.doc_title,
							doc_writer: docs.doc_writer,
							doc_datetime: docs.doc_datetime,
							doc_url: docs.doc_url,
							source: docs.source
						};
						// 댓글일 경우
					} else if (doc_type === config.DOC_TYPE.TYPE_COMMENTS) {}
					jsonObj = objParams;
					callback(null);
				}
			}
		], function (err) {
			if (err) {
				callback(err);
			} else {
				callback(null, (doc_type === config.DOC_TYPE.TYPE_DOCUMENTS ? jsonArray : jsonObj));
			}
		});
	}

	var writeJsonFile = function (dirPath, docsArray, doc_type, callback) {
		const nowString = moment().format("YYYYMMDDHHmm-ssSSS");
		// TODO 파일 이름 및 폴더 체계 생각필요
		const jsonFileName =
			(doc_type === config.DOC_TYPE.TYPE_DOCUMENTS ? "D" : (doc_type === config.DOC_TYPE.TYPE_TOPICS ? "T" : doc_type === config.DOC_TYPE.TYPE_EMOTIONS ? "E" : "DC")) +
			"-00-" + nowString + ".json.swap";
		fs.ensureDir(dirPath, function (err) {
			if (err) {
				callback(err);
			} else {
				var returnName = dirPath + '/' + jsonFileName;
				fs.writeFile(returnName, JSON.stringify(docsArray) + "\n", function (err) {
					if (err) {
						logger.error(err);
						callback(err);
					} else {
						callback(null, returnName);
					}
				});
			}
		});
	};

	var makeJsonConversison = function (doc, callback) {
		var objParams = {};
		objParams = {
			doc_id: doc.doc_id,
			doc_title: doc.doc_title,
			doc_content: doc.doc_content,
			doc_writer: doc.doc_writer,
			doc_url: doc.doc_url,
			img_url: [],
			view_count: 0,
			comment_count: 0,
			like_count: 0,
			dislike_count: 0,
			share_count: 0,
			locations: "",
			source: "",
			uuid: "",
			customer_id: doc.customer_id, //doc.customer_id
			search_keyword_text: doc.search_keyword_text, //doc.ssearch_keyword_text
			pub_year: doc.yyyy,
			pub_month: doc.mm,
			pub_day: doc.dd,
			pub_time: doc.tt,
			depth1_seq: doc.depth1_cd,
			depth2_seq: doc.depth2_cd,
			depth3_seq: doc.depth3_cd,
			depth1_nm: doc.depth1_nm,
			depth2_nm: doc.depth2_nm,
			depth3_nm: doc.depth3_nm,
			doc_second_url: "null"
		}
		callback(null, objParams);
	}

	var makeJsonOld_new = function (doc, buffer, callback) {
		logger.info("doc_id : " + doc.doc_id);
		var objParams = {};
		objParams = {
			doc_id: doc.doc_id,
			doc_title: doc.doc_title,
			doc_content: buffer.toString(),
			doc_writer: doc.doc_writer,
			doc_url: doc.doc_url,
			img_url: [],
			view_count: 0,
			comment_count: 0,
			like_count: 0,
			dislike_count: 0,
			share_count: 0,
			locations: "",
			source: "",
			uuid: "",
			customer_id: doc.customer_id, //doc.customer_id
			search_keyword_text: doc.search_keyword_text, //doc.ssearch_keyword_text
			pub_year: doc.yyyy,
			pub_month: doc.mm,
			pub_day: doc.dd,
			pub_time: doc.tt,
			depth1_seq: doc.depth1_cd,
			depth2_seq: doc.depth2_cd,
			depth3_seq: doc.depth3_cd,
			depth1_nm: doc.depth1_nm,
			depth2_nm: doc.depth2_nm,
			depth3_nm: doc.depth3_nm,
			doc_second_url: "null"
		}
		callback(null, objParams);
	}

	var writeJsonFile_at = function (dirPath, docsArray, callback) {
		const dateString = moment().format("YYYYMMDDHHmm-ssSSS");
		const scdString = 'B-AT-' + dateString + '-00000-I-C';
		// TODO 파일 이름 및 폴더 체계 생각필요
		const jsonFileName = "D-00-" + dateString + ".json";

		logger.info("jsonFileName : " + jsonFileName);
		fs.ensureDir(dirPath + scdString, function (err) {
			if (err) {
				callback(err);
			} else {
				var returnName = dirPath + '/' + scdString + '/' + jsonFileName;
				logger.info("returnName : " + returnName);
				fs.writeFile(returnName, JSON.stringify(docsArray) + "\n", function (err) {
					if (err) {
						logger.error(err);
						callback(err);
					} else {
						callback(null, returnName);
					}
				});
			}
		});
	};

	return {
		urlChanger: urlChanger,
		datetimeMapper: datetimeMapper,
		getDocPkStr: getDocPkStr,
		scd2collectJson: scd2collectJson,
		collectJson2crawlJson: collectJson2crawlJson,
		collectJson2crawlJson_v2: collectJson2crawlJson_v2,
		collectJson2crawlJson_v3: collectJson2crawlJson_v3,
		getCommentJson: getCommentJson,
		getTopicJson: getTopicJson,
		getEmotionJson: getEmotionJson,
		makeJsonOutput: makeJsonOutput,
		writeJsonFile: writeJsonFile,

		makeJsonConversison: makeJsonConversison,
		writeJsonFile_at: writeJsonFile_at,
		makeJsonOld_new: makeJsonOld_new
	};

})();

if (exports) {
	module.exports = jsonMaker;
}
