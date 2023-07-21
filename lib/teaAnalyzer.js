var teaAnalyzer = (function(){
	const logger = require('./logger');
	const config = require('./config');
	const net = require('net');
	const xml = require('xml');
	const xml2js = require('xml2js');
	require('string-format-js');
	
	const teaHost = {
		    host: config.TEA_CONFIG.HOST,
		    port: config.TEA_CONFIG.PORT
		};
	
	// TEA Analysis
	// function teaSocket(title, subtitle, content, term_yn, callback) {
	var teaSocket = function (title, content, term_yn, verb_yn, customer_id,  callback) {
		var buf = new Buffer('');
		
		logger.info("customer_id : "+customer_id);
		var resultXml = '';
		var chunks = '';
		var arrTerms = '';
		var arrTerms2 = '';
		var arrKeyword = new Array();
		
		logger.debug('TEA Analysis : '+title);
		//logger.debug('TEA Analysis : '+content);

		if(content === null) {
			content = '';
		}

		title = title.replace(/</gi,"").replace(/>/gi,"");
		content = content.replace(/</gi,"").replace(/>/gi,"");
	
		var socket = net.createConnection(teaHost, function() {
			socket.setTimeout(1*60*1000);
	        socket.setEncoding('utf8');	
	
			var contents = '';
			contents = contents+'<DOCID>\n';
			contents = contents+'<TITLE>'+title+'\n';
			contents = contents+'<CONTENT>'+content+'\n';
			contents = contents+'<TERMS>\n';
			contents = contents+'<VERB>\n';
			contents = contents+'<TERMS2>\n';
			contents = contents+'<TERMS3>\n';
			contents = contents+'<TOPIC>\n';
			

			logger.info("config.TEA_CONFIG.COLLECTION_TYPE : "+config.TEA_CONFIG.COLLECTION_TYPE)
			var collection_id = 'dmap_data'
			
			for(var i=0; i<config.TEA_CONFIG.COLLECTION_TYPE.length; i++){
			  if(customer_id == config.TEA_CONFIG.COLLECTION_TYPE[i]){
			      collection_id = config.TEA_CONFIG.COLLECTION_TYPE[i];
			      break;
			  }
			  
		    	}	
	
			//logger.info("collection_id : "+collection_id);
			var xmlObject = {
	            request: [
	                {command: 'extractor'},
	                {request_type: 'realtime'},
	                {request_id: '900000'},
	                {params: [
	                    //{param: [{_attr: {name: 'collection_id'}}, 'sample_terms']},					
	                    {param: [{_attr: {name: 'collection_id'}}, collection_id]},
	                    {param: {_attr: {name: 'content'}, _cdata: contents}},
	                    {param: [{_attr: {name: 'item_delimiter'}}, '^']},
	                    {param: [{_attr: {name: 'weight_delimiter'}}, ':']}
	                ]}
	            ]
	        };
	        var xmlString = xml(xmlObject, {declaration: true, indent: '\t'});
	        var xmlLength = Buffer.byteLength(xmlString, 'utf8');
	
	        var header = '12' + '%10d'.format(xmlLength);
	        var packet = header + xmlString;
	        	
	        socket.write(packet);
	    });
	
	    socket.on('data', function(chunk) {
			chunks += chunk.toString();
	    });
	    socket.on('error', function(err) {
			//TODO slack call
			logger.error('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> teaSocket error');
	        logger.error(err);
	    });
	    socket.on('end', function() {
			var buf = new Buffer(chunks.substring(10));
			var parser = new xml2js.Parser();
			var k = 0;
	
			//xml결과
			parser.parseString(buf, function(err, resultData) {
				var status = 'fail';

				if(resultData == null) {
					// logger.debug('### resultData(null): [' + resultData + ']');
				}else {
					// logger.debug('### resultData(notnull): [' + resultData + ']');
					status = resultData.response.results[0].result[0]._;
				}

				// logger.debug('### status ' + status);
				if(status == 'fail') {
					callback(null, arrKeyword);
				} else {
					var data = resultData.response.results[0].result[1]._;
					var terms = data.match(/<TERMS>(.*)/g).toString();
					//logger.info(data);
					terms = terms.replace('<TERMS>',"");
					if (terms.length > 0 && term_yn == 'y') {
						arrTerms = terms.split("^");
						for (var i=0; i < arrTerms.length; i++) {
							var termObj = {topic:"", topic_class:""};
							termObj.topic = arrTerms[i].substring(0, arrTerms[i].indexOf(':'));
							termObj.topic_class = "NN";
							arrKeyword.push(termObj);
							// arrKeyword.push(arrTerms[i].substring(0, arrTerms[i].indexOf(':'))+"|NN");
							// arrKeyword.push(arrTerms[i].substring(0, arrTerms[i].indexOf(':'))+"|NN|브랜드");
						}
					}

					var verbs = data.match(/<VERBS>(.*)/g).toString();
					verbs = verbs.replace('<VERBS>',"");
					if (verbs.length > 0 && verb_yn=='y' ) {
						arrTerms2 = verbs.split("^");
						for (var i=0; i < arrTerms2.length; i++) {
							var termObj = {topic:"", topic_class:""};
							termObj.topic = arrTerms2[i].substring(0, arrTerms2[i].indexOf(':'));
							termObj.topic_class = "VV";
							arrKeyword.push(termObj);
						   // arrKeyword.push(arrTerms2[i].substring(0, arrTerms2[i].indexOf(':'))+"|VV");
						}
					}
					// logger.debug(arrKeyword);
				}		
			});

			callback(null, arrKeyword);
			
			arrTerms = [];
			arrTerms2 = [];
			arrKeyword = [];
	    });	
		
	    socket.on('close', function() {
	        logger.debug('Socket connection closed.');
	    });
	}
	return {
        teaSocket: teaSocket
    };
})();

if (exports) {
	module.exports = teaAnalyzer;
}
