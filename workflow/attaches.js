const { exec } = require('child_process');
const logger = require('../lib/logger');
var async = require('async');
var fs = require('fs-extra');
//const sn3fPath ='C:/SF_1/v_5/bin/';

const conversion = (function() {
    const process_con = function(processCallback) {

        //국가보고서(프리즘)_작업/수목원_작업/0
        //국가보고서(프리즘)_작업/식물원_작업/1
        //국내학술지_작업/2
        //국립수목원 간행물_작업/국립수목원간행물_작업/3
        //국립수목원 간행물_작업/연구개발사업보고서_작업/4
        //푸른누리_작업/5
        //학위논문_작업/6

        var folder_Path = ['국가보고서(프리즘)_작업/수목원_작업/','국가보고서(프리즘)_작업/식물원_작업/','국내학술지_작업/','국립수목원간행물_작업/국립수목원간행물_작업/','국립수목원간행물_작업/연구개발사업보고서_작업/','푸른누리_작업/','학위논문_작업/']
        var mariaDBDao = require('../dao/mariaDBDao');
        var sn3fexe = require('./sn3fExec');
        //var filePath_de ='C:/sourceTree/dmap-exporter/';
        var filePath_done = './attache_done/';
        var attachesPath ='./attaches/'; 
     
             
                           
        async.waterfall([
            // function(ex1){
            //     for(var i=0; i<folder_Path.length; i++){
            //         fs.readdirSync(attachesPath+folder_Path[i]).forEach(function(file) {
            //             //logger.info("file name : "+attachesPath+'국가보고서(프리즘)_작업/수목원_작업/'+file);
            //             var reurl = "";
            //             var allPath =attachesPath+folder_Path[i]+file;
            //             var reurl = attachesPath+folder_Path[i];
            //             var doc_id = 'KNA_'+file.substring(0,5);

            //             const sp= file.split(" ");

            //             if(sp.length>1){
            //                 for(var j=0; j<sp.length; j++){
            //                     reurl +=sp[j];
            //                 }
            //                 logger.info("reurl: "+reurl);
            //                 fs.move(allPath,reurl);
            //                 allPath = reurl;
            //             }
                        
            //             logger.info("allPath : "+allPath+"\ndoc_id : "+doc_id);
            //             // mariaDBDao.updateAttache_report_at(doc_id,allPath,function(err){
            //             //     if(err){
            //             //     }
            //             // });
                        
            //         });
            //          if(i==folder_Path.length-1){
            //              ex1(null);
            //          }
            //     }
            // },
            function(ex2){
            //var result;
                logger.info("[attaches]#ex2 db 접속 data select");
                mariaDBDao.selectConversion(function(err,datas){
                            
                    if(err ||datas.length == 0){

                        if(datas.length == 0){
                            logger.info("[attaches]#ex2-1 DB PATH length 0...");
                            err = 'DB PATH length 0';
                        }
                        ex2(err);
                     }else {
                        logger.info("db data length : "+datas.length);

                        
                        ex2(null,datas);
                    }
                    //logger.info('resultCon = '+result.length);
                            
                });
                
            },function(data,ex3){
                logger.info('[attaches]#ex2-2 txt로 파일변환 및 가져오기');
                var i = 0;
                //var index = result.length;
                //logger.info(result[0].filePath);
                fs.ensureDir(filePath_done, function(err){
                    sn3fexe.process_exec(data,function(err,stdout){ //sn3f 실행 및 json 생성
                        if(err){
                            ex3(err);
                        }else{
                            ex3(null);
                        }
                    });
                });
                        
            
                }],function(err){
                    if(err){
                        processCallback(err);
                    }else{
                        processCallback(null);
                    }
        });

    };

    return {
        process_con: process_con//,
        //process: process
    };
     
        
})();

if (exports) {
    module.exports = conversion;
  }
