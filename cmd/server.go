/**
 * @Author: guobob
 * @Description:
 * @File:  server.go
 * @Version: 1.0.0
 * @Date: 2021/11/12 10:28
 */

package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/bobguo/mysql-replay/util"
	"go.uber.org/zap"
	"net/http"
	"os"
	"time"
)

var logger = zap.L().Named("server")

var startTime = time.Now()

//writing file path
var outDir string
//write done and change restore file path
var strDir string

func generateListenStr(port uint16) string {

	return "0.0.0.0"+":"+fmt.Sprintf("%v",port)

}



type QueryStats struct {
	RunTime int64 `json:"runtime"`
	FileNameSeqNO int64 `json:"file_name_seq_no"`
	WriteDoneFileNum int64 `json:"write_end_file_num"`
	WriteDoneFileSize int64 `json:"write_end_file_size"`
	WritingFileNum  int64 `json:"writing_file_num"`
	WritingFileSize int64 `json:"writing_file_size"`
}


func HandleQueryStats(w http.ResponseWriter, r *http.Request) {
	logger.Info("request query stats from " + r.Host )
	defer logger.Info("response query stats to " + r.Host)
	var err error
	qs := new(QueryStats)
	qs.RunTime = int64(time.Since(startTime).Seconds())
	qs.FileNameSeqNO = util.GetFileNameSeq()
	qs.WriteDoneFileNum ,err  = util.GetFileNumFromPath(strDir)
	if err !=nil{
		_,err = w.Write([]byte(err.Error()))
		if err !=nil{
			logger.Warn("write response file,"+err.Error())
		}
		return
	}
	qs.WriteDoneFileSize,err = util.GetFileSizeFromPath(strDir)
	if err !=nil{
		_,err = w.Write([]byte(err.Error()))
		if err !=nil{
			logger.Warn("write response file,"+err.Error())
		}
		return
	}
	qs.WritingFileNum,err = util.GetFileNumFromPath(outDir)
	if err !=nil{
		_,err = w.Write([]byte(err.Error()))
		if err !=nil{
			logger.Warn("write response file,"+err.Error())
		}
	}
	qs.WritingFileSize,err = util.GetFileSizeFromPath(outDir)
	if err !=nil{
		_,err = w.Write([]byte(err.Error()))
		if err !=nil{
			logger.Warn("write response file,"+err.Error())
		}
	}
	js , err:= json.Marshal(qs)
	if err !=nil{
		_,err = w.Write([]byte(err.Error()))
		if err !=nil{
			logger.Warn("write response file,"+err.Error())
		}
	} else {
		_,err = w.Write(js)
		if err !=nil{
			logger.Warn("write response file,"+err.Error())
		}
	}

}

func HandleExit(w http.ResponseWriter, r *http.Request){
	logger.Info("request exit from " + r.Host )
	defer logger.Info("response exit to " + r.Host )
	logger.Info("receive exit message from ")
	_,err := w.Write([]byte("ok!"))
	if err !=nil {
		logger.Warn("write response file," + err.Error())
	}
	os.Exit(0)
}

func AddPortListenAndServer(port uint16,outputDir ,storeDir string ){

	outDir = outputDir
	strDir = storeDir

	http.HandleFunc("/stats", HandleQueryStats)
	http.HandleFunc("/exit", HandleExit)

	err:= http.ListenAndServe(generateListenStr(port), nil)
	if err !=nil{
		logger.Warn(fmt.Sprintf("listen port:%v fail ,%v",port,err.Error()))
	}

}
