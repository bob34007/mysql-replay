/*******************************************************************************
 * Copyright (c)  2021 PingCAP, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/

/**
 * @Author: guobob
 * @Description:
 * @File:  sqlreplay_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/10 10:21
 */

package sqlreplay

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/agiledragon/gomonkey"
	"github.com/bobguo/mysql-replay/stream"
	"github.com/bobguo/mysql-replay/util"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

var logger *zap.Logger


func init (){
	cfg := zap.NewDevelopmentConfig()
	//cfg.Level = zap.NewAtomicLevelAt()
	cfg.DisableStacktrace = !cfg.Level.Enabled(zap.DebugLevel)
	logger, _ = cfg.Build()
	zap.ReplaceGlobals(logger)
	logger = zap.L().With(zap.String("conn","test-mysql.go"))
	logger = logger.Named("test")
}

func TestReplayEventHandler_GenerateNextFileName(t *testing.T) {
	type fields struct {
		pconn                       stream.ConnID
		dsn                         string
		fsm                         *stream.MySQLFSM
		log                         *zap.Logger
		MySQLConfig                 *mysql.Config
		schema                      string
		pool                        *sql.DB
		conn                        *sql.Conn
		stmts                       map[uint64]statement
		ctx                         context.Context
		filterStr                   string
		needCompareRes              bool
		needCompareExecTime         bool
		rrLastGetCheckPointTime     time.Time
		rrCheckPoint                time.Time
		rrGetCheckPointTimeInterval int64
		rrNeedReplay                bool
		once                        *sync.Once
		ch                          chan stream.MySQLEvent
		wg                          *sync.WaitGroup
		file                        *os.File
		wf                          *WriteFile
		fileNamePrefix              string
		fileName                    string
		filePath                    string
		storePath                   string
		preFileSize                 uint64
		pos                         uint64
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "get next file name ",
			fields: fields{
				fileNamePrefix: "127.0.0.1:4000",
			},
			want: "127.0.0.1:4000-2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &ReplayEventHandler{
				pconn:                       tt.fields.pconn,
				dsn:                         tt.fields.dsn,
				fsm:                         tt.fields.fsm,
				log:                         tt.fields.log,
				MySQLConfig:                 tt.fields.MySQLConfig,
				schema:                      tt.fields.schema,
				pool:                        tt.fields.pool,
				conn:                        tt.fields.conn,
				stmts:                       tt.fields.stmts,
				ctx:                         tt.fields.ctx,
				filterStr:                   tt.fields.filterStr,
				needCompareRes:              tt.fields.needCompareRes,
				needCompareExecTime:         tt.fields.needCompareExecTime,
				rrLastGetCheckPointTime:     tt.fields.rrLastGetCheckPointTime,
				rrCheckPoint:                tt.fields.rrCheckPoint,
				rrGetCheckPointTimeInterval: tt.fields.rrGetCheckPointTimeInterval,
				//rrNeedReplay:                tt.fields.rrNeedReplay,
				once:                        tt.fields.once,
				ch:                          tt.fields.ch,
				wg:                          tt.fields.wg,
				file:                        tt.fields.file,
				wf:                          tt.fields.wf,
				fileNamePrefix:              tt.fields.fileNamePrefix,
				fileName:                    tt.fields.fileName,
				filePath:                    tt.fields.filePath,
				storePath:                   tt.fields.storePath,
				preFileSize:                 tt.fields.preFileSize,
				pos:                         tt.fields.pos,
			}
			if got := h.GenerateNextFileName(); got != tt.want {
				t.Errorf("GenerateNextFileName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFileNameSeq_getNextFileNameSuffix(t *testing.T) {
	tests := []struct {
		name string
		fs   util.FileNameSeq
		want string
	}{
		{
			name:"get file name suffix ",
			fs : util.FileNameSeq(2),
			want: "-3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.fs.GetNextFileNameSuffix(); got != tt.want {
				t.Errorf("getNextFileNameSuffix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOpenNexFile_With_OpenFile_Fail(t *testing.T){
	r := new(ReplayEventHandler)
	r.filePath = "./"
	r.fileNamePrefix ="127.0.0.1:4000"
	err := errors.New("do not have privileges")
	patch := gomonkey.ApplyFunc(util.OpenFile, func (path, fileName string) (*os.File,error){
		return nil,err
	})
	defer patch.Reset()

	err1:=r.OpenNextFile()

	ast:=assert.New(t)
	ast.Nil(r.file)
	ast.Equal(err,err1)
}

func TestOpenNexFile_With_OpenFile_Succ(t *testing.T){
	r := new(ReplayEventHandler)
	r.filePath = "./"
	r.fileNamePrefix ="127.0.0.1:4000"
	patch := gomonkey.ApplyFunc(util.OpenFile, func (path, fileName string) (*os.File,error){
		return new(os.File),nil
	})
	defer patch.Reset()

	err1:=r.OpenNextFile()
	ast:=assert.New(t)
	ast.Equal(r.pos,uint64(0))
	ast.Nil(err1)
}

func TestCheckIfChangeFile_With_timeout_True(t *testing.T){
	r := new(ReplayEventHandler)
	r.fileOpenTime=time.Now().Add(15*time.Minute)
	r.pos =100
	r.preFileSize=90
	b:=r.CheckIfChangeFile()
	assert.New(t).True(b)
}

func TestCheckIfChangeFile_With_True(t *testing.T){
	r := new(ReplayEventHandler)
	r.pos =100
	r.preFileSize=90
	b:=r.CheckIfChangeFile()
	assert.New(t).True(b)
}

func TestCheckIfChangeFile_With_False(t *testing.T){
	r := new(ReplayEventHandler)
	r.fileOpenTime = time.Now()
	r.pos =100
	r.preFileSize=110
	b:=r.CheckIfChangeFile()
	assert.New(t).False(b)
}

func TestCloseAndBackupFile_With_Sync_Fail(t *testing.T){
	r := new(ReplayEventHandler)
	r.file = new(os.File)
	r.filePath="./"
	r.storePath="./"
	r.fileNamePrefix="127.0.0.1:4000"
	r.fileName=r.GenerateNextFileName()
	err := errors.New("has no space left")
	patches := gomonkey.ApplyMethod(reflect.TypeOf(r.file), "Sync",
		func(_ *os.File) error {
			return err
		})
	defer patches.Reset()

	err1 := r.CloseAndBackupFile()

	assert.New(t).Equal(err,err1)
}

func TestCloseAndBackupFile_With_Close_Fail(t *testing.T){
	r := new(ReplayEventHandler)
	r.file = new(os.File)
	r.filePath="./"
	r.storePath="./"
	r.fileNamePrefix="127.0.0.1:4000"
	r.fileName=r.GenerateNextFileName()
	err := errors.New("close file fail")
	patches := gomonkey.ApplyMethod(reflect.TypeOf(r.file), "Sync",
		func(_ *os.File) error {
			return nil
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(r.file), "Close",
		func(_ *os.File) error {
			return err
		})
	defer patches1.Reset()

	err1 := r.CloseAndBackupFile()

	assert.New(t).Equal(err,err1)
}

func TestCloseAndBackupFile_With_storePath_Zero(t *testing.T){
	r := new(ReplayEventHandler)
	r.file = new(os.File)
	r.filePath="./"
	r.storePath=""
	r.fileNamePrefix="127.0.0.1:4000"
	r.fileName=r.GenerateNextFileName()

	patches := gomonkey.ApplyMethod(reflect.TypeOf(r.file), "Sync",
		func(_ *os.File) error {
			return nil
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(r.file), "Close",
		func(_ *os.File) error {
			return nil
		})
	defer patches1.Reset()

	err1 := r.CloseAndBackupFile()

	assert.New(t).Nil(err1)
}

func TestCloseAndBackupFile_With_Rename_Fail(t *testing.T){
	r := new(ReplayEventHandler)
	r.file = new(os.File)
	r.filePath="./"
	r.storePath="./"
	r.fileNamePrefix="127.0.0.1:4000"
	r.fileName=r.GenerateNextFileName()

	patches := gomonkey.ApplyMethod(reflect.TypeOf(r.file), "Sync",
		func(_ *os.File) error {
			return nil
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(r.file), "Close",
		func(_ *os.File) error {
			return nil
		})
	defer patches1.Reset()

	err :=errors.New("do not have privileges")
	patch := gomonkey.ApplyFunc(os.Rename, func (oldpath string, newpath string) error{
		return err
	})
	defer patch.Reset()

	err1 := r.CloseAndBackupFile()

	assert.New(t).Equal(err1,err)
}

func TestCloseAndBackupFile_With_Rename_Succ(t *testing.T){
	r := new(ReplayEventHandler)
	r.file = new(os.File)
	r.filePath="./"
	r.storePath=""
	r.fileNamePrefix="127.0.0.1:4000"
	r.fileName=r.GenerateNextFileName()

	patches := gomonkey.ApplyMethod(reflect.TypeOf(r.file), "Sync",
		func(_ *os.File) error {
			return nil
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(r.file), "Close",
		func(_ *os.File) error {
			return nil
		})
	defer patches1.Reset()

	//err :=errors.New("do not have privileges")
	patch := gomonkey.ApplyFunc(os.Rename, func (oldpath string, newpath string) error{
		return nil
	})
	defer patch.Reset()

	err1 := r.CloseAndBackupFile()

	assert.New(t).Nil(err1)
}

func TestNewWriteFile(t *testing.T){

	wf := NewWriteFile()
	assert.New(t).NotNil(wf)
}

func TestNewReplayEventHandler(t *testing.T){
	var conn stream.ConnID
	cfg :=&util.Config{}
	log :=logger
	cfg.Dsn ="root:glb34007@tcp(172.16.5.189:4000)/TPCC"
	cfg.MySQLConfig =new( mysql.Config)
	cfg.OutputDir ="./"
	cfg.StoreDir ="./"
	cfg.PreFileSize = uint64(100)

	r := NewReplayEventHandler(conn ,log , cfg )
	assert.New(t).NotNil(r)
}

func Test_WriteEvent_Marshal_fail(t *testing.T){
	e := stream.MySQLEvent{
		Type: util.EventStmtPrepare,
		Query: "select * from t where id > ?;",
		Time : time.Now().Unix(),
		DB:"test",
	}

	h :=&ReplayEventHandler{
		log:zap.L().Named("test"),
	}
	err:=errors.New("marshal struct fail")
	patch := gomonkey.ApplyFunc(json.Marshal, func (v interface{}) ([]byte, error){
		return nil,err

	})
	defer patch.Reset()

	h.WriteEvent(e)

}

func Test_WriteEvent_Marshal_succ(t *testing.T){
	e := stream.MySQLEvent{
		Type: util.EventStmtPrepare,
		Query: "select * from t where id > ?;",
		Time : time.Now().Unix(),
		DB:"test",
	}

	h :=&ReplayEventHandler{
		log:zap.L().Named("test"),
	}


	h.WriteEvent(e)

}

func Test_DoEvent_EventStmtPrepare(t *testing.T){
	e := stream.MySQLEvent{
		Type: util.EventStmtPrepare,
		Query: "select * from t where id > ?;",
		Time : time.Now().Unix(),
		DB:"test",
	}
	h :=&ReplayEventHandler{
		log:zap.L().Named("test"),
	}
	patches := gomonkey.ApplyMethod(reflect.TypeOf(h), "ReplayEventAndWriteRes",
		func(_ *ReplayEventHandler,e stream.MySQLEvent) {
			return
		})
	defer patches.Reset()

	h.DoEvent(e)

}

func Test_DoEvent_NotWriteLog(t *testing.T){
	e := stream.MySQLEvent{
		Type: util.EventQuery,
		Query: "select * from t where id > ?;",
		Time : time.Now().Unix(),
		DB:"test",
	}

	h :=&ReplayEventHandler{
		log:zap.L().Named("test"),
	}
	cfg :=&util.Config{}
	patches := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckNeedReplay",
		func(_ *util.Config,ts int64) uint16 {
			return util.NotWriteLog
		})
	defer patches.Reset()

	h.DoEvent(e)
}


func Test_DoEvent_NeedWriteLog(t *testing.T){
	e := stream.MySQLEvent{
		Type: util.EventQuery,
		Query: "select * from t where id > ?;",
		Time : time.Now().Unix(),
		DB:"test",
	}

	h :=&ReplayEventHandler{
		log:zap.L().Named("test"),
	}
	cfg :=&util.Config{}
	patches := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckNeedReplay",
		func(_ *util.Config,ts int64) uint16 {
			return util.NeedWriteLog
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(h), "WriteEvent",
		func(_ *ReplayEventHandler,e stream.MySQLEvent)  {
			return
		})
	defer patches1.Reset()

	h.DoEvent(e)

}

func Test_DoEvent_NeedReplaySQL(t *testing.T){
	e := stream.MySQLEvent{
		Type: util.EventQuery,
		Query: "select * from t where id > ?;",
		Time : time.Now().Unix(),
		DB:"test",
	}

	h :=&ReplayEventHandler{
		log:zap.L().Named("test"),
	}
	cfg :=&util.Config{}
	patches := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckNeedReplay",
		func(_ *util.Config,ts int64) uint16 {
			return util.NeedReplaySQL
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(h), "ReplayEventAndWriteRes",
		func(_ *ReplayEventHandler,e stream.MySQLEvent)  {
			return
		})
	defer patches1.Reset()

	h.DoEvent(e)

}

func Test_DoEvent_Unknown(t *testing.T){
	e := stream.MySQLEvent{
		Type: util.EventQuery,
		Query: "select * from t where id > ?;",
		Time : time.Now().Unix(),
		DB:"test",
	}

	h :=&ReplayEventHandler{
		log:zap.L().Named("test"),
	}
	cfg :=&util.Config{}
	patches := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckNeedReplay",
		func(_ *util.Config,ts int64) uint16 {
			return uint16(10)
		})
	defer patches.Reset()

	h.DoEvent(e)

}

func Test_OnClose(t *testing.T){
	wf := NewWriteFile()
	h:=&ReplayEventHandler{
		ch : make(chan stream.MySQLEvent,100),
		wf:wf,
		wg : new(sync.WaitGroup),
	}
	h.OnClose()
}

