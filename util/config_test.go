/*
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
 */

/**
 * @Author: guobob
 * @Description:
 * @File:  config_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/26 14:34
 */

package util

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/agiledragon/gomonkey"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"reflect"
	"sync"
	"testing"
	"time"
)

func Test_tryConnectDstDB(t *testing.T) {
	cfg :=&Config{
		Dsn:"root:glb34007@tcp(127.0.0.1:3306)/mysql",
		Log:zap.L().Named("test"),
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("mock error: '%s' ", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("select count(*)")
	mock.ExpectCommit().WillReturnError(&mysql.MySQLError{
		Number: 1146,
		Message: "Table 'mysql.users' doesn't exist",
	})

	cfg.MySQLConfig,_=mysql.ParseDSN(cfg.Dsn)
	err = cfg.TryConnectDstDB()
	assert.New(t).NotNil(err)
}


func Test_CheckDsn_DSN_NIl(t *testing.T) {
	cfg:=&Config{
		Dsn:"",
		Log:zap.L().Named("test"),
	}

	err:= cfg.CheckDsn()

	assert.New(t).NotNil(err)
}

func Test_CheckDsn_DSN_Parse_Fail(t *testing.T) {
	cfg:=&Config{
		Dsn:",",
		Log:zap.L().Named("test"),
	}

	err:= cfg.CheckDsn()

	assert.New(t).NotNil(err)
}


func Test_CheckDsn_DSN_Parse_Succ(t *testing.T) {
	cfg :=&Config{
		Dsn:"root:glb34007@tcp(127.0.0.1:3306)/mysql",
		Log:zap.L().Named("test"),
	}

	err:= cfg.CheckDsn()

	assert.New(t).Nil(err)
}

func Test_CheckStoreDir_Online_Store_len_zero(t *testing.T){
	cfg :=&Config{
		StoreDir: "",
		RunType: RunOnline,
		Log:zap.L().Named("test"),
	}

	err:=cfg.CheckStoreDir()
	assert.New(t).NotNil(err)
}

func Test_CheckStoreDir_Online_Store_err(t *testing.T){
	cfg :=&Config{
		StoreDir: "./",
		RunType: RunOnline,
		Log:zap.L().Named("test"),
	}

	err := errors.New("do not have privileges")
	patch := gomonkey.ApplyFunc(CheckDirExistAndPrivileges, func(path string) (bool,error){
		return false,err
	})
	defer patch.Reset()

	err=cfg.CheckStoreDir()
	assert.New(t).NotNil(err)
}

func Test_CheckStoreDir_Online_Store_succ(t *testing.T){
	cfg :=&Config{
		StoreDir: "./",
		RunType: RunOnline,
		Log:zap.L().Named("test"),
	}

	patch := gomonkey.ApplyFunc(CheckDirExistAndPrivileges, func(path string) (bool,error){
		return true,nil
	})
	defer patch.Reset()

	err:=cfg.CheckStoreDir()
	assert.New(t).Nil(err)
}

func Test_CheckOutputDir_len_zero(t *testing.T){
	cfg:=&Config{
		OutputDir: "",
		Log:zap.L().Named("test"),
	}
	err:=cfg.CheckOutputDir()
	assert.New(t).NotNil(err)
}

func Test_CheckOutputDir_check_fail(t *testing.T){
	cfg:=&Config{
		OutputDir: "./",
		Log:zap.L().Named("test"),
	}

	err := errors.New("do not have privileges")
	patch := gomonkey.ApplyFunc(CheckDirExistAndPrivileges, func(path string) (bool,error){
		return false,err
	})
	defer patch.Reset()

	err=cfg.CheckOutputDir()
	assert.New(t).NotNil(err)
}

func Test_CheckOutputDir_check_succ(t *testing.T){
	cfg:=&Config{
		OutputDir: "./",
		Log:zap.L().Named("test"),
	}


	patch := gomonkey.ApplyFunc(CheckDirExistAndPrivileges, func(path string) (bool,error){
		return true,nil
	})
	defer patch.Reset()

	err:=cfg.CheckOutputDir()
	assert.New(t).Nil(err)
}

func Test_CheckParamValid_CheckBeginTime_err(t *testing.T){
	cfg:=&Config{
		BeginTimes: "2021",
	}
	err:=cfg.CheckParamValid()
	assert.New(t).NotNil(err)
}

func Test_CheckParamValid_CheckOutputDir_fail(t *testing.T){
	cfg:=&Config{
		OutputDir: "./",
		Log:zap.L().Named("test"),
	}

	err := errors.New("check output dir fail")
	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckOutputDir",
		func(_ *Config) error {
			return err
		})
	defer patches1.Reset()

	err1:= cfg.CheckParamValid()

	assert.New(t).Equal(err,err1)

}


func Test_CheckParamValid_CheckStoreDir_fail(t *testing.T){
	cfg:=&Config{
		OutputDir: "./",
		StoreDir: "./",
		Log:zap.L().Named("test"),
	}

	err := errors.New("check store dir fail")
	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckOutputDir",
		func(_ *Config) error {
			return nil
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckStoreDir",
		func(_ *Config) error {
			return err
		})
	defer patches2.Reset()

	err1:= cfg.CheckParamValid()

	assert.New(t).Equal(err,err1)

}


func Test_CheckParamValid_CheckDsn_fail(t *testing.T){
	cfg:=&Config{
		OutputDir: "./",
		StoreDir: "./",
		Log:zap.L().Named("test"),
	}


	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckOutputDir",
		func(_ *Config) error {
			return nil
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckStoreDir",
		func(_ *Config) error {
			return nil
		})
	defer patches2.Reset()


	err := errors.New("check dsn fail")
	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckDsn",
		func(_ *Config) error {
			return err
		})
	defer patches3.Reset()

	err1:= cfg.CheckParamValid()

	assert.New(t).Equal(err,err1)

}


func Test_CheckParamValid_TryConnectDstDB_fail(t *testing.T){
	cfg:=&Config{
		OutputDir: "./",
		StoreDir: "./",
		Dsn:"",
		Log:zap.L().Named("test"),
	}


	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckOutputDir",
		func(_ *Config) error {
			return nil
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckStoreDir",
		func(_ *Config) error {
			return nil
		})
	defer patches2.Reset()


	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckDsn",
		func(_ *Config) error {
			return nil
		})
	defer patches3.Reset()


	err:=errors.New("try connect dst db fail")
	patches4 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "TryConnectDstDB",
		func(_ *Config) error {
			return err
		})
	defer patches4.Reset()

	err1:= cfg.CheckParamValid()

	assert.New(t).Equal(err1,err)

}


func Test_CheckParamValid_TryConnectDstDB_succ(t *testing.T){
	cfg:=&Config{
		OutputDir: "./",
		StoreDir: "./",
		Dsn:"",
		Log:zap.L().Named("test"),
	}


	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckOutputDir",
		func(_ *Config) error {
			return nil
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckStoreDir",
		func(_ *Config) error {
			return nil
		})
	defer patches2.Reset()


	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "CheckDsn",
		func(_ *Config) error {
			return nil
		})
	defer patches3.Reset()


	//err:=errors.New("try connect dst db fail")
	patches4 := gomonkey.ApplyMethod(reflect.TypeOf(cfg), "TryConnectDstDB",
		func(_ *Config) error {
			return nil
		})
	defer patches4.Reset()

	err1:= cfg.CheckParamValid()

	assert.New(t).Nil(err1)

}

func Test_CheckBeginTime_len_zero (t *testing.T){
	cfg :=&Config{
		BeginTimes: "",
	}
	err := cfg.CheckBeginTime()
	assert.New(t).Nil(err)
}

func Test_CheckBeginTime_err (t *testing.T){
	cfg :=&Config{
		BeginTimes: "2021",
	}
	err := cfg.CheckBeginTime()
	assert.New(t).NotNil(err)
}

func Test_CheckBeginTime_succ (t *testing.T){
	cfg :=&Config{
		BeginTimes: "2021-11-29 20:49:10.999",
	}
	err := cfg.CheckBeginTime()
	assert.New(t).Nil(err)
}

func Test_GetBeginReplaySQL(t *testing.T){
	cfg:=&Config{
		BeginReplaySQL: false,
	}
	b := cfg.GetBeginReplaySQL()
	assert.New(t).False(b)
}

func Test_GetBeginReplaySQLTime(t *testing.T){
	ts := time.Now().Unix()
	cfg:=&Config{
		BeginReplaySQLTime: ts,
	}
	ts1:=cfg.GetBeginReplaySQLTime()

	assert.New(t).Equal(ts,ts1)
}

func Test_SetBeginReplaySQL (t *testing.T){
	cfg:=&Config{}
	cfg.SetBeginReplaySQL(true)
	assert.New(t).True(cfg.BeginReplaySQL)
}

func TestConfig_CheckNeedReplay(t *testing.T) {
	ts := time.Now().Unix()
	type fields struct {
		Dsn                string
		RunTime            uint32
		OutputDir          string
		PreFileSize        uint64
		StoreDir           string
		ListenPort         uint16
		DataDir            string
		FlushInterval      time.Duration
		DeviceName         string
		SrcPort            uint16
		RunType            uint16
		MySQLConfig        *mysql.Config
		BeginReplaySQLTime int64
		BeginReplaySQL     bool
		BeginTimes         string
		Mu                 sync.RWMutex
		Log                *zap.Logger
	}
	type args struct {
		ts int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   uint16
	}{
		{
			name :"NeedReplaySQL",
			fields:fields{
				BeginReplaySQL:true,
			},
			args:args{
				ts:time.Now().Unix(),
			},
			want:NeedReplaySQL,
		},
		{
			name :"NeedReplaySQL",
			fields:fields{
				BeginReplaySQL:false,
				BeginReplaySQLTime:0,
			},
			args:args{
				ts:time.Now().Unix(),
			},
			want:NeedReplaySQL,
		},
		{
			name :"NotWriteLog",
			fields:fields{
				BeginReplaySQL:false,
				BeginReplaySQLTime:ts,
			},
			args:args{
				ts: ts-150000,
			},
			want:NotWriteLog,
		},
		{
			name :"NeedWriteLog",
			fields:fields{
				BeginReplaySQL:false,
				BeginReplaySQLTime:ts,
			},
			args:args{
				ts:ts-50000,
			},
			want:NotWriteLog,
		},
		{
			name :"NeedReplaySQL",
			fields:fields{
				BeginReplaySQL:false,
				BeginReplaySQLTime:ts,
			},
			args:args{
				ts:ts+150000,
			},
			want:NeedReplaySQL,
		},
	}
		for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Dsn:                tt.fields.Dsn,
				RunTime:            tt.fields.RunTime,
				OutputDir:          tt.fields.OutputDir,
				PreFileSize:        tt.fields.PreFileSize,
				StoreDir:           tt.fields.StoreDir,
				ListenPort:         tt.fields.ListenPort,
				DataDir:            tt.fields.DataDir,
				FlushInterval:      tt.fields.FlushInterval,
				DeviceName:         tt.fields.DeviceName,
				SrcPort:            tt.fields.SrcPort,
				RunType:            tt.fields.RunType,
				MySQLConfig:        tt.fields.MySQLConfig,
				BeginReplaySQLTime: tt.fields.BeginReplaySQLTime,
				BeginReplaySQL:     tt.fields.BeginReplaySQL,
				BeginTimes:         tt.fields.BeginTimes,
				Mu:                 tt.fields.Mu,
				Log:                tt.fields.Log,
			}
			if got := cfg.CheckNeedReplay(tt.args.ts); got != tt.want {
				t.Errorf("CheckNeedReplay() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_ParseDateTime_err (t *testing.T){
	cfg:=&Config{
		BeginTimes: "2021",
	}
	err := cfg.ParseDateTime()
	assert.New(t).NotNil(err)
}
func Test_ParseDateTime_succ (t *testing.T){
	cfg:=&Config{
		BeginTimes: "2021-11-29 21:00:00.100",
	}
	err := cfg.ParseDateTime()
	assert.New(t).Nil(err)
}

func Test_bToi_err (t *testing.T){
	_,err := bToi('A')
	assert.New(t).NotNil(err)
}

func Test_bToi_succ (t *testing.T){
	i,err := bToi('3')
	assert.New(t).Nil(err)
	assert.New(t).Equal(i,3)
}

func Test_parseByteNanoSec_err(t *testing.T){
	b := []byte{'1','A','3'}

	_,err:=parseByteNanoSec(b)

	assert.New(t).NotNil(err)
}

func Test_parseByteNanoSec_succ(t *testing.T){
	b := []byte{'1','2','3'}

	i,err:=parseByteNanoSec(b)

	assert.New(t).Nil(err)
	assert.New(t).Equal(i,123000000)
}

func Test_parseByte2Digits_b1_err (t *testing.T){
	b1 :=byte('c')
	b2 :=byte('1')
	_,err:=parseByte2Digits(b1,b2)
	assert.New(t).NotNil(err)
}

func Test_parseByte2Digits_b2_err (t *testing.T){
	b1 :=byte('1')
	b2 :=byte('c')
	_,err:=parseByte2Digits(b1,b2)
	assert.New(t).NotNil(err)
}

func Test_parseByte2Digits_succ ( t *testing.T){
	b1 := byte('1')
	b2 := byte('1')
	i,err:=parseByte2Digits(b1,b2)
	assert.New(t).Equal(i,11)
	assert.New(t).Nil(err)
}

func Test_parseByteYear_err (t *testing.T){
	b := []byte("2A21")
	_,err:=parseByteYear(b)
	assert.New(t).NotNil(err)

}

func Test_parseByteYear_succ (t *testing.T){
	b :=[]byte("2021")
	year,err:=parseByteYear(b)
	assert.New(t).Equal(year,2021)
	assert.New(t).Nil(err)
}

func Test_parseDateTime_zero(t *testing.T) {
	b := []byte("0000-00-00 00:00:00.000")
	_,err:=parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_parseByteYear_err (t *testing.T){
	b := []byte("20A1-00-00 00:00:00.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_parseByteYear_lt_zero (t *testing.T){
	b := []byte("0000-12-00 00:00:00.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_parseByteYear_b4_err (t *testing.T){
	b := []byte("202112-0000:00:00.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_parseMon_err (t *testing.T){
	b := []byte("2021-A2-0000:00:00.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_parseMon_zero (t *testing.T){
	b := []byte("2021-00-0000:00:00.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_b7_err(t *testing.T){
	b := []byte("2021-010000:00:00.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_parseDay_err (t *testing.T){
	b := []byte("2021-11-AA 00:00:00.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_parseDay_zero (t *testing.T){
	b := []byte("2021-11-00 00:00:00.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}


func Test_parseDateTime_b10_err (t *testing.T){
	b := []byte("2021-11-0000:00:00.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_hour_err (t *testing.T){
	b := []byte("2021-11-01 AA:00:00.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_b13_err (t *testing.T){
	b := []byte("2021-11-01 AA00:00.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_min_err (t *testing.T){
	b := []byte("2021-11-01 11:AA:00.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_b16_err (t *testing.T){
	b := []byte("2021-11-01 11:1100.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}
func Test_parseDateTime_sec_err (t *testing.T){
	b := []byte("2021-11-01 11:01:AA.000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_b19_err (t *testing.T){
	b := []byte("2021-11-01 11:11:00000")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_nanosec_err (t *testing.T){
	b := []byte("2021-11-01 11:11:00.A00")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).NotNil(err)
}

func Test_parseDateTime_succ (t *testing.T){
	b := []byte("2021-11-01 11:11:00.100")
	_,err := parseDateTime(b,time.UTC)
	assert.New(t).Nil(err)
}