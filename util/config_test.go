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
	"testing"
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