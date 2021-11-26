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
 * @File:  config.go
 * @Version: 1.0.0
 * @Date: 2021/11/26 13:55
 */

package util

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"time"
)

const (
	RunText = iota
	RunDir
	RunOnline
)

type Config struct {
	Dsn       string
	RunTime   uint32
	OutputDir string
	PreFileSize uint64
	StoreDir    string
	ListenPort  uint16
	DataDir     string
	FlushInterval time.Duration
	DeviceName    string
	SrcPort       uint16
	RunType       uint16
	MySQLConfig   *mysql.Config
	Log *zap.Logger
}
func (cfg *Config )CheckParamValid ( ) error{

	var err error

	err = cfg.CheckOutputDir()
	if err !=nil{
		return err
	}

	err = cfg.CheckStoreDir()
	if err !=nil{
		return err
	}

	err = cfg.CheckDsn()
	if err!=nil{
		return err
	}

	err =cfg.TryConnectDstDB()
	if err !=nil{
		return err
	}

	return nil
}

func (cfg *Config)CheckOutputDir() error {
	if len(cfg.OutputDir) ==0 {
		err := errors.New("outputDir len is zero")
		return err
	}
	_,err := CheckDirExistAndPrivileges(cfg.OutputDir)
	if err != nil {
		return err
	}
	return nil
}

func (cfg *Config)CheckStoreDir()error {
	switch cfg.RunType {
	case RunDir , RunOnline :
		if len(cfg.StoreDir) ==0{
			return errors.New("store dir len is zero")
		}
	default:
		//
	}
	_,err := CheckDirExistAndPrivileges(cfg.StoreDir)
	if err != nil {
		return err
	}
	return nil
}

func (cfg *Config) CheckDsn() error {
	var err error
	if len(cfg.Dsn) == 0 {
		err = errors.New("parma dsn len is zero")
		return err
	}

	cfg.MySQLConfig, err = mysql.ParseDSN(cfg.Dsn)
	if err != nil {
		return err
	}
	return nil
}

func (cfg *Config) TryConnectDstDB ( ) error {

	connStr := cfg.MySQLConfig.FormatDSN()
	db,err:= sql.Open("mysql",connStr )
	if err !=nil{
		return err
	}
	defer func (){
		err = db.Close()
		if err !=nil {
			cfg.Log.Warn("close db conn fail ," + err.Error())
		}
	}()
	rows,err :=db.Query("select count(*) from mysql.user;")
	defer func() {
		if rows != nil {
			if rs := rows.Close(); rs != nil {
				cfg.Log.Warn("close row fail," + err.Error())
			}
		}
	}()
	if err !=nil{
		return err
	}
	return nil
}


