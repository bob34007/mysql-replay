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
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	RunText = iota
	RunDir
	RunOnline
)

type Config struct {
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
	BeginReplaySQLTime time.Time
	BeginReplaySQL     bool
	BeginTimes         string
	Mu  			   sync.RWMutex
	Log                *zap.Logger
}

func (cfg *Config) CheckParamValid() error {

	var err error

	err = cfg.CheckBeginTime()
	if err != nil {
		return err
	}

	err = cfg.CheckOutputDir()
	if err != nil {
		return err
	}

	err = cfg.CheckStoreDir()
	if err != nil {
		return err
	}

	err = cfg.CheckDsn()
	if err != nil {
		return err
	}

	err = cfg.TryConnectDstDB()
	if err != nil {
		return err
	}

	return nil
}

func (cfg *Config) CheckOutputDir() error {
	if len(cfg.OutputDir) == 0 {
		err := errors.New("outputDir len is zero")
		return err
	}
	_, err := CheckDirExistAndPrivileges(cfg.OutputDir)
	if err != nil {
		return err
	}
	return nil
}

func (cfg *Config) CheckStoreDir() error {
	switch cfg.RunType {
	case RunDir, RunOnline:
		if len(cfg.StoreDir) == 0 {
			return errors.New("store dir len is zero")
		}
	default:
		//
	}
	_, err := CheckDirExistAndPrivileges(cfg.StoreDir)
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

func (cfg *Config) TryConnectDstDB() error {

	connStr := cfg.MySQLConfig.FormatDSN()
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return err
	}
	defer func() {
		err = db.Close()
		if err != nil {
			cfg.Log.Warn("close db conn fail ," + err.Error())
		}
	}()
	rows, err := db.Query("select count(*) from mysql.user;")
	defer func() {
		if rows != nil {
			if rs := rows.Close(); rs != nil {
				cfg.Log.Warn("close row fail," + err.Error())
			}
		}
	}()
	if err != nil {
		return err
	}
	return nil
}

func parseDateTime(b []byte, loc *time.Location) (time.Time, error) {
	const base = "0000-00-00 00:00:00.000"
	// up to "YYYY-MM-DD HH:MM:SS.MMMMMM"
	if string(b) == base[:len(b)] {
		return time.Time{}, errors.New(string(b)+" time is zero ,YYYY-MM-DD HH:MM:SS.MMMMMM")
	}

	year, err := parseByteYear(b)
	if err != nil  {
		return time.Time{}, err
	}
	if year <= 0 {
		return time.Time{},errors.New(string(b)+" year is invalid,YYYY-MM-DD HH:MM:SS.MMMMMM")
	}

	if b[4] != '-' {
		return time.Time{}, fmt.Errorf(string(b)+ " time is not like YYYY-MM-DD HH:MM:SS.MMMMMM")
	}

	m, err := parseByte2Digits(b[5], b[6])
	if err != nil {
		return time.Time{}, err
	}
	if m <= 0 {
		return time.Time{},errors.New(string(b)+" month is invalid,YYYY-MM-DD HH:MM:SS.MMMMMM")
	}
	month := time.Month(m)

	if b[7] != '-' {
		return time.Time{}, fmt.Errorf(string(b)+ " time is not like YYYY-MM-DD HH:MM:SS.MMMMMM")
	}

	day, err := parseByte2Digits(b[8], b[9])
	if err != nil {
		return time.Time{}, err
	}
	if day <= 0 {
		return time.Time{},errors.New(string(b)+" day is invalid,YYYY-MM-DD HH:MM:SS.MMMMMM")
	}

	if b[10] != ' ' {
		return time.Time{}, fmt.Errorf(string(b)+ " time is not like YYYY-MM-DD HH:MM:SS.MMMMMM")
	}

	hour, err := parseByte2Digits(b[11], b[12])
	if err != nil {
		return time.Time{}, err
	}
	if b[13] != ':' {
		return time.Time{}, fmt.Errorf(string(b)+ " time is not like YYYY-MM-DD HH:MM:SS.MMMMMM")
	}

	min, err := parseByte2Digits(b[14], b[15])
	if err != nil {
		return time.Time{}, err
	}
	if b[16] != ':' {
		return time.Time{}, fmt.Errorf(string(b)+ " time is not like YYYY-MM-DD HH:MM:SS.MMMMMM")
	}

	sec, err := parseByte2Digits(b[17], b[18])
	if err != nil {
		return time.Time{}, err
	}

	if b[19] != '.' {
		return time.Time{}, fmt.Errorf(string(b)+ " time is not like YYYY-MM-DD HH:MM:SS.MMMMMM")
	}
	nsec, err := parseByteNanoSec(b[20:])
	if err != nil {
		return time.Time{}, err
	}
	return time.Date(year, month, day, hour, min, sec, nsec, loc), nil

}

func parseByteYear(b []byte) (int, error) {
	year, n := 0, 1000
	for i := 0; i < 4; i++ {
		v, err := bToi(b[i])
		if err != nil {
			return 0, err
		}
		year += v * n
		n /= 10
	}
	return year, nil
}

func parseByte2Digits(b1, b2 byte) (int, error) {
	d1, err := bToi(b1)
	if err != nil {
		return 0, err
	}
	d2, err := bToi(b2)
	if err != nil {
		return 0, err
	}
	return d1*10 + d2, nil
}

func parseByteNanoSec(b []byte) (int, error) {
	ns, digit := 0, 100000 // max is 6-digits
	for i := 0; i < len(b); i++ {
		v, err := bToi(b[i])
		if err != nil {
			return 0, err
		}
		ns += v * digit
		digit /= 10
	}
	// nanoseconds has 10-digits. (needs to scale digits)
	// 10 - 6 = 4, so we have to multiple 1000.
	return ns * 1000, nil
}

func bToi(b byte) (int, error) {
	if b < '0' || b > '9' {
		return 0, errors.New("not [0-9]")
	}
	return int(b - '0'), nil
}

func (cfg *Config) ParseDateTime() error {
	var err error
	if len(cfg.BeginTimes) != 23 {
		err = errors.New("length of time is not 23 digits,YYYY-MM-DD HH:MM:SS.MMMMMM")
		return err
	}
	cfg.BeginReplaySQLTime, err = parseDateTime([]byte(cfg.BeginTimes), time.UTC)
	return err
}

func (cfg *Config) CheckBeginTime() error {
	var err error
	if len(cfg.BeginTimes) == 0 {
		cfg.BeginReplaySQLTime = time.Time{}
		return nil
	}

	err = cfg.ParseDateTime()
	if err != nil {
		return err
	}

	return nil
}

func (cfg *Config) GetBeginReplaySQL()bool{
	cfg.Mu.RLock()
	defer cfg.Mu.RUnlock()
	return cfg.BeginReplaySQL
}
func (cfg *Config) GetBeginReplaySQLTime()time.Time{
	cfg.Mu.RLock()
	defer cfg.Mu.RUnlock()
	return cfg.BeginReplaySQLTime
}

func (cfg *Config) SetBeginReplaySQL(needReplay bool){
	cfg.Mu.Lock()
	defer cfg.Mu.Unlock()
	cfg.BeginReplaySQL=needReplay
}

const (
	NotWriteLog uint16 = iota
	NeedWriteLog
	NeedReplaySQL
)

func (cfg *Config) CheckNeedReplay(ts time.Time) uint16 {

	if cfg.GetBeginReplaySQL()==true {
		return NeedReplaySQL
	}

	beginTs := cfg.GetBeginReplaySQLTime()
	if beginTs.Equal(time.Time{}){
		cfg.SetBeginReplaySQL(true)
		return NeedReplaySQL
	}

	subTime := ts.Sub(cfg.BeginReplaySQLTime).Microseconds()
	if subTime >= -100 && subTime <=100 {
		return NeedWriteLog
	}else if subTime < -100 {
		return NotWriteLog
	} else {
		cfg.SetBeginReplaySQL(true)
		return NeedReplaySQL
	}
}