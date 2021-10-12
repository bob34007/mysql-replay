package util

import (
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"strconv"
)

func CheckParamValid (dsn,runTime,outputDir string ) (int ,*mysql.Config,error){
	var rt int
	var err error
	//fmt.Println(runTime)
	if len(runTime) ==0{
		err = errors.New("runTime len is zero")
		return 0,nil,err
	}

	if len(dsn) == 0 {
		err = errors.New("dsn len is zero")
		return 0,nil,err
	}

	if len(outputDir) ==0 {
		err = errors.New("outputDir len is zero")
		return 0,nil,err
	}

	rt, err = strconv.Atoi(runTime)
	if err != nil {
		return 0,nil,err
	}

	var MySQLConfig *mysql.Config
	MySQLConfig, err = mysql.ParseDSN(dsn)
	if err != nil {
		return 0,nil,err
	}

	_,err = CheckDirExistAndPrivileges(outputDir)
	if err != nil {
		return 0,nil,err
	}

	return rt,MySQLConfig,nil
}