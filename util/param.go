package util

import (
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
)

func CheckParamValid (dsn,outputDir,storeDir string ) (*mysql.Config,error){

	var err error


	if len(dsn) == 0 {
		err = errors.New("dsn len is zero")
		return nil,err
	}

	if len(outputDir) ==0 {
		err = errors.New("outputDir len is zero")
		return nil,err
	}


	var MySQLConfig *mysql.Config
	MySQLConfig, err = mysql.ParseDSN(dsn)
	if err != nil {
		return nil,err
	}

	_,err = CheckDirExistAndPrivileges(outputDir)
	if err != nil {
		return nil,err
	}

	if len(storeDir) > 0 {
		_,err = CheckDirExistAndPrivileges(storeDir)
		if err != nil {
			return nil,err
		}
	}

	return MySQLConfig,nil
}