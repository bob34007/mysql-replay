package util

import (
	"github.com/agiledragon/gomonkey"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)



func TestUtil_CheckParamValid_With_Dsn_Len_Err (t *testing.T){

	dsn :=""
	outputDir :=""

	errStr:="dsn len is zero"

	m,err := CheckParamValid(dsn,outputDir)

	ast := assert.New(t)

	ast.Nil(m)
	ast.Equal(len(err.Error()),len(errStr) )
}

func TestUtil_CheckParamValid_With_OutputDir_Len_Err (t *testing.T){

	dsn :="root:glb34007@tcp(172.16.7.130:3306)/TPCC"
	outputDir :=""

	errStr:="outputDir len is zero"

	m,err := CheckParamValid(dsn,outputDir)

	ast := assert.New(t)

	ast.Nil(m)
	ast.Equal(len(err.Error()),len(errStr) )
}

func TestUtil_CheckParamValid_With_ParseDSN_Fail (t *testing.T){

	dsn :="root:glb34007@tcp(172.16.7.130:3306)/TPCC"
	outputDir :="./"

	errStr:="parse dsn error"
	err1:=errors.New(errStr)



	patch1 := gomonkey.ApplyFunc(mysql.ParseDSN, func (dsn string) (cfg *mysql.Config, err error){
		return nil,err1
	})
	defer patch1.Reset()

	m,err := CheckParamValid(dsn,outputDir)

	ast := assert.New(t)

	ast.Nil(m)
	ast.Equal(len(err.Error()),len(errStr) )
}


func TestUtil_CheckParamValid_With_CheckDirExistAndPrivileges_Fail (t *testing.T){

	dsn :="root:glb34007@tcp(172.16.7.130:3306)/TPCC"
	outputDir :="./"




	cfg1 := new(mysql.Config)

	patch1 := gomonkey.ApplyFunc(mysql.ParseDSN, func (dsn string) (cfg *mysql.Config, err error){
		return cfg1,nil
	})
	defer patch1.Reset()


	errStr:="check dir error"
	err1:=errors.New(errStr)

	patch2 := gomonkey.ApplyFunc(CheckDirExistAndPrivileges, func  (path string) (bool,error){
		return false,err1
	})
	defer patch2.Reset()

	m,err := CheckParamValid(dsn,outputDir)

	ast := assert.New(t)

	ast.Nil(m)
	ast.Equal(len(err.Error()),len(errStr) )
}

func TestUtil_CheckParamValid_Succ (t *testing.T){

	dsn :="root:glb34007@tcp(172.16.7.130:3306)/TPCC"
	outputDir :="./"



	cfg1 := new(mysql.Config)

	patch1 := gomonkey.ApplyFunc(mysql.ParseDSN, func (dsn string) (cfg *mysql.Config, err error){
		return cfg1,nil
	})
	defer patch1.Reset()

	patch2 := gomonkey.ApplyFunc(CheckDirExistAndPrivileges, func  (path string) (bool,error){
		return false,nil
	})
	defer patch2.Reset()

	m,err := CheckParamValid(dsn,outputDir)

	ast := assert.New(t)


	ast.Equal(m,cfg1)
	ast.Nil(err)
}