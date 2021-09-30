package util

import (
	"github.com/agiledragon/gomonkey"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestUtil_CheckParamValid_With_Runtime_Len_Err (t *testing.T){
	runTime :=""
	dsn :=""
	outputDir :=""

	errStr:="runTime len is zero"

	r,m,err := CheckParamValid(dsn,runTime,outputDir)

	ast := assert.New(t)

	ast.Equal(r,0)
	ast.Nil(m)
	ast.Equal(len(err.Error()),len(errStr) )
}

func TestUtil_CheckParamValid_With_Dsn_Len_Err (t *testing.T){
	runTime :="1"
	dsn :=""
	outputDir :=""

	errStr:="dsn len is zero"

	r,m,err := CheckParamValid(dsn,runTime,outputDir)

	ast := assert.New(t)

	ast.Equal(r,0)
	ast.Nil(m)
	ast.Equal(len(err.Error()),len(errStr) )
}

func TestUtil_CheckParamValid_With_OutputDir_Len_Err (t *testing.T){
	runTime :="1"
	dsn :="root:glb34007@tcp(172.16.7.130:3306)/TPCC"
	outputDir :=""

	errStr:="outputDir len is zero"

	r,m,err := CheckParamValid(dsn,runTime,outputDir)

	ast := assert.New(t)

	ast.Equal(r,0)
	ast.Nil(m)
	ast.Equal(len(err.Error()),len(errStr) )
}

func TestUtil_CheckParamValid_With_Atoi_Fail (t *testing.T){
	runTime :="1"
	dsn :="root:glb34007@tcp(172.16.7.130:3306)/TPCC"
	outputDir :="./"

	errStr:="convert string to int error"
	err1:=errors.New(errStr)

	patch := gomonkey.ApplyFunc(strconv.Atoi, func (s string) (int, error) {
		return 0,err1
	})
	defer patch.Reset()

	r,m,err := CheckParamValid(dsn,runTime,outputDir)

	ast := assert.New(t)

	ast.Equal(r,0)
	ast.Nil(m)
	ast.Equal(len(err.Error()),len(errStr) )
}


func TestUtil_CheckParamValid_With_ParseDSN_Fail (t *testing.T){
	runTime :="1"
	dsn :="root:glb34007@tcp(172.16.7.130:3306)/TPCC"
	outputDir :="./"

	errStr:="parse dsn error"
	err1:=errors.New(errStr)

	patch := gomonkey.ApplyFunc(strconv.Atoi, func (s string) (int, error) {
		return 0,nil
	})
	defer patch.Reset()

	patch1 := gomonkey.ApplyFunc(mysql.ParseDSN, func (dsn string) (cfg *mysql.Config, err error){
		return nil,err1
	})
	defer patch1.Reset()

	r,m,err := CheckParamValid(dsn,runTime,outputDir)

	ast := assert.New(t)

	ast.Equal(r,0)
	ast.Nil(m)
	ast.Equal(len(err.Error()),len(errStr) )
}


func TestUtil_CheckParamValid_With_CheckDirExistAndPrivileges_Fail (t *testing.T){
	runTime :="1"
	dsn :="root:glb34007@tcp(172.16.7.130:3306)/TPCC"
	outputDir :="./"


	patch := gomonkey.ApplyFunc(strconv.Atoi, func (s string) (int, error) {
		return 0,nil
	})
	defer patch.Reset()

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

	r,m,err := CheckParamValid(dsn,runTime,outputDir)

	ast := assert.New(t)

	ast.Equal(r,0)
	ast.Nil(m)
	ast.Equal(len(err.Error()),len(errStr) )
}

func TestUtil_CheckParamValid_Succ (t *testing.T){
	runTime :="1"
	dsn :="root:glb34007@tcp(172.16.7.130:3306)/TPCC"
	outputDir :="./"


	patch := gomonkey.ApplyFunc(strconv.Atoi, func (s string) (int, error) {
		return 10,nil
	})
	defer patch.Reset()

	cfg1 := new(mysql.Config)

	patch1 := gomonkey.ApplyFunc(mysql.ParseDSN, func (dsn string) (cfg *mysql.Config, err error){
		return cfg1,nil
	})
	defer patch1.Reset()

	patch2 := gomonkey.ApplyFunc(CheckDirExistAndPrivileges, func  (path string) (bool,error){
		return false,nil
	})
	defer patch2.Reset()

	r,m,err := CheckParamValid(dsn,runTime,outputDir)

	ast := assert.New(t)

	ast.Equal(r,10)
	ast.Equal(m,cfg1)
	ast.Nil(err)
}