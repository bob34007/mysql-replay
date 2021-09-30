package util

import (
	"github.com/agiledragon/gomonkey"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"os"
	"reflect"
	"testing"
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


//test check dir exist success
func TestUtil_CheckDirExist_Succ(t *testing.T) {
	path :="./"

	ok,err:=CheckDirExist(path)
	ast:=assert.New(t)
	ast.Equal(ok,true)
	ast.Nil(err)
}

//test check dir exist success
func TestUtil_CheckDirExist_Stat_Fail(t *testing.T) {
	path :=""
	patch := gomonkey.ApplyFunc(os.Stat, func (name string) (os.FileInfo, error){
		return nil,DIRPATHNOTDIRERRIR
	})
	defer patch.Reset()
	ok,err:=CheckDirExist(path)
	ast:=assert.New(t)
	ast.Equal(ok,false)
	ast.Equal(err,DIRPATHNOTDIRERRIR)
}


func TestUtil_CheckDirExist_IsDir_Fail(t *testing.T) {
	path :="./dir_test.go"
	ok,err:=CheckDirExist(path)
	ast:=assert.New(t)
	ast.Equal(ok,false)
	ast.Equal(err,DIRPATHNOTDIRERRIR)
}

func TestUtil_CreateTempFileNameByUUID(t *testing.T){

	s := CreateTempFileNameByUUID()
	ast := assert.New(t)
	ast.Len(s,len("59f95468-1f38-11ec-b5b7-1e00bb0ecd75"))
}


func TestUtil_CheckDirPrivileges_Create_Fail (t *testing.T){
	path := "./"
	err := errors.New(" read-only file system ")
	patch := gomonkey.ApplyFunc(os.Create, func (name string) (*os.File, error){

		return nil,err
	})
	defer patch.Reset()

	ok,err1:= CheckDirPrivileges(path)
	ast :=assert.New(t)
	ast.Equal(ok,false )
	ast.Equal(err,err1)
}

func TestUtil_CheckDirPrivileges_Close_Fail (t *testing.T){
	path := "./"
	file := new(os.File)
	patch := gomonkey.ApplyFunc(os.Create, func (name string) (*os.File, error){
		return nil,nil
	})
	defer patch.Reset()

	err:=errors.New("close file fail ")

	patches := gomonkey.ApplyMethod(reflect.TypeOf(file), "Close",
		func  (_ *os.File) error {
			return err
		})
	defer patches.Reset()

	ok,err1:= CheckDirPrivileges(path)

	ast :=assert.New(t)
	ast.Equal(ok,false )
	ast.Equal(err,err1)
}

func TestUtil_CheckDirPrivileges_Remove_Fail (t *testing.T){
	path := "./"
	file := new(os.File)
	patch := gomonkey.ApplyFunc(os.Create, func (name string) (*os.File, error){
		return nil,nil
	})
	defer patch.Reset()

	err:=errors.New("remove file fail ")

	patches := gomonkey.ApplyMethod(reflect.TypeOf(file), "Close",
		func  (_ *os.File) error {
			return nil
		})
	defer patches.Reset()

	patch1 := gomonkey.ApplyFunc(os.Remove, func (name string) error{
		return err
	})
	defer patch1.Reset()

	ok,err1:= CheckDirPrivileges(path)

	ast :=assert.New(t)
	ast.Equal(ok,false )
	ast.Equal(err,err1)
}


func TestUtil_CheckDirPrivileges_Success (t *testing.T){
	path := "./"
	file := new(os.File)
	patch := gomonkey.ApplyFunc(os.Create, func (name string) (*os.File, error){
		return nil,nil
	})
	defer patch.Reset()

	patches := gomonkey.ApplyMethod(reflect.TypeOf(file), "Close",
		func  (_ *os.File) error {
			return nil
		})
	defer patches.Reset()

	patch1 := gomonkey.ApplyFunc(os.Remove, func (name string) error{
		return nil
	})
	defer patch1.Reset()

	ok,err1:= CheckDirPrivileges(path)

	ast :=assert.New(t)
	ast.Equal(ok,true )
	ast.Nil(err1)
}


func TestUtil_CheckDirExistAndPrivileges_Len_Fail(t *testing.T){
	path :=""
	ok,err := CheckDirExistAndPrivileges(path)

	ast:=assert.New(t)

	ast.Equal(ok,false)
	ast.Equal(err,DIRPATHLENERROR)
}

func TestUtil_CheckDirExistAndPrivileges_CheckDirExist_With_NotDir (t *testing.T){
	path :="./"
	patch := gomonkey.ApplyFunc(CheckDirExist, func (name string) (bool, error){
		return false,DIRPATHNOTDIRERRIR
	})
	defer patch.Reset()
	ok,err := CheckDirExistAndPrivileges(path)
	ast := assert.New(t)

	ast.Equal(ok,false)
	ast.Equal(err,DIRPATHNOTDIRERRIR)
}


func TestUtil_CheckDirExistAndPrivileges_MkDirALL_Fail(t *testing.T){
	path :="./"
	err :=errors.New("make dir fail")
	patch := gomonkey.ApplyFunc(CheckDirExist, func (name string) (bool, error){
		return false,err
	})
	defer patch.Reset()

	patch1 := gomonkey.ApplyFunc(os.MkdirAll,func (path string, perm os.FileMode) error{
		return err
	})
	defer patch1.Reset()

	ok,err1 := CheckDirExistAndPrivileges(path)
	ast := assert.New(t)

	ast.Equal(ok,false)
	ast.Equal(err1,err)
}


func TestUtil_CheckDirExistAndPrivileges_CheckDirPrivileges_Fail(t *testing.T){
	path :="./"
	err :=errors.New("check dir privileges  fail")
	patch := gomonkey.ApplyFunc(CheckDirExist, func (name string) (bool, error){
		return true,nil
	})
	defer patch.Reset()

	patch1 := gomonkey.ApplyFunc(CheckDirPrivileges,func (path string) (bool, error){
		return false ,err
	})
	defer patch1.Reset()

	ok,err1 := CheckDirExistAndPrivileges(path)
	ast := assert.New(t)

	ast.Equal(ok,false)
	ast.Equal(err1,err)
}


func TestUtil_CheckDirExistAndPrivileges_Succ(t *testing.T){
	path :="./"

	patch := gomonkey.ApplyFunc(CheckDirExist, func (name string) (bool, error){
		return true,nil
	})
	defer patch.Reset()

	patch1 := gomonkey.ApplyFunc(CheckDirPrivileges,func (path string) (bool, error){
		return true ,nil
	})
	defer patch1.Reset()

	ok,err1 := CheckDirExistAndPrivileges(path)
	ast := assert.New(t)

	ast.Equal(ok,true)
	ast.Nil(err1)
}

