package util

import (
	"github.com/agiledragon/gomonkey"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"io/fs"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
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

func TestUtil_GetFilesFromPath_ReadDir_fail (t *testing.T){
	err := errors.New("read dir fail ")
	patch1 := gomonkey.ApplyFunc(ioutil.ReadDir,func (dirname string) ([]fs.FileInfo, error) {
		return nil ,err
	})
	defer patch1.Reset()

	files := new(map[string]int)
	filePath := "./"
	var mu sync.Mutex
	err1 := GetFilesFromPath(filePath,*files,&mu)

	assert.New(t).Equal(err1.Error(),err.Error())
}

func TestUtil_GetFilesFromPath_ReadDir_succ (t *testing.T){

	patch1 := gomonkey.ApplyFunc(ioutil.ReadDir,func (dirname string) ([]fs.FileInfo, error) {
		return nil ,nil
	})
	defer patch1.Reset()

	files := new(map[string]int)
	filePath := "./"
	var mu sync.Mutex
	err1 := GetFilesFromPath(filePath,*files,&mu)

	assert.New(t).Nil(err1)
	assert.New(t).Equal(0,len(*files))
}

func TestUtil_GetDataFile_GetFilesFromPath_fail (t *testing.T){
	files := new(map[string]int)
	filePath := "./"
	var mu sync.Mutex
	err := errors.New("read dir fail ")
	patch1 := gomonkey.ApplyFunc(GetFilesFromPath,func (filePath string,files map[string]int,mu *sync.Mutex) error {
		return err
	})
	defer patch1.Reset()
	err1:= GetDataFile(filePath,*files,&mu)

	assert.New(t).Equal(err.Error(),err1.Error())
}

func TestUtil_GetDataFile_GetFilesFromPath_succ (t *testing.T){
	files := new(map[string]int)
	filePath := "./"
	var mu sync.Mutex
	patch1 := gomonkey.ApplyFunc(GetFilesFromPath,func (filePath string,files map[string]int,mu *sync.Mutex) error {
		return nil
	})
	defer patch1.Reset()
	err:= GetDataFile(filePath,*files,&mu)

	assert.New(t).Nil(err)
}

func TestUtil_GetFileSizeFromPath_Path_len_zero(t *testing.T){

	filePath :=""
	size , err := GetFileSizeFromPath(filePath)
	assert.New(t).Equal(size,int64(0))
	assert.New(t).Nil(err)

}

func TestUtil_GetFileSizeFromPath_ReadDir_fail (t *testing.T) {
	err := errors.New("read dir fail ")
	patch1 := gomonkey.ApplyFunc(ioutil.ReadDir,func (dirname string) ([]fs.FileInfo, error) {
		return nil ,err
	})
	defer patch1.Reset()
	filePath:="./"
	size,err1 := GetFileSizeFromPath(filePath)

	assert.New(t).Equal(size,int64(0))
	assert.New(t).Equal(err.Error(),err1.Error())

}

func TestUtil_GetFileSizeFromPath_ReadDir_succ (t *testing.T) {
	patch1 := gomonkey.ApplyFunc(ioutil.ReadDir,func (dirname string) ([]fs.FileInfo, error) {
		return nil ,nil
	})
	defer patch1.Reset()
	filePath:="./"
	size,err1 := GetFileSizeFromPath(filePath)

	assert.New(t).Equal(size,int64(0))
	assert.New(t).Nil(err1)

}


func TestUtil_GetFileNumFromPath_Path_len_zero(t *testing.T){

	filePath :=""
	size , err := GetFileNumFromPath(filePath)
	assert.New(t).Equal(size,int64(0))
	assert.New(t).Nil(err)
}

func TestUtil_GetFileNumFromPath_ReadDir_fail (t *testing.T) {
	err := errors.New("read dir fail ")
	patch1 := gomonkey.ApplyFunc(ioutil.ReadDir,func (dirname string) ([]fs.FileInfo, error) {
		return nil ,err
	})
	defer patch1.Reset()
	filePath:="./"
	size,err1 := GetFileNumFromPath(filePath)

	assert.New(t).Equal(size,int64(0))
	assert.New(t).Equal(err.Error(),err1.Error())

}

func TestUtil_GetFileNumFromPath_ReadDir_succ (t *testing.T) {
	patch1 := gomonkey.ApplyFunc(ioutil.ReadDir,func (dirname string) ([]fs.FileInfo, error) {
		return nil ,nil
	})
	defer patch1.Reset()
	filePath:="./"
	size,err1 := GetFileNumFromPath(filePath)

	assert.New(t).Equal(size,int64(0))
	assert.New(t).Nil(err1)

}