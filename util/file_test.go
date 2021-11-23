package util

import (
	"github.com/agiledragon/gomonkey"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestUtil_OpenFile_With_Path_Len_Err( t *testing.T){
	path :=""
	fileName :="abc.txt"
	errStr :="path or filename len is 0"
	f,err:=OpenFile (path,fileName)

	ast :=assert.New(t)

	ast.Nil(f)
	ast.Equal(len(err.Error()),len(errStr))
}

func TestUtil_OpenFile_With_FileName_Len_Err( t *testing.T){
	path :="./"
	fileName :=""
	errStr :="path or filename len is 0"
	f,err:=OpenFile (path,fileName)

	ast :=assert.New(t)

	ast.Nil(f)
	ast.Equal(len(err.Error()),len(errStr))
}

func TestUtil_OpenFile_Fail( t *testing.T){
	path :="./"
	fileName :="abc.txt"



	err1 := errors.New("open file fail")

	patch := gomonkey.ApplyFunc(os.OpenFile, func (name string, flag int, perm os.FileMode) (*os.File, error){
		return nil,err1
	})
	defer patch.Reset()

	f,err:=OpenFile (path,fileName)

	ast :=assert.New(t)

	ast.Nil(f)
	ast.Equal(err1,err)
}

func TestUtil_OpenFile_Succ( t *testing.T){
	path :="./"
	fileName :="abc.txt"


	f1 := new(os.File)


	patch := gomonkey.ApplyFunc(os.OpenFile, func (name string, flag int, perm os.FileMode) (*os.File, error){
		return f1,nil
	})
	defer patch.Reset()

	f,err:=OpenFile (path,fileName)

	ast :=assert.New(t)

	ast.Nil(err)
	ast.Equal(f1,f)
}

func TestFileNameSeq_GetNextFileNameSuffix(t *testing.T) {
	FileNameSuffix =1
	wantString:="-2"
	fileNameSuffix := FileNameSuffix.GetNextFileNameSuffix()
	assert.New(t).Equal(fileNameSuffix,wantString)
}
func Test_GetFileNameSeq(t *testing.T){
	FileNameSuffix =2
	fileNameSeq := GetFileNameSeq()
	assert.New(t).Equal(fileNameSeq,int64(2))
}

