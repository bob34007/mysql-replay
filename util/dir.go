package util

import (
	"fmt"
	"github.com/pingcap/errors"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"sync"
)

var DIRPATHLENERROR = errors.New("dir path len is 0")
var DIRPATHNOTDIRERRIR = errors.New("the path is not dir")

var log =  zap.L().With(zap.String("util", "file"))

func CheckDirExist(path string) (bool,error){
	s,err:=os.Stat(path)
	if err!=nil{
		log.Info("Check dir exist fail , " + err.Error())
		return false,err
	}
	ok :=s.IsDir()
	if !ok{
		log.Info("Check dir exist fail , " + path + "is not dir")
		return ok,DIRPATHNOTDIRERRIR
	}
	return ok,nil
}




func CreateTempFileNameByUUID() string {
	u1 := uuid.NewV1()
	uStr := u1.String()
	return uStr
}

//Determine whether the directory permissions are correct by
//successfully creating temporary files under the directory ,
//will remove file before return
func CheckDirPrivileges(path string) (bool, error) {
	uuid := CreateTempFileNameByUUID()
	fileName := fmt.Sprintf("%s/.%s-temp", path, uuid)
	file, err := os.Create(fileName)
	if err != nil {
		log.Error(" create file for check dir privileges fail , "+err.Error())
		return false, err
	}
	err= file.Close()
	if err!=nil{
		log.Warn("close file " + fileName +"fail , " + err.Error()  )
		return false ,err
	}
	err = os.Remove(fileName)
	if err != nil {
		log.Error("remove file for check dir privileges fail , "+ err.Error())
		return false, err
	}
	return true, nil
}

func CheckDirExistAndPrivileges (path string) (bool,error){

	if len(path) ==0{
		log.Error("dir path len is zero")
		return false,DIRPATHLENERROR
	}
	ok,err:=CheckDirExist(path)
	if !ok{
		if err==DIRPATHNOTDIRERRIR{
			return false,err
		}
		err=os.MkdirAll(path,0744)
		if err!=nil{
			log.Error("make dir fail , "+err.Error())
			return false,err
		}
	}
	ok ,err = CheckDirPrivileges(path)
	if !ok {
		log.Error(" check dir privileges fail , " + err.Error())
		return false ,err
	}
	return ok ,nil

}



func GetFileNumFromPath(path string) (int64,error){
	var fileNum int64 =0
	if len(path) ==0{
		return 0,nil
	}
	files,err := ioutil.ReadDir(path)
	if err !=nil{
		return 0,err
	}
	for _,f := range files {
		if f.IsDir() {
			continue
		}
		fileNum++
	}

	return fileNum,nil
}

func GetFileSizeFromPath(path string) (int64,error){
	var fileSizeCnt int64 =0
	if len(path) ==0{
		return 0,nil
	}
	files,err := ioutil.ReadDir(path)
	if err !=nil{
		return 0,err
	}
	for _,f := range files {
		if f.IsDir() {
			continue
		}
		fileSizeCnt += f.Size()
	}
	return fileSizeCnt,nil
}

func GetDataFile(filePath string,files map[string]int,mu *sync.Mutex) error {

	err := GetFilesFromPath(filePath,files,mu)
	if err!=nil{
		return err
	}
	return nil
}

func GetFilesFromPath(filePath string,files map[string]int,mu *sync.Mutex) error {

	fs, err := ioutil.ReadDir(filePath)
	if err!=nil{
		return err
	}

	for _, file := range fs {
		if file.IsDir() {
			continue
		} else {
			mu.Lock()
			files[file.Name()]=0
			mu.Unlock()
		}
	}

	return nil
}

