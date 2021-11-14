package util

import (
	"fmt"
	"github.com/pingcap/errors"
	"os"
	"sync"
)


type FileNameSeq int

var (
	FileNameSuffix FileNameSeq =1
	mu sync.Mutex
)

func  GetFileNameSeq() int64 {
	mu.Lock()
	defer mu.Unlock()
	return int64(FileNameSuffix)
}

func (fs FileNameSeq) GetNextFileNameSuffix ()string {
	mu.Lock()
	defer mu.Unlock()
	FileNameSuffix ++
	return fmt.Sprintf("-%v",FileNameSuffix)
}



func OpenFile(path, fileName string) (*os.File,error) {
	if len(path)==0 || len(fileName)==0{
		err:= errors.New("path or filename len is 0")
		return nil,err
	}

	//fmt.Println("open file " , fileName)

	fn := path + "/" + fileName
	f,err:=os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC,0755)
	if err!=nil{
		return nil,err
	}
	return f,nil
}


