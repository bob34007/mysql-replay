package util

import (
	"github.com/pingcap/errors"
	"os"
)

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
