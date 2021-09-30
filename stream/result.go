package stream

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"os"
)

type ResForWriteFile struct {
	Type   uint64        `json:"type"`
	StmtID uint64        `json:"stmtID,omitempty"`
	Params []interface{} `json:"params,omitempty"`
	DB     string        `json:"db,omitempty"`
	Query  string        `json:"query,omitempty"`
	//read from packet
	PrBeginTime uint64           `json:"pr-begin-time"`
	PrEndTime   uint64           `json:"pr-end-time"`
	PrErrorNo   uint16           `json:"pr-error-no"`
	PrErrorDesc string           `json:"pr-error-desc"`
	PrResult    [][]driver.Value `json:"pr-result"`
	//read from replay server
	RrBeginTime uint64           `json:"rr-begin-time"`
	RrEndTime   uint64           `json:"rr-end-time"`
	RrErrorNo   uint16           `json:"rr-error-no"`
	RrErrorDesc string           `json:"rr-error-desc"`
	RrResult    [][]driver.Value `json:"rr-result"`
	Logger      *zap.Logger
	File        *os.File
	Pos         int64
}


func NewResForWriteFile(pr *PacketRes, rr *ReplayRes,e *MySQLEvent, file *os.File) *ResForWriteFile {

	rs := new(ResForWriteFile)
	//common
	//TODO
	rs.DB = e.DB
	rs.Type =e.Type
	rs.StmtID = e.StmtID
	rs.Params =e.Params

	// packet result
	rs.File = file
	rs.PrBeginTime = pr.sqlBeginTime
	rs.PrEndTime = pr.sqlEndTime
	rs.PrErrorNo = pr.errNo
	rs.PrErrorDesc = pr.errDesc
	if pr.bRows == nil {
		rs.RrResult = pr.tRows.rs.columnValue
	} else {
		rs.PrResult = pr.bRows.rs.columnValue
	}

	//replay server result
	rs.RrBeginTime = rr.SqlBeginTime
	rs.RrEndTime = rr.SqlEndTime
	rs.RrErrorNo = rr.ErrNO
	rs.RrErrorDesc = rr.ErrDesc
	rs.RrResult = rr.ColValues


	rs.Pos = 0
	rs.Logger = zap.L().With(zap.String("conn", "write-data"))
	return rs
}

//formate struct as json and write to file
//if write file fail ,file will truncate and seek to last write pos
//func return write pos end ,and error
func (rs *ResForWriteFile) WriteResToFile() (int64, error) {
	res, err := json.Marshal(rs)
	if err != nil {
		rs.Logger.Warn("format json fail ," + err.Error())
		return rs.Pos,err
	}
	res= append(res,'\n')
	lens := len(res)
	l := make([]byte, 8)

	binary.BigEndian.PutUint64(l, uint64(lens))

	err = rs.WriteData(l,0)
	if err!=nil{
		return rs.Pos,err
	}

	rs.Pos += 8

	err = rs.WriteData(res,8)
	if err!=nil{
		return rs.Pos,err
	}
	rs.Pos += int64(len(res))
	return rs.Pos, nil
}


func (rs *ResForWriteFile ) WriteData (s []byte,n int64) error{

	_, err := rs.File.Write(s)
	if err != nil {
		rs.Logger.Warn("write data fail , " + err.Error())
		rs.Pos -= n
		e := rs.File.Truncate(rs.Pos)
		if e != nil {
			panic("truncate file fail , " + e.Error()+fmt.Sprintf("%v",rs.Pos))
		}
		_, e1 := rs.File.Seek(rs.Pos, 0)
		if e1 != nil {
			panic("seek file fail , " + e1.Error()+fmt.Sprintf("%v",rs.Pos))
		}
		return err
	}
	return nil
}