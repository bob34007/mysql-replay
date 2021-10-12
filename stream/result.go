package stream

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
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
	PrResult    [][]string `json:"pr-result"`
	//read from replay server
	RrBeginTime uint64           `json:"rr-begin-time"`
	RrEndTime   uint64           `json:"rr-end-time"`
	RrErrorNo   uint16           `json:"rr-error-no"`
	RrErrorDesc string           `json:"rr-error-desc"`
	RrResult    [][]string  `json:"rr-result"`
	Logger      *zap.Logger
	File        *os.File
	Pos         int64
}

func ConvertResToStr(v [][]driver.Value,log *zap.Logger) ([][]string,error) {
	resSet := make([][]string,0)
	for a :=range v{
		rowStr := make([]string,0)
		for b :=range v[a] {
			var c string
			if v[a][b] == nil{
				//pay attention on the following logic
				//If driver.Value is nil, we convert it to a string of length 0,
				//but then we can't compare nil to a string of length 0
				c = ""
				rowStr=append(rowStr,c)
				continue
			}
			err := convertAssignRows(&c, v[a][b])
			if err !=nil{
				log.Warn("convert driver.Value to string fail , "+ err.Error())
				return nil ,err
			} else {
				rowStr=append(rowStr,c)
			}
		}
		resSet = append(resSet,rowStr)
	}
	return resSet,nil
}




func NewResForWriteFile(pr *PacketRes, rr *ReplayRes,e *MySQLEvent, file *os.File) (*ResForWriteFile,error) {
	var err error
	rs := new(ResForWriteFile)
	//common
	rs.DB = e.DB
	rs.Type =e.Type

	if rs.Type == EventQuery{
		rs.Query = e.Query
	}
	//fmt.Println(e.Query)
	if rs.Type == EventStmtExecute {
		rs.StmtID = e.StmtID
		rs.Params = rr.Values
		rs.Query = rr.SqlStatment
	}

	// packet result
	rs.File = file
	rs.PrBeginTime = pr.sqlBeginTime
	rs.PrEndTime = pr.sqlEndTime
	rs.PrErrorNo = pr.errNo
	rs.PrErrorDesc = pr.errDesc

	//fmt.Println(rs)
	rs.Logger = zap.L().With(zap.String("conn", "write-data"))
	if pr.bRows != nil {
		rs.PrResult ,err = ConvertResToStr(pr.bRows.rs.columnValue,rs.Logger)
		if err !=nil{
			return rs ,err
		}
	} else if pr.tRows !=nil  {
		rs.PrResult,err = ConvertResToStr( pr.tRows.rs.columnValue ,rs.Logger)
		if err !=nil{
			return rs ,err
		}
	}
	//replay server result
	rs.RrBeginTime = rr.SqlBeginTime
	rs.RrEndTime = rr.SqlEndTime
	rs.RrErrorNo = rr.ErrNO
	rs.RrErrorDesc = rr.ErrDesc
	rs.RrResult,err = ConvertResToStr( rr.ColValues,rs.Logger)
	if err !=nil{
		return rs ,err
	}


	return rs,nil
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
	//fmt.Println(res)
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
	//fmt.Println(rs.File.Name(),m)
	if err != nil {
		rs.Logger.Warn("write data fail , " + err.Error())
		return err
	}
	return nil
}