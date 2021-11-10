package result

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"github.com/bobguo/mysql-replay/stream"
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
	FilePath    string
	FileNamePrefix  string
	Pos uint64

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
			err := stream.ConvertAssignRows(v[a][b],&c)
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

func NewResForWriteFile(pr *stream.PacketRes, rr *stream.ReplayRes,e *stream.MySQLEvent,
	filePath,fileNamePrefix string ,file *os.File,pos uint64) (*ResForWriteFile,error) {
	var err error
	rs := new(ResForWriteFile)
	rs.DB = e.DB
	rs.Type =e.Type

	if rs.Type == stream.EventQuery{
		rs.Query = e.Query
	}

	if rs.Type == stream.EventStmtExecute {
		rs.StmtID = e.StmtID
		rs.Params = rr.Values
		rs.Query = rr.SqlStatment
	}

	rs.File = file
	rs.Pos =pos
	rs.PrBeginTime = pr.GetSqlBeginTime()
	rs.PrEndTime = pr.GetSqlEndTime()
	rs.PrErrorNo = pr.GetErrNo()
	rs.PrErrorDesc = pr.GetErrDesc()

	rs.Logger = zap.L().With(zap.String("conn", "write-data"))

	val := pr.GetColumnVal()
	if val !=nil{
		rs.PrResult,err = ConvertResToStr( val,rs.Logger)
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
	rs.FilePath=filePath
	rs.FileNamePrefix=fileNamePrefix
	return rs,nil
}

//formate struct as json and write to file
//if write file fail ,file will truncate and seek to last write pos
//func return write pos end ,and error
func (rs *ResForWriteFile) WriteResToFile() (uint64, error) {
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

	writeLen ,err := rs.WriteData(l)
	if err!=nil{
		return rs.Pos,err
	}

	writeLen1 ,err := rs.WriteData(res)
	if err!=nil{
		return rs.Pos,err
	}
	rs.Pos += writeLen + writeLen1
	return rs.Pos, nil
}


func (rs *ResForWriteFile ) WriteData (s []byte) (uint64,error) {
	writeLen, err := rs.File.Write(s)
	if err != nil {
		rs.Logger.Warn("write data fail , " + err.Error())
		return 0,err
	}

	return uint64(writeLen),nil
}

