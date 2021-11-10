package result

import (
	"database/sql/driver"
	"encoding/json"
	"github.com/bobguo/mysql-replay/stream"
	"github.com/pingcap/errors"
	"reflect"

	"github.com/agiledragon/gomonkey"
	//"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	//"go.uber.org/zap"
	"os"
	//"reflect"
	"testing"
	"time"
)

var pr *stream.PacketRes
var rr *stream.ReplayRes
var e *stream.MySQLEvent
var v [][]driver.Value

func init() {
	pr = new(stream.PacketRes)

	columnValue := make([][]driver.Value, 0)
	rowv := make([]driver.Value, 0)
	rowv = append(rowv, "abc", "def", "hij")
	columnValue = append(columnValue, rowv)



	rr = new(stream.ReplayRes)
	rr.ColValues = make([][]driver.Value, 0)
	rr.ColValues = append(rr.ColValues, rowv)
	rr.ErrNO = 0
	rr.ErrDesc = "success"
	rr.SqlBeginTime = uint64(time.Now().UnixNano())
	rr.SqlEndTime = rr.SqlBeginTime

	e = new(stream.MySQLEvent)

}



func TestStream_NewResForWriteFile(t *testing.T) {

	file := new(os.File)
	rs ,_:= NewResForWriteFile(pr, rr, e,"./","192.16.8.1.1:8000",file,0)

	ast := assert.New(t)

	ast.NotNil(rs)

}

func TestStream_WriteResToFile_Succ(t *testing.T) {

	file := new(os.File)

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetSqlBeginTime",
		func(_ *stream.PacketRes) uint64 {
			return uint64(time.Now().UnixNano())
		})
	defer patches1.Reset()
	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetSqlEndTime",
		func(_ *stream.PacketRes) uint64 {
			return uint64(time.Now().UnixNano())
		})
	defer patches2.Reset()
	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetErrNo",
		func(_ *stream.PacketRes) uint16 {
			return uint16(0)
		})
	defer patches3.Reset()
	patches4 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetErrDesc",
		func(_ *stream.PacketRes) string {
			return "success"
		})
	defer patches4.Reset()
	patches5 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetColumnVal",
		func(_ *stream.PacketRes) [][]driver.Value {
			return v
		})
	defer patches5.Reset()

	rs ,err:= NewResForWriteFile(pr, rr, e,"./","192.16.8.1.1:8000",file,0)

	outputs := make([]gomonkey.OutputCell, 0)

	value1 := make([]interface{}, 0)
	value1 = append(value1, uint64(8),nil)
	a := gomonkey.OutputCell{
		Values: value1,
		Times:  1,
	}

	value2 := make([]interface{}, 0)
	value2 = append(value2, uint64(10),nil)
	b := gomonkey.OutputCell{
		Values: value2,
		Times:  2,
	}

	outputs = append(outputs, a, b)

	patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(rs), "WriteData",
		outputs)
	defer patches.Reset()

	n, err := rs.WriteResToFile()
	ast := assert.New(t)

	ast.Greater(n, uint64(0))
	ast.Nil(err)

}

func TestStream_WriteResToFile_Write_Len_Fail(t *testing.T) {

	file := new(os.File)
	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetSqlBeginTime",
		func(_ *stream.PacketRes) uint64 {
			return uint64(time.Now().UnixNano())
		})
	defer patches1.Reset()
	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetSqlEndTime",
		func(_ *stream.PacketRes) uint64 {
			return uint64(time.Now().UnixNano())
		})
	defer patches2.Reset()
	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetErrNo",
		func(_ *stream.PacketRes) uint16 {
			return uint16(0)
		})
	defer patches3.Reset()
	patches4 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetErrDesc",
		func(_ *stream.PacketRes) string {
			return "success"
		})
	defer patches4.Reset()
	patches5 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetColumnVal",
		func(_ *stream.PacketRes) [][]driver.Value {
			return v
		})
	defer patches5.Reset()
	rs ,err:= NewResForWriteFile(pr, rr,e,"./","192.16.8.1.1:8000",file,0)

	outputs := make([]gomonkey.OutputCell, 0)

	err = errors.New("no space left")

	value1 := make([]interface{}, 0)
	value1 = append(value1, uint64(0),err)
	a := gomonkey.OutputCell{
		Values: value1,
		Times:  1,
	}

	outputs = append(outputs, a)

	patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(rs), "WriteData",
		outputs)
	defer patches.Reset()

	n, err1 := rs.WriteResToFile()
	ast := assert.New(t)

	ast.Equal(n, uint64(0))
	ast.Equal(err, err1)
}

func TestStream_WriteResToFile_Write_Res_Fail(t *testing.T) {

	file := new(os.File)
	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetSqlBeginTime",
		func(_ *stream.PacketRes) uint64 {
			return uint64(time.Now().UnixNano())
		})
	defer patches1.Reset()
	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetSqlEndTime",
		func(_ *stream.PacketRes) uint64 {
			return uint64(time.Now().UnixNano())
		})
	defer patches2.Reset()
	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetErrNo",
		func(_ *stream.PacketRes) uint16 {
			return uint16(0)
		})
	defer patches3.Reset()
	patches4 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetErrDesc",
		func(_ *stream.PacketRes) string {
			return "success"
		})
	defer patches4.Reset()
	patches5 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetColumnVal",
		func(_ *stream.PacketRes) [][]driver.Value {
			return v
		})
	defer patches5.Reset()
	rs ,_:= NewResForWriteFile(pr, rr,e,"./","192.16.8.1.1:8000",file,0)

	outputs := make([]gomonkey.OutputCell, 0)

	err := errors.New("no space left")

	value1 := make([]interface{}, 0)
	value1 = append(value1, uint64(8),nil)
	a := gomonkey.OutputCell{
		Values: value1,
		Times:  1,
	}

	value2 := make([]interface{}, 0)
	value2 = append(value2, uint64(0),err)
	b := gomonkey.OutputCell{
		Values: value2,
		Times:  2,
	}

	outputs = append(outputs, a, b)

	patches := gomonkey.ApplyMethodSeq(reflect.TypeOf(rs), "WriteData",
		outputs)
	defer patches.Reset()

	n, err1 := rs.WriteResToFile()
	ast := assert.New(t)

	ast.Equal(n, uint64(0))
	ast.Equal(err, err1)
}

func TestStream_WriteResToFile_Marshal_Fail(t *testing.T) {

	file := new(os.File)
	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetSqlBeginTime",
		func(_ *stream.PacketRes) uint64 {
			return uint64(time.Now().UnixNano())
		})
	defer patches1.Reset()
	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetSqlEndTime",
		func(_ *stream.PacketRes) uint64 {
			return uint64(time.Now().UnixNano())
		})
	defer patches2.Reset()
	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetErrNo",
		func(_ *stream.PacketRes) uint16 {
			return uint16(0)
		})
	defer patches3.Reset()
	patches4 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetErrDesc",
		func(_ *stream.PacketRes) string {
			return "success"
		})
	defer patches4.Reset()
	patches5 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetColumnVal",
		func(_ *stream.PacketRes) [][]driver.Value {
			return v
		})
	defer patches5.Reset()
	rs ,_:= NewResForWriteFile(pr, rr,e,"./","192.16.8.1.1:8000",file,0)

	err := errors.New("format json fail")

	patch := gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
		return nil, err
	})
	defer patch.Reset()

	n, err1 := rs.WriteResToFile()
	ast := assert.New(t)

	ast.Equal(n, uint64(0))
	ast.Equal(err, err1)
}

func TestStream_WriteData_Succ(t *testing.T) {

	file := new(os.File)
	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetSqlBeginTime",
		func(_ *stream.PacketRes) uint64 {
			return uint64(time.Now().UnixNano())
		})
	defer patches1.Reset()
	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetSqlEndTime",
		func(_ *stream.PacketRes) uint64 {
			return uint64(time.Now().UnixNano())
		})
	defer patches2.Reset()
	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetErrNo",
		func(_ *stream.PacketRes) uint16 {
			return uint16(0)
		})
	defer patches3.Reset()
	patches4 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetErrDesc",
		func(_ *stream.PacketRes) string {
			return "success"
		})
	defer patches4.Reset()
	patches5 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetColumnVal",
		func(_ *stream.PacketRes) [][]driver.Value {
			return v
		})
	defer patches5.Reset()
	rs ,err:= NewResForWriteFile(pr, rr,e,"./","192.16.8.1.1:8000",file,0)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(file), "Write",
		func(_ *os.File, b []byte) (n int, err error) {
			return 0, nil
		})
	defer patches.Reset()

	b := make([]byte, 0)
	b = append(b, 'a', 'b', 'c')

	_,err = rs.WriteData(b)

	ast := assert.New(t)

	ast.Nil(err)
}

func TestStream_WriteData_Fail(t *testing.T) {

	file := new(os.File)
	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetSqlBeginTime",
		func(_ *stream.PacketRes) uint64 {
			return uint64(time.Now().UnixNano())
		})
	defer patches1.Reset()
	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetSqlEndTime",
		func(_ *stream.PacketRes) uint64 {
			return uint64(time.Now().UnixNano())
		})
	defer patches2.Reset()
	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetErrNo",
		func(_ *stream.PacketRes) uint16 {
			return uint16(0)
		})
	defer patches3.Reset()
	patches4 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetErrDesc",
		func(_ *stream.PacketRes) string {
			return "success"
		})
	defer patches4.Reset()
	patches5 := gomonkey.ApplyMethod(reflect.TypeOf(pr), "GetColumnVal",
		func(_ *stream.PacketRes) [][]driver.Value {
			return v
		})
	defer patches5.Reset()
	rs ,_:= NewResForWriteFile(pr, rr,e,"./","192.16.8.1.1:8000",file,0)

	err1 := errors.New(" no space left ")

	patches := gomonkey.ApplyMethod(reflect.TypeOf(file), "Write",
		func(_ *os.File, b []byte) (n int, err error) {
			return 0, err1
		})
	defer patches.Reset()


	b := make([]byte, 0)
	b = append(b, 'a', 'b', 'c')

	_,err2 := rs.WriteData(b)

	ast := assert.New(t)
	ast.Equal(err2, err1)
}
