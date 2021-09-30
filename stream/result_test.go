package stream

import (
	"database/sql/driver"
	"encoding/json"
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

var pr *PacketRes
var rr *ReplayRes

func init() {
	pr = new(PacketRes)
	rows := new(textRows)
	pr.tRows = rows
	pr.sqlBeginTime = uint64(time.Now().UnixNano())
	pr.sqlEndTime = pr.sqlBeginTime

	pr.tRows.rs.columnValue = make([][]driver.Value, 0)
	rowv := make([]driver.Value, 0)
	rowv = append(rowv, "abc", "def", "hij")
	rows.rs.columnValue = append(rows.rs.columnValue, rowv)
	pr.bRows = nil
	pr.errNo = 0
	pr.errDesc = "sucess"

	rr = new(ReplayRes)
	rr.ColValues = make([][]driver.Value, 0)
	rr.ColValues = append(rr.ColValues, rowv)
	rr.ErrNO = 0
	rr.ErrDesc = "success"
	rr.SqlBeginTime = uint64(time.Now().UnixNano())
	rr.SqlEndTime = rr.SqlBeginTime

}

func TestStream_NewResForWriteFile(t *testing.T) {

	file := new(os.File)
	rs := NewResForWriteFile(pr, rr, file)

	ast := assert.New(t)

	ast.NotNil(rs)

}

func TestStream_WriteResToFile_Succ(t *testing.T) {

	file := new(os.File)
	rs := NewResForWriteFile(pr, rr, file)

	outputs := make([]gomonkey.OutputCell, 0)

	value1 := make([]interface{}, 0)
	value1 = append(value1, nil)
	a := gomonkey.OutputCell{
		Values: value1,
		Times:  1,
	}

	value2 := make([]interface{}, 0)
	value2 = append(value2, nil)
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

	ast.Greater(n, int64(0))
	ast.Nil(err)

}

func TestStream_WriteResToFile_Write_Len_Fail(t *testing.T) {

	file := new(os.File)
	rs := NewResForWriteFile(pr, rr, file)

	outputs := make([]gomonkey.OutputCell, 0)

	err := errors.New("no space left")

	value1 := make([]interface{}, 0)
	value1 = append(value1, err)
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

	ast.Equal(n, int64(0))
	ast.Equal(err, err1)
}

func TestStream_WriteResToFile_Write_Res_Fail(t *testing.T) {

	file := new(os.File)
	rs := NewResForWriteFile(pr, rr, file)

	outputs := make([]gomonkey.OutputCell, 0)

	err := errors.New("no space left")

	value1 := make([]interface{}, 0)
	value1 = append(value1, nil)
	a := gomonkey.OutputCell{
		Values: value1,
		Times:  1,
	}

	value2 := make([]interface{}, 0)
	value2 = append(value2, err)
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

	ast.Equal(n, int64(8))
	ast.Equal(err, err1)
}

func TestStream_WriteResToFile_Marshal_Fail(t *testing.T) {

	file := new(os.File)
	rs := NewResForWriteFile(pr, rr, file)

	err := errors.New("format json fail")

	patch := gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
		return nil, err
	})
	defer patch.Reset()

	n, err1 := rs.WriteResToFile()
	ast := assert.New(t)

	ast.Equal(n, int64(0))
	ast.Equal(err, err1)
}

func TestStream_WriteData_Succ(t *testing.T) {

	file := new(os.File)
	rs := NewResForWriteFile(pr, rr, file)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(file), "Write",
		func(_ *os.File, b []byte) (n int, err error) {
			return 0, nil
		})
	defer patches.Reset()

	b := make([]byte, 0)
	b = append(b, 'a', 'b', 'c')

	err := rs.WriteData(b, 0)

	ast := assert.New(t)

	ast.Nil(err)
}

func TestStream_WriteData_Fail(t *testing.T) {

	file := new(os.File)
	rs := NewResForWriteFile(pr, rr, file)

	err1 := errors.New(" no space left ")

	patches := gomonkey.ApplyMethod(reflect.TypeOf(file), "Write",
		func(_ *os.File, b []byte) (n int, err error) {
			return 0, err1
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(file), "Truncate",
		func(_ *os.File, size int64) error {
			return nil
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(file), "Seek",
		func(_ *os.File, offset int64, whence int) (ret int64, err error) {
			return 0, nil
		})
	defer patches2.Reset()

	b := make([]byte, 0)
	b = append(b, 'a', 'b', 'c')

	err2 := rs.WriteData(b, 0)

	ast := assert.New(t)
	ast.Equal(err2, err1)
}
