/**
 * @Author: guobob
 * @Description:
 * @File:  utils_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/23 09:50
 */

package stream

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"github.com/stretchr/testify/assert"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func Test_bToi_succ ( t *testing.T){
	b := byte('5')
	i,err:=bToi(b)
	ast := assert.New(t)
	ast.Equal(i,5)
	ast.Nil(err)
}

func Test_bToi_fail_lt_zero ( t *testing.T){
	b := byte(' ')
	i,err:=bToi(b)
	ast := assert.New(t)
	ast.Equal(i,0)
	ast.NotNil(err)
}
func Test_bToi_fail_gt_nine ( t *testing.T){
	b := byte('A')
	i,err:=bToi(b)
	ast := assert.New(t)
	ast.Equal(i,0)
	ast.NotNil(err)
}

func Test_parseByteYear_succ (t *testing.T){
	year:=[]byte("2021")
	y,err:=parseByteYear(year)
	ast:=assert.New(t)
	ast.Equal(y,2021)
	ast.Nil(err)
}

func Test_parseByteYear_fail (t *testing.T){
	year:=[]byte("2A21")
	y,err:=parseByteYear(year)
	ast:=assert.New(t)
	ast.Equal(y,0)
	ast.NotNil(err)
}

func Test_parseByte2Digits_succ (t *testing.T){
	d1 := byte('1')
	d2 := byte('2')
	i,err := parseByte2Digits(d1,d2)
	ast:=assert.New(t)
	ast.Equal(i,12)
	ast.Nil(err)
}

func Test_parseByte2Digits_fail_d1 (t *testing.T){
	d1 := byte('A')
	d2 := byte('2')
	i,err := parseByte2Digits(d1,d2)
	ast:=assert.New(t)
	ast.Equal(i,0)
	ast.NotNil(err)
}
func Test_parseByte2Digits_fail_d2 (t *testing.T){
	d1 := byte('1')
	d2 := byte('A')
	i,err := parseByte2Digits(d1,d2)
	ast:=assert.New(t)
	ast.Equal(i,0)
	ast.NotNil(err)
}

func Test_parseByteNanoSec_succ (t *testing.T){
	nanosec := []byte("123456")
	i,err:=parseByteNanoSec(nanosec)
	ast:=assert.New(t)
	ast.Equal(i,123456000)
	ast.Nil(err)
}
func Test_parseByteNanoSec_fail (t *testing.T){
	nanosec := []byte("12A456")
	i,err:=parseByteNanoSec(nanosec)
	ast:=assert.New(t)
	ast.Equal(i,0)
	ast.NotNil(err)
}
func Test_uint64ToString (t *testing.T){
	l := uint64(123456)
	s:= string(uint64ToString(l))
	ast:=assert.New(t)
	ast.Equal(s,"123456")
}

func Test_stringToInt (t *testing.T){
	s:=[]byte("123456")
	i:=stringToInt(s)
	assert.New(t).Equal(i,123456)
}

func Test_uint64ToBytes (t *testing.T){
	l := uint64(123456)
	s := uint64ToBytes(l)
	assert.New(t).Equal(string(s),"@\xe2\x01\x00\x00\x00\x00\x00")
}


func Test_readLengthEncodedString_read_from_nil (t *testing.T){
	var s []byte
	res, b, i, err:=readLengthEncodedString(s)
	ast:=assert.New(t)
	ast.Nil(res)
	ast.True(b)
	ast.Equal(i,0)
	ast.NotNil(err)
	ast.Equal(err.Error(),"read data from nil slice ,slice:[],len:0,cap:0")
}

func Test_readLengthEncodedString_read_from_len_zero (t *testing.T){
	s := make([]byte,10)
	s1:=s[0:0]
	res,b,i,err:=readLengthEncodedString(s1)
	ast:=assert.New(t)
	ast.Equal(res,[]byte{})
	ast.True(b)
	ast.Equal(i,1)
	ast.Nil(err)
}

func Test_skipLengthEncodedString_slice_len_zero(t *testing.T){
	var s []byte
	i,err:= skipLengthEncodedString(s)

	ast:=assert.New(t)
	ast.Equal(i,1)
	ast.Nil(err)
}

func Test_skipLengthEncodedString_len_fb (t *testing.T){
	s := make([]byte,0,10)
	s=append(s,0xfb)
	i,err:= skipLengthEncodedString(s)
	ast:=assert.New(t)
	ast.Equal(i,1)
	ast.Nil(err)
}

func Test_skipLengthEncodedString_len_fc_return_EOF (t *testing.T){
	s := make([]byte,0,10)
	s=append(s,0xfc,1,2)

	i,err:= skipLengthEncodedString(s)

	ast:=assert.New(t)
	ast.Equal(i,516)
	ast.Equal(err,io.EOF)
}

func Test_skipLengthEncodedString_len_fc_return_nil (t *testing.T){
	s := make([]byte,517,517)
	s[0]=0xfc
	s[1]=1
	s[2]=2

	i,err:= skipLengthEncodedString(s)

	ast:=assert.New(t)
	ast.Equal(i,516)
	ast.Nil(err)
}

func Test_readLengthEncodedInteger_slice_len_zero(t *testing.T){
	var s []byte
	i,b,j:=readLengthEncodedInteger(s)
	ast:= assert.New(t)
	ast.Equal(i,uint64(0))
	ast.Equal(j,1)
	ast.True(b)
}

func Test_readLengthEncodedInteger_slice_len_fb(t *testing.T){
	var s []byte
	s=append(s,0xfb)
	i,b,j:=readLengthEncodedInteger(s)
	ast:= assert.New(t)
	ast.Equal(i,uint64(0))
	ast.Equal(j,1)
	ast.True(b)
}

func Test_readLengthEncodedInteger_slice_len_fc(t *testing.T){
	var s []byte
	s=append(s,0xfc,1,2)
	i,b,j:=readLengthEncodedInteger(s)
	ast:= assert.New(t)
	ast.Equal(i,uint64(513))
	ast.Equal(j,3)
	ast.False(b)
}

func Test_readLengthEncodedInteger_slice_len_fd(t *testing.T){
	var s []byte
	s=append(s,0xfd,1,2,3)
	i,b,j:=readLengthEncodedInteger(s)
	ast:= assert.New(t)
	ast.Equal(i,uint64(197121))
	ast.Equal(j,4)
	ast.False(b)
}

func Test_readLengthEncodedInteger_slice_len_fe(t *testing.T){
	var s []byte
	s=append(s,0xfe,1,2,3,4,5,6,7,8)
	i,b,j:=readLengthEncodedInteger(s)
	ast:= assert.New(t)
	ast.Equal(i,uint64(578437695752307201))
	ast.Equal(j,9)
	ast.False(b)
}


func Test_readLengthEncodedInteger_slice_len_16(t *testing.T){
	var s []byte
	s=append(s,16,1,2,3,4,5,6,7,8)
	i,b,j:=readLengthEncodedInteger(s)
	ast:= assert.New(t)
	ast.Equal(i,uint64(16))
	ast.Equal(j,1)
	ast.False(b)
}

func Test_reserveBuffer_need_grow(t *testing.T){
	s := make([]byte,0,10)
	ns := reserveBuffer(s,20)
	assert.New(t).Equal(cap(ns),20)
}

func Test_reserveBuffer_not_need_grow(t *testing.T){
	s := make([]byte,0,30)
	ns := reserveBuffer(s,20)
	assert.New(t).Equal(len(ns),20)
}

func Test_atomicBool_IsSet(t *testing.T){
	a := &atomicBool{
		_noCopy:noCopy{},
		value: 1,
	}
	b:=a.IsSet()
	assert.New(t).True(b)
}

func Test_atomicBool_Set_true(t *testing.T){
	a:=&atomicBool{
		_noCopy:noCopy{},
		value: 0,
	}
	a.Set(true)
	assert.New(t).Equal(a.value,uint32(1))
}
func Test_atomicBool_Set_false(t *testing.T){
	a:=&atomicBool{
		_noCopy:noCopy{},
		value: 0,
	}
	a.Set(false)
	assert.New(t).Equal(a.value,uint32(0))
}

func Test_atomicBool_TrySet_true(t *testing.T){
	a:=&atomicBool{
		_noCopy:noCopy{},
		value: 0,
	}
	b:=a.TrySet(true)
	assert.New(t).True(b)
}

func Test_atomicBool_TrySet_false(t *testing.T){
	a:=&atomicBool{
		_noCopy:noCopy{},
		value: 0,
	}
	b:=a.TrySet(false)
	assert.New(t).False(b)
}

func Test_atomicError_set_error(t *testing.T){
	a:=&atomicError{
		_noCopy: noCopy{},
		value: atomic.Value{},
	}

	a.Set(errors.New("errors happen"))
	err:=a.Value()
	assert.New(t).NotNil(err)
}

func Test_atomicError_value_nil(t *testing.T){
	a:=&atomicError{
		_noCopy: noCopy{},
		value: atomic.Value{},
	}
	err:=a.Value()
	assert.New(t).Nil(err)
}

func Test_readBool_true_true(t *testing.T){
	input :="true"
	a,b:=readBool(input)
	assert.New(t).True(a)
	assert.New(t).True(b)
}

func Test_readBool_true_false(t *testing.T){
	input :="false"
	a,b:=readBool(input)
	assert.New(t).False(a)
	assert.New(t).True(b)
}

func Test_readBool_true_other(t *testing.T){
	input :="f"
	a,b:=readBool(input)
	assert.New(t).False(a)
	assert.New(t).False(b)
}

func Test_parseDateTime(t *testing.T){
	dateStr := []byte("2021-11-24 09:53:53.000000")

	ts,err:= parseDateTime(dateStr,time.UTC)

	ast:=assert.New(t)
	ast.Equal(ts.String()[0:19],"2021-11-24 09:53:53")
	ast.Nil(err)

}


func Test_appendMicrosecs_decimals_eq_zero(t *testing.T){
	dst:=[]byte("2021-11-24 10:10:00")
	src:=[]byte("123456")
	d:=appendMicrosecs(dst,src,0)
	assert.New(t).Equal(string(d),string(dst))
}

func Test_appendMicrosecs_src_len_zero(t *testing.T){
	dst:=[]byte("2021-11-24 10:10:00")
	src:=[]byte("")
	d:=appendMicrosecs(dst,src,6)
	assert.New(t).Equal(string(d),string(dst)+".000000")
}

func Test_appendMicrosecs_decimals_1(t *testing.T){
	dst:=[]byte("2021-11-24 10:10:00")
	src:=[]byte("123456")
	d:=appendMicrosecs(dst,src,1)
	assert.New(t).Equal(string(d),string(dst)+".2")
}

func Test_appendMicrosecs_decimals_2(t *testing.T){
	dst:=[]byte("2021-11-24 10:10:00")
	src:=[]byte("123456")
	d:=appendMicrosecs(dst,src,2)
	assert.New(t).Equal(string(d),string(dst)+".25")
}

func Test_appendMicrosecs_decimals_3(t *testing.T){
	dst:=[]byte("2021-11-24 10:10:00")
	src:=[]byte("123456")
	d:=appendMicrosecs(dst,src,3)
	assert.New(t).Equal(string(d),string(dst)+".250")
}

func Test_appendMicrosecs_decimals_4(t *testing.T){
	dst:=[]byte("2021-11-24 10:10:00")
	src:=[]byte("123456")
	d:=appendMicrosecs(dst,src,4)
	assert.New(t).Equal(string(d),string(dst)+".2504")
}

func Test_appendMicrosecs_decimals_5(t *testing.T){
	dst:=[]byte("2021-11-24 10:10:00")
	src:=[]byte("123456")
	d:=appendMicrosecs(dst,src,5)
	assert.New(t).Equal(string(d),string(dst)+".25041")
}
func Test_appendMicrosecs_decimals_6(t *testing.T){
	dst:=[]byte("2021-11-24 10:10:00")
	src:=[]byte("123456")
	d:=appendMicrosecs(dst,src,6)
	assert.New(t).Equal(string(d),string(dst)+".250417")
}

func Test_escapeBytesQuotes(t *testing.T){
	buf :=[]byte("12345")
	src:=[]byte("'\abc")
	dst:=escapeBytesQuotes(buf,src)
	assert.New(t).Equal(len(dst),10)
}

func Test_escapeStringQuotes(t *testing.T) {
	buf :=[]byte("12345")
	src :="'\abc"
	dst:=escapeStringQuotes(buf,src)
	assert.New(t).Equal(len(dst),10)
}

func Test_escapeBytesBackslash(t *testing.T){
	buf:=[]byte("12345")
	var src []byte
	src=append(src,'\x00','\n','\r','\x1a','\'','"','\\','a')
	dst:=escapeBytesBackslash(buf,src)
	assert.New(t).Equal(len(dst),20)
}

func Test_escapeStringBackslash(t *testing.T){
	buf:=[]byte("12345")
	var src []byte
	src=append(src,'\x00','\n','\r','\x1a','\'','"','\\','a')
	dst:=escapeStringBackslash(buf,string(src))
	assert.New(t).Equal(len(dst),20)
}

func Test_namedValueToValue_ret_error(t *testing.T) {
	var input []driver.NamedValue

	input = append(input,driver.NamedValue{
		Name:"test",
	})
	d,err:=namedValueToValue(input)
	ast:=assert.New(t)
	ast.Nil(d)
	ast.NotNil(err)
}

func Test_namedValueToValue_ret_nil(t *testing.T) {
	var input []driver.NamedValue

	input = append(input,driver.NamedValue{
		Name:"",
		Value: 10,
	})
	d,err:=namedValueToValue(input)
	ast:=assert.New(t)
	ast.Equal(len(d),1)
	ast.Nil(err)
}

func Test_mapIsolationLevel(t *testing.T) {
	type args struct {
		level driver.IsolationLevel
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name:"LevelRepeatableRead",
			args:args{
				level:driver.IsolationLevel(4),
			},
			want:"REPEATABLE READ",
			wantErr: false,
		},
		{
			name:"LevelReadCommitted",
			args:args{
				level:driver.IsolationLevel(2),
			},
			want:"READ COMMITTED",
			wantErr: false,
		},
		{
			name:"LevelReadUncommitted",
			args:args{
				level:driver.IsolationLevel(1),
			},
			want:"READ UNCOMMITTED",
			wantErr: false,
		},
		{
			name:"LevelSerializable",
			args:args{
				level:driver.IsolationLevel(6),
			},
			want:"SERIALIZABLE",
			wantErr: false,
		},
		{
			name:"LevelSerializable",
			args:args{
				level:driver.IsolationLevel(60),
			},
			want:"",
			wantErr: true,
		},
	}
		for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mapIsolationLevel(tt.args.level)
			if (err != nil) != tt.wantErr {
				t.Errorf("mapIsolationLevel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("mapIsolationLevel() got = %v, want %v", got, tt.want)
			}
		})
	}
}


func Test_appendDateTime(t *testing.T){
	var buf []byte
	ts :=time.Date(2021, 11, 30, 21, 45, 00, 100, time.UTC)
	d,err:=appendDateTime(buf,ts)
	ast:=assert.New(t)
	ast.Equal(len(d),27)
	ast.Nil(err)
}

func Test_parseBinaryDateTime_case_0(t *testing.T){
	num:=uint64(0)
	var data []byte
	_,err := parseBinaryDateTime(num,data,time.UTC)
	assert.New(t).Nil(err)
}

func Test_parseBinaryDateTime_case_4(t *testing.T){
	num:=uint64(4)
	data:=make([]byte,10,10)
	binary.LittleEndian.PutUint16(data[0:], 2021)
	data[2]=11
	_,err := parseBinaryDateTime(num,data,time.UTC)
	assert.New(t).Nil(err)
}

func Test_parseBinaryDateTime_case_7(t *testing.T){
	num:=uint64(7)
	data:=make([]byte,10,10)
	binary.LittleEndian.PutUint16(data[0:], 2021)
	data[2]=11
	data[3]=24
	data[4]=15
	data[5]=5
	data[6]=30
	_,err := parseBinaryDateTime(num,data,time.UTC)
	assert.New(t).Nil(err)
}

func Test_parseBinaryDateTime_case_11(t *testing.T){
	num:=uint64(11)
	data:=make([]byte,20,20)
	binary.LittleEndian.PutUint16(data[0:], 2021)
	data[2]=11
	data[3]=24
	data[4]=15
	data[5]=5
	data[6]=30
	binary.LittleEndian.PutUint32(data[7:], 998990)
	_,err := parseBinaryDateTime(num,data,time.UTC)
	assert.New(t).Nil(err)
}

func Test_parseBinaryDateTime_case_12(t *testing.T){
	num:=uint64(12)
	data:=make([]byte,20,20)
	binary.LittleEndian.PutUint16(data[0:], 2021)
	data[2]=11
	data[3]=24
	data[4]=15
	data[5]=5
	data[6]=30
	binary.LittleEndian.PutUint32(data[7:], 998990)
	_,err := parseBinaryDateTime(num,data,time.UTC)
	assert.New(t).NotNil(err)
}