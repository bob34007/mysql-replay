/**
 * @Author: guobob
 * @Description:
 * @File:  utils_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/23 09:50
 */

package stream

import (
	"github.com/stretchr/testify/assert"
	"testing"
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

