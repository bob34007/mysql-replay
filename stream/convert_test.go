/**
 * @Author: guobob
 * @Description:
 * @File:  convert_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/24 10:31
 */

package stream

import (
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func Test_strconvErr(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:"is not  number",
			args:args{
				err:errors.New("errors happen"),
			},
			wantErr: true,
		},
		{
			name:"is   number",
			args:args{
				err:&strconv.NumError{
					Func:"testfunc",
					Num:"testnum",
					Err:nil,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := strconvErr(tt.args.err); (err != nil) != tt.wantErr {
				t.Errorf("strconvErr() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_cloneBytes(t *testing.T) {
	type args struct {
		b []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name:"input nil",
			args:args{
				b: nil,
			},
			want:nil,
		},
		{
			name:"input nil",
			args:args{
				b: []byte("12345"),
			},
			want:[]byte("12345"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cloneBytes(tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("cloneBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_asString(t *testing.T) {
	type args struct {
		src interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name : "string",
			args:args{"123456"},
			want:"123456",
		},
		{
			name : "[]byte]",
			args:args{[]byte("123456")},
			want:"123456",
		},
		{
			name : "int",
			args:args{10},
			want:"10",
		},
		{
			name : "Uint",
			args:args{uint(10)},
			want:"10",
		},
		{
			name : "Float64",
			args:args{float64(10)},
			want:"10",
		},
		{
			name : "Float32",
			args:args{float32(10)},
			want:"10",
		},
		{
			name : "bool",
			args:args{false},
			want:"false",
		},
		{
			name : "map",
			args:args{new(map[string]int)},
			want:"&map[]",
		},
	}
		for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := asString(tt.args.src); got != tt.want {
				t.Errorf("asString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_asBytes(t *testing.T) {
	type args struct {
		buf []byte
		rv  reflect.Value
	}
	tests := []struct {
		name   string
		args   args
		wantB  []byte
		wantOk bool
	}{
		{
			name:"int",
			args:args{
				rv : reflect.ValueOf(1),
			},
			wantB:[]byte("1"),
			wantOk: true,
		},
		{
			name:"uint",
			args:args{
				rv : reflect.ValueOf(uint(1)),
			},
			wantB:[]byte("1"),
			wantOk: true,
		},
		{
			name:"float32",
			args:args{
				rv : reflect.ValueOf(float32(1)),
			},
			wantB:[]byte("1"),
			wantOk: true,
		},
		{
			name:"float64",
			args:args{
				rv : reflect.ValueOf(float64(1)),
			},
			wantB:[]byte("1"),
			wantOk: true,
		},
		{
			name:"bool",
			args:args{
				rv : reflect.ValueOf(true),
			},
			wantB:[]byte("true"),
			wantOk: true,
		},
		{
			name:"bool",
			args:args{
				rv : reflect.ValueOf("12345"),
			},
			wantB:[]byte("12345"),
			wantOk: true,
		},
		{
			name:"slice",
			args:args{
				rv : reflect.ValueOf([]byte("12345")),
			},
			wantB:nil,
			wantOk: false,
		},

	}
		for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotB, gotOk := asBytes(tt.args.buf, tt.args.rv)
			if !reflect.DeepEqual(gotB, tt.wantB) {
				t.Errorf("asBytes() gotB = %v, want %v", gotB, tt.wantB)
			}
			if gotOk != tt.wantOk {
				t.Errorf("asBytes() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func Test_NullString_Scan_nil(t *testing.T){
	ns := &NullString{}
	err := ns.Scan(nil)
	ast := assert.New(t)
	ast.Equal(ns.String,"")
	ast.False(ns.Valid)
	ast.Nil(err)
}

func Test_NullString_Scan(t *testing.T){
	ns := &NullString{}
	err := ns.Scan("12345")
	ast:=assert.New(t)
	ast.Equal(ns.String,"12345")
	ast.True(ns.Valid)
	ast.Nil(err)
}

func Test_NullString_Value_nil(t *testing.T){
	ns := &NullString{
		Valid: false,
	}
	d,err:=ns.Value()

	ast:=assert.New(t)
	ast.Nil(d)
	ast.Nil(err)
}

func Test_NullString_Value(t *testing.T){
	ns := &NullString{
		String:"12345",
		Valid: true,
	}
	d,err:=ns.Value()

	ast:=assert.New(t)
	ast.Equal(d,"12345")
	ast.Nil(err)
}

func Test_NullInt64_Scan_nil(t *testing.T){
	ni := &NullInt64{}
	err := ni.Scan(nil)
	ast:=assert.New(t)
	ast.Nil(err)
	ast.Equal(ni.Int64,int64(0))
	ast.False(ni.Valid)
}

func Test_NullInt64_Scan(t *testing.T){
	ni := &NullInt64{}
	err := ni.Scan(10)
	ast:=assert.New(t)
	ast.Nil(err)
	ast.Equal(ni.Int64,int64(10))
	ast.True(ni.Valid)
}

func Test_NullInt64_Value_nil(t *testing.T){
	ni := &NullInt64{
		Valid: false,
	}
	d,err:=ni.Value()

	ast:=assert.New(t)
	ast.Nil(d)
	ast.Nil(err)
}

func Test_NullInt64_Value(t *testing.T){
	ni := &NullInt64{
		Valid: true ,
		Int64: 10,
	}
	d,err:=ni.Value()

	ast:=assert.New(t)
	ast.Equal(d,int64(10))
	ast.Nil(err)
}


func Test_NullFloat64_Scan_nil(t *testing.T){
	ni := &NullFloat64{}
	err := ni.Scan(nil)
	ast:=assert.New(t)
	ast.Nil(err)
	ast.Equal(ni.Float64,float64(0))
	ast.False(ni.Valid)
}

func Test_NullFloat64_Scan(t *testing.T){
	ni := &NullFloat64{}
	err := ni.Scan(10)
	ast:=assert.New(t)
	ast.Nil(err)
	ast.Equal(ni.Float64,float64(10))
	ast.True(ni.Valid)
}

func Test_NullFloat64_Value_nil(t *testing.T){
	ni := &NullFloat64{
		Valid: false,
	}
	d,err:=ni.Value()

	ast:=assert.New(t)
	ast.Nil(d)
	ast.Nil(err)
}

func Test_NullFloat64_Value(t *testing.T){
	ni := &NullFloat64{
		Valid: true ,
		Float64: 10,
	}
	d,err:=ni.Value()

	ast:=assert.New(t)
	ast.Equal(d,float64(10))
	ast.Nil(err)
}



func Test_NullInt32_Scan_nil(t *testing.T){
	ni := &NullInt32{}
	err := ni.Scan(nil)
	ast:=assert.New(t)
	ast.Nil(err)
	ast.Equal(ni.Int32,int32(0))
	ast.False(ni.Valid)
}

func Test_NullInt32_Scan(t *testing.T){
	ni := &NullInt32{}
	err := ni.Scan(10)
	ast:=assert.New(t)
	ast.Nil(err)
	ast.Equal(ni.Int32,int32(10))
	ast.True(ni.Valid)
}

func Test_NullInt32_Value_nil(t *testing.T){
	ni := &NullInt32{
		Valid: false,
	}
	d,err:=ni.Value()

	ast:=assert.New(t)
	ast.Nil(d)
	ast.Nil(err)
}

func Test_NullInt32_Value(t *testing.T){
	ni := &NullInt32{
		Valid: true ,
		Int32: 10,
	}
	d,err:=ni.Value()

	ast:=assert.New(t)
	ast.Equal(d,int64(10))
	ast.Nil(err)
}


func Test_NullBool_Scan_nil(t *testing.T){
	ni := &NullBool{}
	err := ni.Scan(nil)
	ast:=assert.New(t)
	ast.Nil(err)
	ast.Equal(ni.Bool,false)
	ast.False(ni.Valid)
}

func Test_NullBool_Scan(t *testing.T){
	ni := &NullBool{}
	err := ni.Scan(true)
	ast:=assert.New(t)
	ast.Nil(err)
	ast.Equal(ni.Bool,true)
	ast.True(ni.Valid)
}

func Test_NullBool_Value_nil(t *testing.T){
	ni := &NullBool{
		Valid: false,
	}
	d,err:=ni.Value()

	ast:=assert.New(t)
	ast.Nil(d)
	ast.Nil(err)
}

func Test_NullBool_Value(t *testing.T){
	ni := &NullBool{
		Valid: true ,
		Bool: true,
	}
	d,err:=ni.Value()

	ast:=assert.New(t)
	ast.Equal(d,true)
	ast.Nil(err)
}

func Test_NullTime_Scan_nil(t *testing.T){
	ni := &NullTime{}
	err := ni.Scan(nil)
	ast:=assert.New(t)
	ast.Nil(err)
	ast.NotNil(ni.Time)
	ast.False(ni.Valid)
}

func Test_NullTime_Scan(t *testing.T){
	ts:=time.Now()
	ni := &NullTime{}
	err := ni.Scan(ts)
	ast:=assert.New(t)
	ast.Nil(err)
	ast.Equal(ni.Time,ts)
	ast.True(ni.Valid)
}

func Test_NullTime_Value_nil(t *testing.T){
	ni := &NullTime{
		Valid: false,
	}
	d,err:=ni.Value()

	ast:=assert.New(t)
	ast.Nil(d)
	ast.Nil(err)
}

func Test_NullTime_Value(t *testing.T){
	ts := time.Now()
	ni := &NullTime{
		Valid: true ,
		Time:ts,
	}
	d,err:=ni.Value()

	ast:=assert.New(t)
	ast.Equal(d,ts)
	ast.Nil(err)
}



