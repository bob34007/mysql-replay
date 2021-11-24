/**
 * @Author: guobob
 * @Description:
 * @File:  errors_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/24 11:53
 */

package stream

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMySQLError_Error(t *testing.T) {
	type fields struct {
		Number  uint16
		Message string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name :"test error",
			fields:fields{
				Number: 1062,
				Message: "duplicate primary key",
			},
			want:fmt.Sprintf("Error %d: %s", 1062, "duplicate primary key"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			me := &MySQLError{
				Number:  tt.fields.Number,
				Message: tt.fields.Message,
			}
			if got := me.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMySQLError_Is(t *testing.T) {
	type fields struct {
		Number  uint16
		Message string
	}
	type args struct {
		err error
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name:"not MySQLError",
			fields:fields{
				Number: 1062,
				Message: "duplicate primary key",
			},
			args:args{
				err:errors.New("errors happen"),
			},
			want:false ,
		},
		{
			name:"is MySQLError",
			fields:fields{
				Number: 1062,
				Message: "duplicate primary key",
			},
			args:args{
				err: &MySQLError{
					Number: 1062,
					Message: "duplicate primary key",
				},
			},
			want:true ,
		},
	}
		for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			me := &MySQLError{
				Number:  tt.fields.Number,
				Message: tt.fields.Message,
			}
			if got := me.Is(tt.args.err); got != tt.want {
				t.Errorf("Is() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_SetLogger_nil(t *testing.T){
	err := SetLogger(nil)
	assert.New(t).NotNil(err)
}

type LOG struct {}
func (l *LOG)Print(v ...interface{}){
	fmt.Println("")
}

func Test_SetLogger(t *testing.T){
	logger:=&LOG{}

	err:= SetLogger(logger)

	ast:=assert.New(t)
	ast.Nil(err)
}