/**
 * @Author: guobob
 * @Description:
 * @File:  util_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/21 16:55
 */

package cmd

import (
	"github.com/google/gopacket"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func Test_getFirstFileName(t *testing.T) {

	mfile := make(map[string]int )
	mfile["test1"] = 0
	mfile["test2"] = 0

	mfile1 := make(map[string]int)
	mfile1["test1"]=1
	mfile1["test3"]=0
	mfile1["test2"]=0

	type args struct {
		files map[string]int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name : "get first fileName-test1",
			args : args{
				files:mfile,
			},
			want : "test1",
		},
		{
			name : "get first fileName-test2",
			args : args{
				files:mfile1,
			},
			want : "test2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getFirstFileName(tt.args.files); got != tt.want {
				t.Errorf("getFirstFileName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_captureContext (t *testing.T){
	ci := new(gopacket.CaptureInfo)
	res := captureContext(*ci)
	assert.New(t).NotNil(res)
}

func Test_GetCaptureInfo (t *testing.T){
	ci := &Context{
		CaptureInfo: *new(gopacket.CaptureInfo),
	}
	res := ci.GetCaptureInfo()
	assert.New(t).NotNil(res)
}


type ForTest struct{}

func (f ForTest)String() string{
	return ""
}
func (f ForTest)Signal(){
	return
}
func Test_HandleSigs ( t *testing.T){
	f :=ForTest{}
	sigs := make(chan os.Signal,1)
	exits:=make(chan bool ,1 )
	sigs <- f

	HandleSigs(sigs,exits)
}