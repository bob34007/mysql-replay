/**
 * @Author: guobob
 * @Description:
 * @File:  server_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/21 17:05
 */

package cmd

import (
	"errors"
	"github.com/agiledragon/gomonkey"
	"net/http"
	"testing"
)

func Test_generateListenStr(t *testing.T) {
	type args struct {
		port uint16
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name:"generate listen string",
			args:args{
				port:4000,
			},
			want:"0.0.0.0"+":"+"4000",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := generateListenStr(tt.args.port); got != tt.want {
				t.Errorf("generateListenStr() = %v, want %v", got, tt.want)
			}
		})
	}
}

/*
func Test_HandleQueryStats_GetFileNumFromPath_fail(t *testing.T){
	w := new(http.ResponseWriter)
	r:=new(http.Request)
	//err := errors.New("get file num from path fail ")

	patch := gomonkey.ApplyFunc(util.GetFileNameSeq, func() int64{
		return 0
	})
	defer patch.Reset()
	HandleQueryStats(*w,r)

}
*/

func Test_getStatic (t *testing.T){
	qs:=new(QueryStats)
	getStatic(qs)
}

func Test_AddPortListenAndServer( t *testing.T){
	port := uint16(7002)
	outputDir :="./"
	storeDir := "./"
	err := errors.New("add listen port fail")
	patch := gomonkey.ApplyFunc(http.ListenAndServe,func (addr string, handler http.Handler) error {
		return err
	})
	defer patch.Reset()
	AddPortListenAndServer(port,outputDir,storeDir)

}

