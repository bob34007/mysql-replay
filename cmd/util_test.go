/**
 * @Author: guobob
 * @Description:
 * @File:  util_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/21 16:55
 */

package cmd

import (
	"testing"
)

func Test_getFirstFileName(t *testing.T) {

	mfile := make(map[string]int )
	mfile["test1"] = 0
	mfile["test2"] = 0

	mfile1 := make(map[string]int)
	mfile1["test1"]=1
	mfile1["test2"]=0
	mfile1["test3"]=0

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



