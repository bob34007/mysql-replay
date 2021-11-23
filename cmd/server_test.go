/**
 * @Author: guobob
 * @Description:
 * @File:  server_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/21 17:05
 */

package cmd

import (
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
