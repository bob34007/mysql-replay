/*******************************************************************************
 * Copyright (c)  2021 PingCAP, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/

/**
 * @Author: guobob
 * @Description:
 * @File:  online_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/8 21:35
 */

package cmd

import "testing"

func Test_generateLogName(t *testing.T) {
	type args struct {
		device string
		port   uint16
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "generateLogName",
			args: args{
				device: "eth0",
				port:   4000,
			},
			want: "4000-eth0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := generateLogName( tt.args.device, tt.args.port); got != tt.want {
				t.Errorf("generateLogName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getFilter(t *testing.T) {
	type args struct {
		port uint16
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test getFilter",
			args: args{
				port: 4000,
			},
			want: "tcp and ((src port 4000) or (dst port 4000))",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getFilter(tt.args.port); got != tt.want {
				t.Errorf("bPFFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}
