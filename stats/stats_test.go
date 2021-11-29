/*
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
 */

/**
 * @Author: guobob
 * @Description:
 * @File:  stats_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/29 15:17
 */

package stats

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_AddStatic_false(t *testing.T){
	AddStatic("PacketChanLen",uint64(100),false )
	assert.New(t).Equal(Static["PacketChanLen"],uint64(100))
}

func Test_AddStatic_true(t *testing.T){
	AddStatic("ReadPacket",uint64(1),true )
	assert.New(t).Equal(Static["ReadPacket"],uint64(1))
}

func Test_AddStatic_not_exist(t *testing.T){
	AddStatic("ReadPack",uint64(1),true )
	assert.New(t).Equal(Static["ReadPack"],uint64(1))
}

func Test_DumpStatic( t *testing.T){
	str := DumpStatic()
	assert.New(t).NotEqual(len(str),0)
}