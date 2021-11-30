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
 * @File:  stats.go
 * @Version: 1.0.0
 * @Date: 2021/11/29 14:46
 */

package stats

import (
	"fmt"
	"sync"
)

var (
	Static  map[string]uint64
	Mu sync.RWMutex
)

func init(){
	Static = make(map[string]uint64)
	Static["ReadPacket"]=0
	Static["DealPacket"]=0
	Static["GetSQL"]=0
	Static["DealSQL"]=0
	Static["GetRes"]=0
	Static["WriteRes"]=0
	Static["PacketChanLen"]=0
	Static["SQLChanLen"]=0
	Static["WriteResChanLen"]=0
	Static["ExecSQLFail"]=0
	Static["WriteResFileFail"]=0
	Static["FormatJsonFail"]=0
}

func AddStatic(key string , value uint64, replace bool){
	Mu.Lock()
	defer Mu.Unlock()
	if v,ok := Static[key]; !ok{
		Static[key]=value
	} else {
		if !replace {
			Static[key] = v + value
		}else {
			Static[key] = value
		}
	}
}

func DumpStatic()  string  {
	var staticStr string
	for k,v := range Static {
		kvStr := fmt.Sprintf("%v-%v\n",k,v)
		staticStr+=kvStr
	}
	return staticStr
}

func GetValue (key string )uint64{
	Mu.RLock()
	defer Mu.RUnlock()
	if v,ok:=Static[key];!ok{
		return 0
	} else {
		return v
	}
}