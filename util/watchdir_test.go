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
 * @File:  watchdir_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/24 14:06
 */

package util

import (
	"context"
	"go.uber.org/zap"
	"sync"
	"testing"
	"time"
)


func TestWatchDirCreateFile(t *testing.T) {

	filePath:="./"
	files:=make(map[string]int)
	var mu sync.Mutex
	log:=zap.L().Named("test")
	ctx, cancel := context.WithCancel(context.Background())
	go  func (){
		time.Sleep(time.Millisecond *100)
		cancel()
	}()
	WatchDirCreateFile(ctx,filePath,files,&mu,log)

}