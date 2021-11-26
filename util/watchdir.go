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
 * @File:  watchdir.go
 * @Version: 1.0.0
 * @Date: 2021/11/9 16:32
 */

package util

import (
	"context"
	"github.com/radovskyb/watcher"
	"go.uber.org/zap"

	"sync"
	"time"
)

func WatchDirCreateFile(ctx context.Context, filePath string,
	files map[string]int, mu *sync.Mutex, log *zap.Logger) {
	w := watcher.New()

	// SetMaxEvents to 1 to allow at most 1 event's to be received
	// on the Event channel per watching cycle.
	//
	// If SetMaxEvents is not set, the default is to send all events.
	//w.SetMaxEvents(1)

	// Only notify rename and move events.
	w.FilterOps(watcher.Create)

	go func()  {
		for {
			select {
			case event := <-w.Event:
				mu.Lock()
				files[event.FileInfo.Name()]=0
				mu.Unlock()
			case err := <-w.Error:
				log.Error(err.Error())
				continue
			case <-w.Closed:
				return
			case <-ctx.Done():
				w.Close()
				return
			}
		}
	}()

	// Watch this folder for changes.
	if err := w.Add(filePath); err != nil {
		log.Error(err.Error())
		panic(err)
	}

	// Start the watching process - it'll check for changes every 100ms.
	if err := w.Start(time.Millisecond * 100); err != nil {
		log.Error(err.Error())
		panic(err)
	}

	return
}
