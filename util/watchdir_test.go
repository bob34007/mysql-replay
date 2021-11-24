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