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
 * @File:  dir.go
 * @Version: 1.0.0
 * @Date: 2021/11/19 17:17
 */

package cmd

import (
	"context"
	"fmt"
	"github.com/bobguo/mysql-replay/sqlreplay"
	"github.com/bobguo/mysql-replay/stats"
	"github.com/bobguo/mysql-replay/stream"
	"github.com/bobguo/mysql-replay/util"
	"github.com/google/gopacket/reassembly"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func NewDirTextCommand() *cobra.Command {
	//add sub command replay
	cmd := &cobra.Command{
		Use:   "dir",
		Short: "dir Text format utilities",
	}
	cmd.AddCommand(NewDirTextDumpReplayCommand())
	return cmd
}

func NewDirTextDumpReplayCommand() *cobra.Command {
	//Replay sql from dir
	var (
		options = stream.FactoryOptions{Synchronized: true}
		cfg     = &util.Config{RunType: util.RunDir}
	)
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay dir packet",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			ts := time.Now()
			var exit bool
			cfg.PreFileSize = cfg.PreFileSize * 1024 * 1024
			cfg.Log = zap.L().Named("dir-text-replay")
			cfg.Log.Info("process begin run at " + time.Now().String())
			err = cfg.CheckParamValid()
			if err != nil {
				cfg.Log.Error("check param fail , " + err.Error())
				return err
			}

			mu := new(sync.Mutex)
			files := make(map[string]int, 0)
			err = util.GetDataFile(cfg.DataDir, files, mu)
			if err != nil {
				cfg.Log.Error("get file from dataDir fail , " + err.Error())
				return err
			}

			go printTime()

			ctx, cancel := context.WithCancel(context.Background())
			go util.WatchDirCreateFile(ctx, cfg.DataDir, files, mu, cfg.Log)
			go AddPortListenAndServer(cfg.ListenPort, cfg.OutputDir, cfg.StoreDir)

			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()

			factory := stream.NewFactoryFromEventHandler(func(conn stream.ConnID) stream.MySQLEventHandler {
				logger := conn.Logger("replay")
				return sqlreplay.NewReplayEventHandler(conn, logger, cfg)
			}, options)

			pool := reassembly.NewStreamPool(factory)
			assembler := reassembly.NewAssembler(pool)
			lastFlushTime := time.Time{}

			fileName := ""
			errChan := make(chan error, 1)
			sigs := make(chan os.Signal, 1)
			exitChan := make(chan bool, 1)
			signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
			go HandleSigs(sigs,exitChan)

			handleFileNum := int32(0)

			for {
				if atomic.LoadInt32(&handleFileNum) == 0 {

					mu.Lock()
					fileName = getFirstFileName(files)
					if len(fileName) > 0 {
						go handlePcapFile(ctx, cfg.DataDir+"/"+fileName, cfg,
							assembler, &lastFlushTime, errChan, &handleFileNum)
						atomic.AddInt32(&handleFileNum, 1)
					}
					mu.Unlock()
				}

				select {
				case err = <-errChan:
					if err != nil {
						cfg.Log.Error(fmt.Sprintf("handle file %s fail ,%v",
							cfg.DataDir+"/"+fileName, err))
						cancel()
						goto LOOP
					}
				case <-ticker.C:
					if time.Since(ts).Seconds() > float64(cfg.RunTime*60) {
						cancel()
						goto LOOP
					}
				case exit = <-exitChan:
					cancel()
					goto LOOP
				}
			}
		LOOP:
			//sleep 200ms and wait for other go routine to exit
			time.Sleep(time.Millisecond *200 )
			cfg.Log.Info("read packet end ,begin close all goroutine")
			if exit == false {
				i := assembler.FlushAll()
				cfg.Log.Info(fmt.Sprintf("read packet end ,end close all goroutine , %v groutine", i))
			}
			cfg.Log.Info(stats.DumpStatic())
			cfg.Log.Info("process end run at " + time.Now().String())
			return err
		},
	}

	cfg.ParseFlagForRunDir(cmd.Flags())
	cmd.Flags().BoolVar(&options.ForceStart, "force-start", false, "accept streams even if no SYN have been seen")
	return cmd
}
