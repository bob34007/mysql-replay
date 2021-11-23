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
	"github.com/bobguo/mysql-replay/replay"
	"github.com/bobguo/mysql-replay/stream"
	"github.com/bobguo/mysql-replay/util"
	"github.com/google/gopacket/reassembly"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"sync"
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
		options   = stream.FactoryOptions{Synchronized: true}
		dsn       string
		runTime   uint32
		outputDir string
		//flushInterval time.Duration
		preFileSize uint64
		storeDir    string
		listenPort  uint16
		dataDir     string
		flushInterval time.Duration
	)
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay dir packet",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			preFileSize = preFileSize * 1024 * 1024
			log := zap.L().Named("dir-text-replay")
			log.Info("process begin run at " + time.Now().String())

			ts := time.Now()
			var ticker *time.Ticker
			MySQLCfg , err := util.CheckParamValid(dsn, outputDir,storeDir)
			if err != nil {
				log.Error("parse param error , " + err.Error())
				return nil
			}

			mu := new(sync.Mutex)

			files := make(map[string]int, 0)
			err = util.GetDataFile(dataDir, files, mu)
			if err != nil {
				log.Error("get file from dataDir fail , " + err.Error())
				return nil
			}
			//fmt.Println(files)

			go printTime(log)

			ctx, cancel := context.WithCancel(context.Background())
			go util.WatchDirCreateFile(ctx, dataDir, files, mu, log)

			go AddPortListenAndServer(listenPort, outputDir, storeDir)

			if runTime > 0 {
				ticker = time.NewTicker(3 * time.Second)
				defer ticker.Stop()
			}


			factory := stream.NewFactoryFromEventHandler(func(conn stream.ConnID) stream.MySQLEventHandler {
				logger := conn.Logger("replay")
				return replay.NewReplayEventHandler(conn, logger, dsn, MySQLCfg, outputDir, storeDir, preFileSize)
			}, options)

			pool := reassembly.NewStreamPool(factory)
			assembler := reassembly.NewAssembler(pool)
			lastFlushTime := time.Time{}


			fileName := ""
			for {
				mu.Lock()
					fileName = getFirstFileName(files)
					if len(fileName) == 0 {
						mu.Unlock()
						continue
					}
				mu.Unlock()
				log.Info("process file " + fileName)
				err := HandlePcapFile(dataDir+"/"+fileName, ts , runTime, ticker,
					assembler,&lastFlushTime,flushInterval )
				if err != nil && err != ERRORTIMEOUT {
					return err
				} else if err == ERRORTIMEOUT {
					break
				}
			}
			cancel()
			log.Info("read packet end ,begin close all goroutine")
			i := assembler.FlushAll()
			log.Info(fmt.Sprintf("read packet end ,end close all goroutine , %v groutine",i))
			log.Info("process end run at " + time.Now().String())
			return nil
		},
	}
	cmd.Flags().StringVarP(&dsn, "dsn", "d", "", "replay server dsn")
	cmd.Flags().BoolVar(&options.ForceStart, "force-start", false, "accept streams even if no SYN have been seen")
	cmd.Flags().Uint32VarP(&runTime, "runtime", "t", 0, "replay server run time")
	cmd.Flags().StringVarP(&outputDir, "output", "o", "./output", "directory used to write the result set ")
	cmd.Flags().StringVarP(&storeDir, "storeDir", "S", "", "save result dir")
	cmd.Flags().Uint64VarP(&preFileSize, "filesize", "s", UINT64MAX, "Baseline size per document ,uint M")
	cmd.Flags().Uint16VarP(&listenPort, "listen-port", "p", 7002, "http server port , Provide query statistical (query) information and exit (exit) services")
	cmd.Flags().StringVarP(&dataDir, "data-dir", "D", "./data", "directory used to read pcap file")
	cmd.Flags().DurationVar(&flushInterval, "flush-interval", time.Minute, "flush interval")
	return cmd
}
