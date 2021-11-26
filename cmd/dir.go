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
		cfg = &util.Config{RunType: util.RunDir}
	)
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay dir packet",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			cfg.PreFileSize = cfg.PreFileSize * 1024 * 1024
			cfg.Log = zap.L().Named("dir-text-replay")
			cfg.Log.Info("process begin run at " + time.Now().String())

			ts := time.Now()
			var ticker *time.Ticker

			err = cfg.CheckParamValid()
			if err !=nil{
				return err
			}


			mu := new(sync.Mutex)

			files := make(map[string]int, 0)
			err = util.GetDataFile(cfg.DataDir, files, mu)
			if err != nil {
				cfg.Log.Error("get file from dataDir fail , " + err.Error())
				return nil
			}

			go printTime(cfg.Log)

			ctx, cancel := context.WithCancel(context.Background())
			go util.WatchDirCreateFile(ctx, cfg.DataDir, files, mu, cfg.Log)

			go AddPortListenAndServer(cfg.ListenPort, cfg.OutputDir, cfg.StoreDir)

			if cfg.RunTime > 0 {
				ticker = time.NewTicker(3 * time.Second)
				defer ticker.Stop()
			}


			factory := stream.NewFactoryFromEventHandler(func(conn stream.ConnID) stream.MySQLEventHandler {
				logger := conn.Logger("replay")
				return replay.NewReplayEventHandler(conn, logger, cfg.Dsn, cfg.MySQLConfig, cfg.OutputDir, cfg.StoreDir, cfg.PreFileSize)
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
				cfg.Log.Info("process file " + fileName)
				err = HandlePcapFile(cfg.DataDir+"/"+fileName, ts , cfg.RunTime, ticker,
					assembler,&lastFlushTime,cfg.FlushInterval ,cfg.Log)
				if err != nil && err != ERRORTIMEOUT {
					cfg.Log.Error("process pcap file fail , " + err.Error())
					break
				} else if err == ERRORTIMEOUT {
					cfg.Log.Info("program run time out " + fmt.Sprintf("%v",cfg.RunTime))
					break
				}
			}
			cancel()
			cfg.Log.Info("read packet end ,begin close all goroutine")
			i := assembler.FlushAll()
			cfg.Log.Info(fmt.Sprintf("read packet end ,end close all goroutine , %v groutine",i))
			cfg.Log.Info("process end run at " + time.Now().String())
			return nil
		},
	}
	cmd.Flags().StringVarP(&cfg.Dsn, "dsn", "d", "", "replay server dsn")
	cmd.Flags().BoolVar(&options.ForceStart, "force-start", false, "accept streams even if no SYN have been seen")
	cmd.Flags().Uint32VarP(&cfg.RunTime, "runtime", "t", 0, "replay server run time")
	cmd.Flags().StringVarP(&cfg.OutputDir, "output", "o", "./output", "directory used to write the result set ")
	cmd.Flags().StringVarP(&cfg.StoreDir, "storeDir", "S", "", "save result dir")
	cmd.Flags().Uint64VarP(&cfg.PreFileSize, "filesize", "s", UINT64MAX, "Baseline size per document ,uint M")
	cmd.Flags().Uint16VarP(&cfg.ListenPort, "listen-port", "p", 7002, "http server port , Provide query statistical (query) information and exit (exit) services")
	cmd.Flags().StringVarP(&cfg.DataDir, "data-dir", "D", "./data", "directory used to read pcap file")
	cmd.Flags().DurationVar(&cfg.FlushInterval, "flush-interval", time.Minute*3, "flush interval")
	return cmd
}
