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
 * @File:  online.go
 * @Version: 1.0.0
 * @Date: 2021/11/6 17:11
 */

package cmd

import (
	"fmt"
	"github.com/bobguo/mysql-replay/replay"
	"github.com/bobguo/mysql-replay/stream"
	"github.com/bobguo/mysql-replay/util"
	"github.com/go-sql-driver/mysql"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/google/gopacket/reassembly"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"strconv"
	"time"
)

const MAXPACKETLEN = 16 * 1024 * 1024

func NewOnlineCommand() *cobra.Command {
	//add sub command replay
	cmd := &cobra.Command{
		Use:   "online",
		Short: "traffic capture online",
	}
	cmd.AddCommand(NewOnlineReplayCommand())
	return cmd
}

func generateLogName( device string, port uint16) string {
	return fmt.Sprintf("%v-%s", port, device)
}

func bPFFilter(port uint16) string {
	return "tcp and port " + strconv.Itoa(int(port))
}

func trafficCapture(device string, port uint16,
	dsn, outputDir,tmpDir string, MySQLCfg *mysql.Config,
	preFileSize uint64, options stream.FactoryOptions,
	lastFlushTime *time.Time, flushInterval time.Duration,
	 runTime uint32, log *zap.Logger) error {


	var packetNum uint64
	ts:= time.Now()
	handle, err := pcap.OpenLive(device, MAXPACKETLEN, false, pcap.BlockForever)
	if err != nil {
		return err
	}
	defer handle.Close()

	//set filter
	filter := bPFFilter(port)
	log.Info("SetBPFFilter" + filter)
	err = handle.SetBPFFilter(filter)
	if err != nil {
		return err
	}

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	// Process packet here
	factory := stream.NewFactoryFromEventHandler(func(conn stream.ConnID) stream.MySQLEventHandler {
		logger := conn.Logger("replay")
		return replay.NewReplayEventHandler(conn, logger, dsn, MySQLCfg, outputDir,tmpDir, preFileSize)
	}, options)
	pool := reassembly.NewStreamPool(factory)
	assembler := reassembly.NewAssembler(pool)

	for pkt := range packetSource.Packets() {
		if time.Since(ts).Seconds() > float64(runTime *60) {
			logger.Warn("program run timeout , " + fmt.Sprintf("%v",int64(runTime *60)))
			return ERRORTIMEOUT
		}
		if meta := pkt.Metadata(); meta != nil && meta.Timestamp.Sub(*lastFlushTime) > flushInterval {
			assembler.FlushCloseOlderThan(*lastFlushTime)
			*lastFlushTime = meta.Timestamp
		}

		layer := pkt.Layer(layers.LayerTypeTCP)
		if layer == nil {
			log.Error("pkt.Layer id is nil")
			continue
		}
		tcp := layer.(*layers.TCP)
		if tcp.DstPort != layers.TCPPort(port) && tcp.SrcPort !=layers.TCPPort(port) {
			continue
		}

		packetNum ++
		if packetNum%10000==0{
			log.Warn("receive packet num : " + fmt.Sprintf("%v",packetNum))
		}
		assembler.AssembleWithContext(pkt.NetworkLayer().NetworkFlow(), tcp, captureContext(pkt.Metadata().CaptureInfo))
	}
	return nil
}

func printTime(log *zap.Logger){
	t := time.NewTicker(time.Second * 60)
	ts := time.Now()
	for {
		select{
		  case <-t.C:
			 fmt.Println(time.Now().String() +":"+ fmt.Sprintf("program run %v seconds",time.Since(ts).Seconds()))
			  default:
				  time.Sleep(time.Second *5)
		}

	}


}

func NewOnlineReplayCommand() *cobra.Command {
	//Replay sql from online net packet
	var (
		options       = stream.FactoryOptions{Synchronized: true}
		dsn           string
		runTime       uint32
		outputDir     string
		flushInterval time.Duration
		preFileSize   uint64
		deviceName    string
		srcPort       uint16
		storeDir      string
		listenPort    uint16
	)
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay pcap files",
		RunE: func(cmd *cobra.Command, args []string) error {
			preFileSize = preFileSize * 1024 * 1024
			lastFlushTime := time.Time{}
			logName := generateLogName(deviceName, srcPort)
			log := zap.L().Named(logName)
			log.Info("process begin run at " + time.Now().String())

			go printTime(log)

			ts := time.Now()

			MySQLCfg, err := util.CheckParamValid(dsn,  outputDir)
			if err != nil {
				log.Error("parse param error , " + err.Error())
				return nil
			}

			go AddPortListenAndServer(listenPort,outputDir,storeDir)
			//general packet to json
			err = trafficCapture(deviceName, srcPort, dsn, outputDir,storeDir, MySQLCfg, preFileSize,
				options, &lastFlushTime, flushInterval,  runTime, log)
			if err != nil && err != ERRORTIMEOUT{
				return err
			} else if err==ERRORTIMEOUT{
				log.Info("process time to stop ,start at :"+ts.String()+" end at :" + time.Now().String()+
					" specify running time : " + fmt.Sprintf("%vs",runTime*60 ))
				return nil
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&dsn, "dsn", "d", "", "replay server dsn")
	cmd.Flags().BoolVar(&options.ForceStart, "force-start", false, "accept streams even if no SYN have been seen")
	cmd.Flags().Uint32VarP(&runTime, "runtime", "t", 0, "replay server run time")
	cmd.Flags().StringVarP(&outputDir, "output", "o", "./output", "directory used to write the result set ")
	cmd.Flags().DurationVar(&flushInterval, "flush-interval", time.Minute, "flush interval")
	cmd.Flags().StringVarP(&deviceName, "device", "D", "eth0", "device name")
	cmd.Flags().StringVarP(&storeDir, "storeDir", "S", "./store", "save result dir")
	srcPort = *cmd.Flags().Uint16P("srcPort", "P", 4000, "server port")
	cmd.Flags().Uint64VarP(&preFileSize, "filesize", "s", UINT64MAX, "Baseline size per document ,uint M")
	cmd.Flags().Uint16VarP(&listenPort, "listen-port", "p", 7002, "http server port , Provide query statistical (query) information and exit (exit) services")
	return cmd
}
