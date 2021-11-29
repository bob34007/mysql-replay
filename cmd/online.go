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
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/google/gopacket/reassembly"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"time"
)

//const MAXPACKETLEN = 16 * 1024 * 1024

func NewOnlineCommand() *cobra.Command {
	//add sub command replay
	cmd := &cobra.Command{
		Use:   "online",
		Short: "traffic capture online",
	}
	cmd.AddCommand(NewOnlineReplayCommand())
	return cmd
}

func generateLogName(device string, port uint16) string {
	return fmt.Sprintf("%v-%s", port, device)
}

func getFilter(port uint16) string {
	filter := fmt.Sprintf("tcp and ((src port %v) or (dst port %v))", port, port)
	return filter
}

func trafficCapture(cfg *util.Config,options stream.FactoryOptions) error {

	var packetNum uint64
	//var InvalidMsgPktNum uint64
	ts := time.Now()
	handle, err := pcap.OpenLive(cfg.DeviceName, 65535, false, pcap.BlockForever)
	if err != nil {
		return err
	}
	defer handle.Close()

	//set filter
	filter := getFilter(cfg.SrcPort)
	cfg.Log.Info("SetBPFFilter " + filter)
	err = handle.SetBPFFilter(filter)
	if err != nil {
		return err
	}

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())

	// Process packet here
	factory := stream.NewFactoryFromEventHandler(func(conn stream.ConnID) stream.MySQLEventHandler {
		logger := conn.Logger("replay")
		return replay.NewReplayEventHandler(conn, logger, cfg)
	}, options)

	pool := reassembly.NewStreamPool(factory)
	assembler := reassembly.NewAssembler(pool)

	packets := packetSource.Packets()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case pkt := <-packets:
			if pkt.NetworkLayer() == nil || pkt.TransportLayer() == nil {
				continue
			}
			layer := pkt.Layer(layers.LayerTypeTCP)
			if layer == nil {
				cfg.Log.Error("pkt.Layer is nil")
				continue
			}
			tcp := layer.(*layers.TCP)

			packetNum++
			if packetNum%100000 == 0 {
				cfg.Log.Warn("receive packet num : " + fmt.Sprintf("%v", packetNum))
			}
			assembler.AssembleWithContext(pkt.NetworkLayer().NetworkFlow(), tcp,
				captureContext(pkt.Metadata().CaptureInfo))

		case <-ticker.C:
			if time.Since(ts).Seconds() > float64(cfg.RunTime*60) {
				cfg.Log.Warn("program run timeout , " + fmt.Sprintf("%v", int64(cfg.RunTime*60)))
				return ERRORTIMEOUT
			}

			stats, err := handle.Stats()
			cfg.Log.Warn(fmt.Sprintf("flushing all streams that haven't seen"+
				" packets in the last 2 minutes, pcap stats: %+v %v", stats, err))
			flushed, closed := assembler.FlushCloseOlderThan(time.Now().Add(-2*time.Minute))
			cfg.Log.Warn(fmt.Sprintf("flushed old connect %v-%v", flushed, closed))
		default :
			//
		}

	}

	//return nil
}


func NewOnlineReplayCommand() *cobra.Command {
	//Replay sql from online net packet
	var (
		options       = stream.FactoryOptions{Synchronized: true}
		cfg = &util.Config{RunType: util.RunOnline}
	)
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay online packet",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.PreFileSize = cfg.PreFileSize * 1024 * 1024
			logName := generateLogName(cfg.DeviceName, cfg.SrcPort)
			cfg.Log = zap.L().Named(logName)
			cfg.Log.Info("process begin run at " + time.Now().String())

			ts := time.Now()

			err := cfg.CheckParamValid()
			if err != nil {
				cfg.Log.Error("parse param error , " + err.Error())
				return nil
			}

			go printTime(cfg.Log)
			go AddPortListenAndServer(cfg.ListenPort, cfg.OutputDir, cfg.StoreDir)
			//handle online packet
			err = trafficCapture(cfg,options)
			if err != nil && err != ERRORTIMEOUT {
				return err
			} else if err == ERRORTIMEOUT {
				cfg.Log.Info("process time to stop ,start at :" + ts.String() + " end at :" + time.Now().String() +
					" specify running time : " + fmt.Sprintf("%vs", cfg.RunTime*60))
				return nil
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&cfg.Dsn, "dsn", "d", "", "replay server dsn")
	cmd.Flags().BoolVar(&options.ForceStart, "force-start", false, "accept streams even if no SYN have been seen")
	cmd.Flags().Uint32VarP(&cfg.RunTime, "runtime", "t", 0, "replay server run time")
	cmd.Flags().StringVarP(&cfg.OutputDir, "output", "o", "./output", "directory used to write the result set ")
	//cmd.Flags().DurationVar(&flushInterval, "flush-interval", time.Minute*10, "flush interval")
	cmd.Flags().StringVarP(&cfg.DeviceName, "device", "D", "eth0", "device name")
	cmd.Flags().StringVarP(&cfg.StoreDir, "storeDir", "S", "", "save result dir")
	cmd.Flags().Uint16VarP(&cfg.SrcPort, "srcPort", "P", 4000, "server port")
	cmd.Flags().Uint64VarP(&cfg.PreFileSize, "filesize", "s", UINT64MAX, "Baseline size per document ,uint M")
	cmd.Flags().Uint16VarP(&cfg.ListenPort, "listen-port", "p", 7002, "http server port , Provide query statistical (query) information and exit (exit) services")
	return cmd
}
