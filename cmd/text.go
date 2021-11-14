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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"time"
)

var UINT64MAX uint64= 1<<64 -1
var ERRORTIMEOUT = errors.New("replay runtime out")

func HandlePcapFile(name string, ts time.Time, rt uint32, ticker *time.Ticker,
	assembler *reassembly.Assembler,lastFlushTime *time.Time,flushIneterval time.Duration) error {
	var f *pcap.Handle
	var err error
	f, err = pcap.OpenOffline(name)
	if err != nil {
		return errors.Annotate(err, "open "+name)
	}
	defer f.Close()
	//logger := zap.L().With(zap.String("conn", "compare"))
	src := gopacket.NewPacketSource(f, f.LinkType())
	for pkt := range src.Packets() {
		if meta := pkt.Metadata(); meta != nil && meta.Timestamp.Sub(*lastFlushTime) > flushIneterval {
			assembler.FlushCloseOlderThan(*lastFlushTime)
			*lastFlushTime = meta.Timestamp
		}

		layer := pkt.Layer(layers.LayerTypeTCP)
		if layer == nil {
			continue
		}
		tcp := layer.(*layers.TCP)
		assembler.AssembleWithContext(pkt.NetworkLayer().NetworkFlow(), tcp, captureContext(pkt.Metadata().CaptureInfo))
		if rt > 0 {
			select {
			case <-ticker.C:
				if time.Since(ts).Seconds() > float64(rt*60) {
					return ERRORTIMEOUT
				}
			default:
				//
			}
		}
	}
	return nil
}


func GenerateFileSeqString(seq int ) string {
	str := fmt.Sprintf("-%v",seq)
	return str
}



func NewTextDumpReplayCommand() *cobra.Command {
	//Replay sql from pcap files，and compare reslut from pcap file and
	//replay server
	var (
		options   = stream.FactoryOptions{Synchronized: true}
		dsn       string
		runTime   uint32
		outputDir string
		flushInterval time.Duration
		preFileSize uint64
		listenPort uint16
	)
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay pcap files",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			preFileSize = preFileSize *1024 *1024
			log.Info("process begin run at " + time.Now().String())
			if len(args) == 0 {
				return cmd.Help()
			}

			ts := time.Now()
			var ticker *time.Ticker
			MySQLCfg, err := util.CheckParamValid(dsn,  outputDir)
			if err != nil {
				log.Error("parse param error , " + err.Error())
				return nil
			}

			go AddPortListenAndServer(listenPort,outputDir,"")

			if runTime > 0 {
				ticker = time.NewTicker(3 * time.Second)
				defer ticker.Stop()
			}

			factory := stream.NewFactoryFromEventHandler(func(conn stream.ConnID) stream.MySQLEventHandler {
				logger := conn.Logger("replay")
				return replay.NewReplayEventHandler(conn,logger,dsn,MySQLCfg,outputDir,"",preFileSize)
			}, options)

			pool := reassembly.NewStreamPool(factory)
			assembler := reassembly.NewAssembler(pool)

			lastFlushTime := time.Time{}

			for _, in := range args {
				zap.L().Info("processing " + in)
				err = HandlePcapFile(in, ts, runTime, ticker, assembler,&lastFlushTime,flushInterval)
				if err != nil && err != ERRORTIMEOUT {
					return err
				} else if err == ERRORTIMEOUT {
					break
				}
				//assembler.FlushCloseOlderThan(factory.LastStreamTime().Add(-3 * time.Minute))
			}
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
	cmd.Flags().StringVarP(&outputDir, "output", "o", "./output", "directory used to write the result set")
	cmd.Flags().DurationVar(&flushInterval, "flush-interval", time.Minute, "flush interval")
	cmd.Flags().Uint64VarP(&preFileSize,"filesize","s",UINT64MAX,"Baseline size per document , unit M")
	cmd.Flags().Uint16VarP(&listenPort, "listen-port", "P", 7002, "http server port , Provide query statistical (query) information and exit (exit) services")

	return cmd
}


func NewTextCommand() *cobra.Command {
	//add sub command replay
	cmd := &cobra.Command{
		Use:   "text",
		Short: "Text format utilities",
	}
	cmd.AddCommand(NewTextDumpReplayCommand())
	return cmd
}
