package cmd

import (
	"fmt"
	"time"

	"github.com/bobguo/mysql-replay/sqlreplay"
	"github.com/bobguo/mysql-replay/stream"
	"github.com/bobguo/mysql-replay/util"
	"github.com/google/gopacket/reassembly"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var ERRORTIMEOUT = errors.New("replay runtime out")

func GenerateFileSeqString(seq int) string {
	str := fmt.Sprintf("-%v", seq)
	return str
}

func NewTextDumpReplayCommand() *cobra.Command {
	//Replay sql from pcap files，and compare reslut from pcap file and
	//replay server
	var (
		options = stream.FactoryOptions{Synchronized: true}
		cfg     = &util.Config{RunType: util.RunText}
	)
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay pcap files",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			cfg.PreFileSize = cfg.PreFileSize * 1024 * 1024
			log := zap.L().Named("text-replay")
			cfg.Log.Info("process begin run at " + time.Now().String())
			if len(args) == 0 {
				return cmd.Help()
			}

			err = cfg.CheckParamValid()
			if err != nil {
				log.Error("parse param error , " + err.Error())
				return nil
			}

			go printTime()
			go AddPortListenAndServer(cfg.ListenPort, cfg.OutputDir, cfg.StoreDir)

			factory := stream.NewFactoryFromEventHandler(func(conn stream.ConnID) stream.MySQLEventHandler {
				logger := conn.Logger("replay")
				return sqlreplay.NewReplayEventHandler(conn, logger, cfg)
			}, options)
			pool := reassembly.NewStreamPool(factory)
			assembler := reassembly.NewAssembler(pool)

			lastFlushTime := time.Time{}

			for _, in := range args {
				zap.L().Info("processing " + in)
				err = HandlePcapFileByText(in, assembler, &lastFlushTime, cfg.FlushInterval, cfg.Log)
				if err != nil {
					return err
				}
			}

			log.Info("read packet end ,begin close all goroutine")
			i := assembler.FlushAll()
			log.Info(fmt.Sprintf("read packet end ,end close all goroutine , %v groutine", i))
			log.Info("process end run at " + time.Now().String())
			return nil
		},
	}

	cfg.ParseFlagForRunText(cmd.Flags())
	cmd.Flags().BoolVar(&options.ForceStart, "force-start", false, "accept streams even if no SYN have been seen")
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
