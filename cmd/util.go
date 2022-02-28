package cmd

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/bobguo/mysql-replay/util"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/google/gopacket/reassembly"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

func captureContext(ci gopacket.CaptureInfo) *Context {
	return &Context{ci}
}

type Context struct {
	CaptureInfo gopacket.CaptureInfo
}

func (c *Context) GetCaptureInfo() gopacket.CaptureInfo {
	return c.CaptureInfo
}

func handlePcapFile(ctx context.Context, name string, cfg *util.Config, assembler *reassembly.Assembler,
	lastFlushTime *time.Time, errChan chan error, handleFileNum *int32) {
	cfg.Log.Info("process file " + name)
	var f *pcap.Handle
	var err error
	f, err = pcap.OpenOffline(name)
	if err != nil {
		cfg.Log.Error("open pcap file fail " + err.Error())
		errChan <- err
		return
	}
	defer f.Close()
	defer atomic.AddInt32(handleFileNum, -1)
	pkts := gopacket.NewPacketSource(f, f.LinkType()).Packets()
	for {
		select {
		case pkt, ok := <-pkts:
			if !ok {
				errChan <- nil
				return
			}
			if meta := pkt.Metadata(); meta != nil && meta.Timestamp.Sub(*lastFlushTime) > cfg.FlushInterval {
				flushed, closed := assembler.FlushCloseOlderThan(*lastFlushTime)
				cfg.Log.Info(fmt.Sprintf("flush old connect fulshed:%v,closed:%v", flushed, closed))
				*lastFlushTime = meta.Timestamp
			}

			layer := pkt.Layer(layers.LayerTypeTCP)
			if layer != nil {
				tcp := layer.(*layers.TCP)
				assembler.AssembleWithContext(pkt.NetworkLayer().NetworkFlow(), tcp, captureContext(pkt.Metadata().CaptureInfo))
			}

		case <-ctx.Done():
			cfg.Log.Info("the program will exit ")
			errChan <- nil
			return
		}
	}
}

func HandlePcapFile(name string, assembler *reassembly.Assembler, lastFlushTime *time.Time,
	flushInterval time.Duration, log *zap.Logger) error {
	fmt.Println("process file ", name)

	var f *pcap.Handle
	var err error
	f, err = pcap.OpenOffline(name)
	if err != nil {
		log.Error("open pcap file fail " + err.Error())
		return errors.Annotate(err, "open "+name)
	}
	defer f.Close()

	src := gopacket.NewPacketSource(f, f.LinkType())
	for pkt := range src.Packets() {
		if meta := pkt.Metadata(); meta != nil && meta.Timestamp.Sub(*lastFlushTime) > flushInterval {
			flushed, closed := assembler.FlushCloseOlderThan(*lastFlushTime)
			log.Info(fmt.Sprintf("flush old connect fulshed:%v,closed:%v", flushed, closed))
			*lastFlushTime = meta.Timestamp
		}

		layer := pkt.Layer(layers.LayerTypeTCP)
		if layer != nil {
			tcp := layer.(*layers.TCP)
			assembler.AssembleWithContext(pkt.NetworkLayer().NetworkFlow(), tcp, captureContext(pkt.Metadata().CaptureInfo))
		}
	}
	return nil
}

func printTime() {
	t := time.NewTicker(time.Second * 60)
	ts := time.Now()
	for {
		select {
		case <-t.C:
			fmt.Println(time.Now().String() + ":" + fmt.Sprintf("program run %v seconds", time.Since(ts).Seconds()))
		default:
			time.Sleep(time.Second * 5)
		}

	}

}

func getFirstFileName(files map[string]int) string {
	fileName := ""
	for k, v := range files {
		if v != 0 {
			continue
		}
		if len(fileName) == 0 {
			fileName = k
		}
		if k < fileName {
			fileName = k
		}
	}

	if len(fileName) > 0 {
		files[fileName] = 1
	}

	return fileName
}

func HandleSigs(sigs chan os.Signal, exits chan bool) {
	<-sigs
	exits <- true
}
