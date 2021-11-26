package cmd

import (
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/google/gopacket/reassembly"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"time"
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



func HandlePcapFile(name string, ts time.Time, rt uint32, ticker *time.Ticker,
	assembler *reassembly.Assembler,lastFlushTime *time.Time,flushIneterval time.Duration,log *zap.Logger) error {
	fmt.Println("process file ",name)
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
			flushed,closed:=assembler.FlushCloseOlderThan(*lastFlushTime)
			log.Info(fmt.Sprintf("flush old connect fulshed:%v,closed:%v",flushed,closed))
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


func printTime(log *zap.Logger) {
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
	fileName:=""
	for k,v :=range files{
		if v != 0 {
			continue
		}
		if len(fileName) ==0 {
			fileName = k
		}
		if k < fileName{
			fileName = k
		}
	}

	files[fileName] =1

	return fileName
}

