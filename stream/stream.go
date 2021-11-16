package stream

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/reassembly"
	"go.uber.org/zap"
)

type MySQLPacket struct {
	Conn ConnID
	Time time.Time
	Dir  reassembly.TCPFlowDirection
	Len  int
	Seq  int
	Data []byte
}

type ConnID [2]gopacket.Flow

func (k ConnID) SrcAddr() string {
	return k[0].Src().String() + ":" + k[1].Src().String()
}

func (k ConnID) DstAddr() string {
	return k[0].Dst().String() + ":" + k[1].Dst().String()
}

func (k ConnID) String() string {
	return k.SrcAddr() + "->" + k.DstAddr()
}

func (k ConnID) Reverse() ConnID {
	return ConnID{k[0].Reverse(), k[1].Reverse()}
}

func (k ConnID) Hash() uint64 {
	h := fnvHash(k[0].Src().Raw(), k[1].Src().Raw()) + fnvHash(k[0].Dst().Raw(), k[1].Dst().Raw())
	h ^= uint64(k[0].EndpointType())
	h *= fnvPrime
	h ^= uint64(k[1].EndpointType())
	h *= fnvPrime
	return h
}

func (k ConnID) HashStr() string {
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], k.Hash())
	return hex.EncodeToString(buf[:])
}

func (k ConnID) Logger(name string) *zap.Logger {
	logger := zap.L().With(zap.String("conn", k.HashStr()+":"+k.SrcAddr()))
	if len(name) > 0 {
		logger = logger.Named(name)
	}
	return logger
}

type FactoryOptions struct {
	ConnCacheSize uint
	Synchronized  bool
	ForceStart    bool
}

func NewFactoryFromPacketHandler(factory func(ConnID) MySQLPacketHandler, opts FactoryOptions) *mysqlStreamFactory {
	if factory == nil {
		factory = defaultHandlerFactory
	}
	return &mysqlStreamFactory{new: factory, opts: opts}
}

var _ reassembly.StreamFactory = &mysqlStreamFactory{}

type mysqlStreamFactory struct {
	new  func(key ConnID) MySQLPacketHandler
	opts FactoryOptions

	//ts time.Time
}

//func (f *mysqlStreamFactory) LastStreamTime() time.Time { return f.ts }

func (f *mysqlStreamFactory) New(netFlow, tcpFlow gopacket.Flow, tcp *layers.TCP, ac reassembly.AssemblerContext) reassembly.Stream {
	conn := ConnID{netFlow, tcpFlow}
	log := conn.Logger("mysql-stream")
	h, ch, done := f.new(conn), make(chan MySQLPacket, f.opts.ConnCacheSize), make(chan struct{})
	if !f.opts.Synchronized {
		go func() {
			defer close(done)
			for pkt := range ch {
				h.OnPacket(pkt)
			}
		}()
	}
	/*if ac != nil && f.ts.Sub(ac.GetCaptureInfo().Timestamp) < 0 {
		f.ts = ac.GetCaptureInfo().Timestamp
	}*/
	//stats.Add(stats.Streams, 1)
	return &mysqlStream{
		conn: conn,
		log:  log,
		ch:   ch,
		done: done,
		h:    h,
		opts: f.opts,
	}
}

var _ reassembly.Stream = &mysqlStream{}

type mysqlStream struct {
	conn ConnID

	log  *zap.Logger
	buf0 *bytes.Buffer
	buf1 *bytes.Buffer
	pkt0 *MySQLPacket
	pkt1 *MySQLPacket
	t0   time.Time
	t1   time.Time

	ch   chan MySQLPacket
	done chan struct{}

	h    MySQLPacketHandler
	opts FactoryOptions
}

func (s *mysqlStream) Accept(tcp *layers.TCP, ci gopacket.CaptureInfo, dir reassembly.TCPFlowDirection, nextSeq reassembly.Sequence, start *bool, ac reassembly.AssemblerContext) bool {
	if !s.h.Accept(ci, dir, tcp) {
		return false
	}
	if s.opts.ForceStart {
		*start = true
	}
	return true
}

func (s *mysqlStream) ReassembledSG(sg reassembly.ScatterGather, ac reassembly.AssemblerContext) {
	length, _ := sg.Lengths()
	if length == 0 {
		s.log.Warn("get packet data len is zero")
		return
	}

	data := sg.Fetch(length)
	dir, _, _, skip := sg.Info()
	buf := s.getBuf(dir)
	ts := s.getTime(dir)

	if ac != nil {
		t := ac.GetCaptureInfo().Timestamp
		if ts.Sub(t) < 0 {
			s.setTime(dir, t)
			ts = t
		}
	}

	if skip < 0 {
		s.log.Warn("trim duplicated data", zap.String("dir", dir.String()), zap.Int("size", -skip))
		if -skip >= len(data) {
			s.log.Warn("trim too much data", zap.String("dir", dir.String()), zap.Int("size", -skip), zap.Int("data-size", len(data)))
			return
		}
		data = data[-skip:]
	}

	if buf == nil {
		buf = bytes.NewBuffer(data)
		if seq := lookupPacketSeq(buf); s.getBuf(!dir) == nil && seq != 0 {
			s.log.Warn("drop init packet with non-zero seq",
				zap.String("dir", dir.String()), zap.String("data", formatData(data)))
			return
		}
	} else {
		if skip > 0 {
			s.log.Warn("fill skipped data", zap.String("dir", dir.String()), zap.Int("size", skip))
			buf.Grow(skip)
			buf.Write(make([]byte, skip))
		}
		buf.Write(data)
	}
	s.setBuf(dir, buf)

	cnt := 0
	for buf.Len() > 0 {
		pkt := s.getPkt(dir)
		if pkt == nil {
			pkt = &MySQLPacket{
				Conn: s.conn,
				Time: ts,
				Dir:  dir,
				Len:  lookupPacketLen(buf),
				Seq:  lookupPacketSeq(buf),
			}
		}
		if pkt.Seq == -1 || buf.Len() < pkt.Len+4 {
			s.log.Warn("wait for more packet data", zap.String("dir", dir.String()),
				zap.String("msg" ,fmt.Sprintf("pkt seq is %v,pkt len %v,buf len %v",
					pkt.Seq,pkt.Len,buf.Len())))
			if s.getPkt(dir) == nil && pkt.Seq >= 0 {
				s.setPkt(dir, pkt)
			}
			if ac == nil && cnt > 0 {
				s.log.Warn("fallback to last seen time",
					zap.String("dir", dir.String()), zap.Int("packets", cnt), zap.Time("time", ts))
			}
			return
		}
		pkt.Data = make([]byte, pkt.Len)
		copy(pkt.Data, buf.Next(pkt.Len + 4)[4:])
		cnt += 1
		//stats.Add(stats.Packets, 1)
		if s.opts.Synchronized {
			s.h.OnPacket(*pkt)
		} else {
			s.ch <- *pkt
		}
		s.setPkt(dir, nil)
	}
	if ac == nil && cnt > 0 {
		s.log.Warn("fallback to last seen time",
			zap.String("dir", dir.String()), zap.Int("packets", cnt), zap.Time("time", ts))
	}
}

func (s *mysqlStream) ReassemblyComplete(ac reassembly.AssemblerContext) bool {
	s.log.Info("read packet complete")
	close(s.ch)
	if !s.opts.Synchronized {
		<-s.done
	}
	s.h.OnClose()
	//stats.Add(stats.Streams, -1)
	return true
}

func (s *mysqlStream) getBuf(dir reassembly.TCPFlowDirection) *bytes.Buffer {
	if dir == reassembly.TCPDirClientToServer {
		return s.buf0
	} else {
		return s.buf1
	}
}

func (s *mysqlStream) setBuf(dir reassembly.TCPFlowDirection, buf *bytes.Buffer) {
	if dir == reassembly.TCPDirClientToServer {
		s.buf0 = buf
	} else {
		s.buf1 = buf
	}
}

func (s *mysqlStream) getPkt(dir reassembly.TCPFlowDirection) *MySQLPacket {
	if dir == reassembly.TCPDirClientToServer {
		return s.pkt0
	} else {
		return s.pkt1
	}
}

func (s *mysqlStream) setPkt(dir reassembly.TCPFlowDirection, pkt *MySQLPacket) {
	if dir == reassembly.TCPDirClientToServer {
		s.pkt0 = pkt
	} else {
		s.pkt1 = pkt
	}
}

func (s *mysqlStream) getTime(dir reassembly.TCPFlowDirection) time.Time {
	if dir == reassembly.TCPDirClientToServer {
		return s.t0
	} else {
		return s.t1
	}
}

func (s *mysqlStream) setTime(dir reassembly.TCPFlowDirection, t time.Time) {
	if dir == reassembly.TCPDirClientToServer {
		s.t0 = t
	} else {
		s.t1 = t
	}
}

func lookupPacketLen(buf *bytes.Buffer) int {
	if buf.Len() < 3 {
		return -1
	}
	bs := buf.Bytes()[:3]
	return int(uint32(bs[0]) | uint32(bs[1])<<8 | uint32(bs[2])<<16)
}

func lookupPacketSeq(buf *bytes.Buffer) int {
	if buf.Len() < 4 {
		return -1
	}
	return int(buf.Bytes()[3])
}

func formatData(data []byte) string {
	if len(data) > 500 {
		return string(data[:297]) + "..." + string(data[len(data)-200:])
	} else {
		return string(data)
	}
}

const (
	fnvBasis = 14695981039346656037
	fnvPrime = 1099511628211
)

func fnvHash(chunks ...[]byte) (h uint64) {
	h = fnvBasis
	for _, chunk := range chunks {
		for i := 0; i < len(chunk); i++ {
			h ^= uint64(chunk[i])
			h *= fnvPrime
		}
	}
	return
}
