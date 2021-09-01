package stream

import (
	"encoding/hex"
	"strings"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/reassembly"
	"go.uber.org/zap"
)

type MySQLPacketHandler interface {
	Accept(ci gopacket.CaptureInfo, dir reassembly.TCPFlowDirection, tcp *layers.TCP) bool
	OnPacket(pkt MySQLPacket)
	OnClose()
}

var _ MySQLPacketHandler = &defaultHandler{}

type defaultHandler struct {
	log *zap.Logger
	fsm *MySQLFSM
}

func defaultHandlerFactory(conn ConnID) MySQLPacketHandler {
	log := conn.Logger("mysql-stream")
	log.Info("new stream")
	return &defaultHandler{log: log, fsm: NewMySQLFSM(log)}
}

func (h *defaultHandler) Accept(ci gopacket.CaptureInfo, dir reassembly.TCPFlowDirection, tcp *layers.TCP) bool {
	return true
}

func (h *defaultHandler) OnPacket(pkt MySQLPacket) {
	h.fsm.Handle(pkt)
	msg := "packet"
	if pkt.Len > 0 {
		dump := hex.Dump(pkt.Data)
		lines := strings.Split(strings.TrimSpace(dump), "\n")
		if len(lines) > 6 {
			abbr := lines[:3]
			abbr = append(abbr, "...")
			abbr = append(abbr, lines[len(lines)-3:]...)
			lines = abbr
		}
		msg += "\n\t" + strings.Join(lines, "\n\t") + "\n"
	}
	h.log.Info(msg,
		zap.Time("time", pkt.Time),
		zap.String("dir", pkt.Dir.String()),
		zap.Int("len", pkt.Len),
		zap.Int("seq", pkt.Seq),
	)
}

func (h *defaultHandler) OnClose() {
	h.log.Info("close")
}

func RejectConn(conn ConnID) MySQLPacketHandler {
	return &rejectHandler{}
}

type rejectHandler struct{}

func (r *rejectHandler) Accept(ci gopacket.CaptureInfo, dir reassembly.TCPFlowDirection, tcp *layers.TCP) bool {
	return false
}

func (r *rejectHandler) OnPacket(pkt MySQLPacket) {}

func (r *rejectHandler) OnClose() {}
