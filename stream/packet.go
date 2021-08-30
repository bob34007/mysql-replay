package stream

import (
	"database/sql"
	"encoding/hex"
	"regexp"
	"strings"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/reassembly"
	"github.com/zyguan/mysql-replay/stats"
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

var _ MySQLPacketHandler = &rejectHandler{}

type rejectHandler struct{}

func (r *rejectHandler) Accept(ci gopacket.CaptureInfo, dir reassembly.TCPFlowDirection, tcp *layers.TCP) bool {
	return false
}

func (r *rejectHandler) OnPacket(pkt MySQLPacket) {}

func (r *rejectHandler) OnClose() {}

type ReplayOptions struct {
	DryRun    bool
	TargetDSN string
	FilterIn  string
	FilterOut string
}

func (o ReplayOptions) NewPacketHandler(conn ConnID) MySQLPacketHandler {
	log := conn.Logger("mysql-stream")
	rh := &replayHandler{opts: o, conn: conn, log: log, fsm: NewMySQLFSM(log)}
	if len(o.FilterIn) >= 0 {
		if p, err := regexp.Compile(o.FilterIn); err != nil {
			log.Warn("invalid filter-in regexp", zap.Error(err))
		} else {
			rh.filter = func(s string) bool {
				return p.FindStringIndex(s) != nil
			}
		}
	}
	if len(o.FilterOut) > 0 {
		if p, err := regexp.Compile(o.FilterOut); err != nil {
			log.Warn("invalid filter-out regexp", zap.Error(err))
		} else {
			if filter := rh.filter; filter != nil {
				rh.filter = func(s string) bool {
					return filter(s) && p.FindStringIndex(s) == nil
				}
			} else {
				rh.filter = func(s string) bool {
					return p.FindStringIndex(s) == nil
				}
			}
		}
	}
	if o.DryRun {
		log.Debug("fake connect to target db", zap.String("dsn", o.TargetDSN))
		return rh
	}
	var err error
	rh.db, err = sql.Open("mysql", o.TargetDSN)
	if err != nil {
		log.Error("reject connection due to error",
			zap.String("dsn", o.TargetDSN), zap.Error(err))
		return RejectConn(conn)
	}
	rh.log.Debug("open connection to " + rh.opts.TargetDSN)
	stats.Add(stats.Connections, 1)
	return rh
}

var _ MySQLPacketHandler = &replayHandler{}

type replayHandler struct {
	opts   ReplayOptions
	conn   ConnID
	fsm    *MySQLFSM
	log    *zap.Logger
	db     *sql.DB
	filter func(s string) bool
}

func (rh *replayHandler) Accept(ci gopacket.CaptureInfo, dir reassembly.TCPFlowDirection, tcp *layers.TCP) bool {
	return true
}

func (rh *replayHandler) OnPacket(pkt MySQLPacket) {
	rh.fsm.Handle(pkt)
	if !rh.fsm.Ready() || !rh.fsm.Changed() {
		return
	}
	switch rh.fsm.State() {
	case StateComQuery:
		stats.Add(stats.Queries, 1)
		query := rh.fsm.Query()
		if rh.filter != nil && !rh.filter(query) {
			return
		}
		if rh.db == nil {
			rh.l(pkt.Dir).Info("execute query", zap.String("sql", query))
			return
		}
		if _, err := rh.db.Exec(query); err != nil {
			rh.l(pkt.Dir).Warn("execute query", zap.String("sql", query), zap.Error(err))
			stats.Add(stats.FailedQueries, 1)
		}
	}
}

func (rh *replayHandler) OnClose() {
	rh.log.Debug("close connection to " + rh.opts.TargetDSN)
	if rh.db != nil {
		rh.db.Close()
		stats.Add(stats.Connections, -1)
	}
}

func (rh *replayHandler) l(dir reassembly.TCPFlowDirection) *zap.Logger {
	return rh.log.With(zap.String("dir", dir.String()))
}
