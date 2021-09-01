package cmd

import (
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/bobguo/mysql-replay/stats"
	"github.com/bobguo/mysql-replay/stream"
	"github.com/go-sql-driver/mysql"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/google/gopacket/reassembly"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	Sm          *sync.Mutex
	ExecSqlNum  uint64
	ExecSuccNum uint64
	ExecFailNum uint64
)

func NewTextDumpCommand() *cobra.Command {
	var (
		options = stream.FactoryOptions{Synchronized: true}
		output  string
		dsn     string
	)
	cmd := &cobra.Command{
		Use:   "dump",
		Short: "Dump pcap files",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			if len(output) > 0 {
				os.MkdirAll(output, 0755)
			}

			factory := stream.NewFactoryFromEventHandler(func(conn stream.ConnID) stream.MySQLEventHandler {
				log := conn.Logger("dump")
				out, err := os.CreateTemp(output, "."+conn.HashStr()+".*")
				if err != nil {
					log.Error("failed to create file for dumping events", zap.Error(err))
					return nil
				}
				return &textDumpHandler{
					conn: conn,
					buf:  make([]byte, 0, 4096),
					log:  log,
					out:  out,
					w:    bufio.NewWriterSize(out, 1048576),
				}
			}, options)
			pool := reassembly.NewStreamPool(factory)
			assembler := reassembly.NewAssembler(pool)

			handle := func(name string) error {
				f, err := pcap.OpenOffline(name)
				if err != nil {
					return errors.Annotate(err, "open "+name)
				}
				defer f.Close()
				src := gopacket.NewPacketSource(f, f.LinkType())
				for pkt := range src.Packets() {
					layer := pkt.Layer(layers.LayerTypeTCP)
					if layer == nil {
						continue
					}
					tcp := layer.(*layers.TCP)
					assembler.AssembleWithContext(pkt.NetworkLayer().NetworkFlow(), tcp, captureContext(pkt.Metadata().CaptureInfo))
				}
				return nil
			}

			for _, in := range args {
				zap.L().Info("processing " + in)
				err := handle(in)
				if err != nil {
					return err
				}
				assembler.FlushCloseOlderThan(factory.LastStreamTime().Add(-3 * time.Minute))
			}
			assembler.FlushAll()

			return nil
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "", "output directory")
	cmd.Flags().StringVarP(&dsn, "dsn", "d", "", "replay server dsn")
	cmd.Flags().BoolVar(&options.ForceStart, "force-start", false, "accept streams even if no SYN have been seen")
	return cmd
}

func NewTextDumpReplayCommand() *cobra.Command {
	var (
		options = stream.FactoryOptions{Synchronized: true}
		dsn     string
	)
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay pcap files",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			if len(dsn) == 0 {
				log.Error("need to specify DSN for replay sql ")
				return nil
			}
			MySQLConfig, err := mysql.ParseDSN(dsn)
			if err != nil {
				log.Error("fail to parse DSN to MySQLCongif ,", zap.Error(err))
				return nil
			}
			factory := stream.NewFactoryFromEventHandler(func(conn stream.ConnID) stream.MySQLEventHandler {
				log := conn.Logger("replay")
				return &replayEventHandler{
					pconn:       conn,
					log:         log,
					dsn:         dsn,
					MySQLConfig: MySQLConfig,
					ctx:         context.Background(),
					Rr:          new(stream.ReplayRes),
					stmts:       make(map[uint64]statement),
				}
			}, options)
			pool := reassembly.NewStreamPool(factory)
			assembler := reassembly.NewAssembler(pool)

			handle := func(name string) error {
				f, err := pcap.OpenOffline(name)
				if err != nil {
					return errors.Annotate(err, "open "+name)
				}
				defer f.Close()
				src := gopacket.NewPacketSource(f, f.LinkType())
				for pkt := range src.Packets() {
					layer := pkt.Layer(layers.LayerTypeTCP)
					if layer == nil {
						continue
					}
					tcp := layer.(*layers.TCP)
					assembler.AssembleWithContext(pkt.NetworkLayer().NetworkFlow(), tcp, captureContext(pkt.Metadata().CaptureInfo))
				}
				return nil
			}

			for _, in := range args {
				zap.L().Info("processing " + in)
				err := handle(in)
				if err != nil {
					return err
				}
				assembler.FlushCloseOlderThan(factory.LastStreamTime().Add(-3 * time.Minute))
			}
			assembler.FlushAll()

			return nil
		},
	}

	cmd.Flags().StringVarP(&dsn, "dsn", "d", "", "replay server dsn")
	cmd.Flags().BoolVar(&options.ForceStart, "force-start", false, "accept streams even if no SYN have been seen")
	return cmd
}

type statement struct {
	query  string
	handle *sql.Stmt
}

type replayEventHandler struct {
	pconn          stream.ConnID
	dsn            string
	fsm            *stream.MySQLFSM
	log            *zap.Logger
	MySQLConfig    *mysql.Config
	schema         string
	pool           *sql.DB
	conn           *sql.Conn
	stmts          map[uint64]statement
	ctx            context.Context
	needCompareRes bool
	Rr             *stream.ReplayRes
}

func (h *replayEventHandler) OnEvent(e stream.MySQLEvent) {
	//ctx := context.Background()
	err := h.ApplyEvent(h.ctx, e)
	if err == nil {
		if mysqlError, ok := err.(*mysql.MySQLError); ok {
			h.Rr.ErrNO = mysqlError.Number
			h.Rr.ErrDesc = mysqlError.Message
		}
	}

	if h.needCompareRes {
		h.fsm = e.Fsm
		res := h.fsm.CompareRes(h.Rr)
		if res.ErrCode != 0 {
			logstr, err := json.Marshal(res)
			if err != nil {
				h.log.Warn("compare result marshal to json error " + err.Error())
			}
			h.log.Warn(string(logstr))
		}
	}
}

func (h *replayEventHandler) OnClose() {
	h.quit(false)
}

func (h *replayEventHandler) ApplyEvent(ctx context.Context, e stream.MySQLEvent) error {
	var err error
	h.needCompareRes = false
LOOP:
	switch e.Type {
	case stream.EventQuery:
		if h.fsm.IsSelectStmtOrSelectPrepare(e.Query) {
			h.Rr.ColValues = make([][]driver.Value, 0)
			err = h.execute(ctx, e.Query)
			h.needCompareRes = true
		}
	case stream.EventStmtPrepare:
		if h.fsm.IsSelectStmtOrSelectPrepare(e.Query) {
			err = h.stmtPrepare(ctx, e.StmtID, e.Query)
		}
	case stream.EventStmtExecute:
		if _, ok := h.stmts[e.StmtID]; ok {
			h.Rr.ColValues = make([][]driver.Value, 0)
			err = h.stmtExecute(ctx, e.StmtID, e.Params)
			h.needCompareRes = true
		}
	case stream.EventStmtClose:
		h.stmtClose(ctx, e.StmtID)
	case stream.EventHandshake:
		h.quit(false)
		err = h.handshake(ctx, e.DB)
	case stream.EventQuit:
		h.quit(false)
	default:
		h.log.Warn("unknown event", zap.Any("value", e))
		//continue
	}
	if err != nil {
		if sqlErr := errors.Unwrap(err); sqlErr == context.DeadlineExceeded || sqlErr == sql.ErrConnDone || sqlErr == mysql.ErrInvalidConn {
			h.log.Warn("reconnect after "+e.String(), zap.String("cause", sqlErr.Error()))
			h.quit(true)
			err = h.handshake(ctx, h.schema)
			if err != nil {
				h.log.Warn("reconnect error", zap.Error(err))
			} else {
				//reconnect success ,try exec query again
				goto LOOP
			}
		} else {
			h.log.Warn("failed to apply "+e.String(), zap.Error(err))
		}
	}
	return err
}

func (h *replayEventHandler) open(schema string) (*sql.DB, error) {
	cfg := h.MySQLConfig
	if len(schema) > 0 && cfg.DBName != schema {
		cfg = cfg.Clone()
		cfg.DBName = schema
	}
	return sql.Open("mysql", cfg.FormatDSN())
}

func (h *replayEventHandler) handshake(ctx context.Context, schema string) error {
	pool, err := h.open(schema)
	if err != nil {
		return err
	}
	h.pool = pool
	h.schema = schema
	_, err = h.getConn(ctx)
	return err
}

func (h *replayEventHandler) getConn(ctx context.Context) (*sql.Conn, error) {
	var err error
	if h.pool == nil {
		h.pool, err = h.open(h.schema)
		if err != nil {
			return nil, err
		}
	}
	if h.conn == nil {
		h.conn, err = h.pool.Conn(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		stats.Add(stats.Connections, 1)
	}
	return h.conn, nil
}

func (h *replayEventHandler) quit(reconnect bool) {
	for id, stmt := range h.stmts {
		if stmt.handle != nil {
			stmt.handle.Close()
			stmt.handle = nil
		}
		if reconnect {
			h.stmts[id] = stmt
		} else {
			delete(h.stmts, id)
		}
	}
	if h.conn != nil {
		h.conn.Close()
		h.conn = nil
		stats.Add(stats.Connections, -1)
	}
	if h.pool != nil {
		h.pool.Close()
		h.pool = nil
	}
}

func (h *replayEventHandler) execute(ctx context.Context, query string) error {
	conn, err := h.getConn(ctx)
	if err != nil {
		return err
	}
	/*if h.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.QueryTimeout)
		defer cancel()
	}*/
	stats.Add(stats.Queries, 1)
	stats.Add(stats.ConnRunning, 1)
	h.Rr.SqlBeginTime = time.Now().UnixNano() / 1000000
	h.Rr.SqlStatment = query
	rows, err := conn.QueryContext(ctx, query)
	stats.Add(stats.ConnRunning, -1)
	if err != nil {
		h.Rr.SqlEndTime = time.Now().UnixNano() / 1000000
		stats.Add(stats.FailedQueries, 1)
		return errors.Trace(err)
	}
	for rows.Next() {
		h.ReadRowValues(rows)
	}
	h.Rr.SqlEndTime = time.Now().UnixNano() / 1000000
	defer rows.Close()
	return nil
}

func (h *replayEventHandler) stmtPrepare(ctx context.Context, id uint64, query string) error {
	stmt := h.stmts[id]
	stmt.query = query
	if stmt.handle != nil {
		stmt.handle.Close()
		stmt.handle = nil
	}
	delete(h.stmts, id)
	conn, err := h.getConn(ctx)
	if err != nil {
		return err
	}
	stats.Add(stats.StmtPrepares, 1)
	stmt.handle, err = conn.PrepareContext(ctx, stmt.query)
	if err != nil {
		stats.Add(stats.FailedStmtPrepares, 1)
		return errors.Trace(err)
	}
	h.stmts[id] = stmt
	return nil
}

func (h *replayEventHandler) getQuery(s *sql.Stmt) string {
	rs := reflect.ValueOf(s)
	foo := rs.Elem().FieldByName("query")
	rf := foo
	rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()
	z := rf.Interface().(string)
	return z
}
func (h *replayEventHandler) stmtExecute(ctx context.Context, id uint64, params []interface{}) error {
	stmt, err := h.getStmt(ctx, id)
	if err != nil {
		return err
	}
	/*if h.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.QueryTimeout)
		defer cancel()
	}*/
	h.Rr.SqlStatment = h.getQuery(stmt)
	h.Rr.Values = params
	stats.Add(stats.StmtExecutes, 1)
	stats.Add(stats.ConnRunning, 1)
	h.Rr.SqlBeginTime = time.Now().UnixNano() / 1000000
	rows, err := stmt.QueryContext(ctx, params...)
	stats.Add(stats.ConnRunning, -1)
	if err != nil {
		h.Rr.SqlEndTime = time.Now().UnixNano() / 1000000
		stats.Add(stats.FailedStmtExecutes, 1)
		return errors.Trace(err)
	}
	h.Rr.ColNames, _ = rows.Columns()
	//hr.Rr.ColTypes, _ = rows.ColumnTypes()
	for rows.Next() {
		h.ReadRowValues(rows)
	}
	h.Rr.SqlEndTime = time.Now().UnixNano() / 1000000
	defer rows.Close()
	return nil
}

func (h *replayEventHandler) stmtClose(ctx context.Context, id uint64) {
	stmt, ok := h.stmts[id]
	if !ok {
		return
	}
	if stmt.handle != nil {
		stmt.handle.Close()
		stmt.handle = nil
	}
	delete(h.stmts, id)
}

func (h *replayEventHandler) getStmt(ctx context.Context, id uint64) (*sql.Stmt, error) {
	stmt, ok := h.stmts[id]
	if ok && stmt.handle != nil {
		return stmt.handle, nil
	} else if !ok {
		return nil, errors.Errorf("no such statement #%d", id)
	}
	conn, err := h.getConn(ctx)
	if err != nil {
		return nil, err
	}
	stmt.handle, err = conn.PrepareContext(ctx, stmt.query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	h.stmts[id] = stmt
	return stmt.handle, nil
}

//get column from sql.Rows structure
func (h *replayEventHandler) GetColNames(f *sql.Rows) {
	var err error
	h.Rr.ColNames, err = f.Columns()
	if err != nil {
		h.log.Info("read column name err ,", zap.Error(err))
	}
}

//get the lastcols value from the sql.Rows
//structure using unsafe and reflection mechanisms
//and load it into the cache
func (h *replayEventHandler) ReadRowValues(f *sql.Rows) {
	rs := reflect.ValueOf(f)
	foo := rs.Elem().FieldByName("lastcols")
	rf := foo
	rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()
	z := rf.Interface().([]driver.Value)
	h.Rr.ColValues = append(h.Rr.ColValues, z)
}

type textDumpHandler struct {
	conn stream.ConnID
	buf  []byte
	log  *zap.Logger
	out  *os.File
	w    *bufio.Writer
	fst  int64
	lst  int64
}

func (h *textDumpHandler) OnEvent(e stream.MySQLEvent) {
	var err error
	h.buf = h.buf[:0]
	h.buf, err = stream.AppendEvent(h.buf, e)
	if err != nil {
		h.log.Error("failed to dump event", zap.Any("value", e), zap.Error(err))
		return
	}
	h.w.Write(h.buf)
	h.w.WriteString("\n")
	h.lst = e.Time
	if h.fst == 0 {
		h.fst = e.Time
	}
}

func (h *textDumpHandler) OnClose() {
	h.w.Flush()
	h.out.Close()
	path := h.out.Name()
	if h.fst == 0 {
		os.Remove(path)
	} else {
		os.Rename(path, filepath.Join(filepath.Dir(path), fmt.Sprintf("%d.%d.%s.tsv", h.fst, h.lst, h.conn.HashStr())))
	}
}

func NewTextCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "text",
		Short: "Text format utilities",
	}
	cmd.AddCommand(NewTextDumpCommand())
	cmd.AddCommand(NewTextDumpReplayCommand())
	return cmd
}
