package cmd

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/google/gopacket/reassembly"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"github.com/zyguan/mysql-replay/event"
	"github.com/zyguan/mysql-replay/stats"
	"github.com/zyguan/mysql-replay/stream"
	"go.uber.org/zap"
)

func NewTextDumpCommand() *cobra.Command {
	var (
		options = stream.FactoryOptions{Synchronized: true}
		output  string
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
	cmd.Flags().BoolVar(&options.ForceStart, "force-start", false, "accept streams even if no SYN have been seen")
	return cmd
}

type textDumpHandler struct {
	conn stream.ConnID
	buf  []byte
	log  *zap.Logger
	out  *os.File
	w    *bufio.Writer

	fst int64
	lst int64
}

func (h *textDumpHandler) OnEvent(e event.MySQLEvent) {
	var err error
	h.buf = h.buf[:0]
	h.buf, err = event.AppendEvent(h.buf, e)
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

func NewTextPlayCommand() *cobra.Command {
	var (
		config         playConfig
		targetDSN      string
		reportInterval time.Duration
	)
	cmd := &cobra.Command{
		Use:   "play",
		Short: "Play mysql events from text files",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var (
				done = make(chan struct{})
				err  error
				ctl  *playControl
			)
			ctl, err = newPlayControl(config, args[0], targetDSN)
			if err != nil {
				return err
			}

			fields := make([]zap.Field, 0, 7)
			loadFields := func() {
				metrics := stats.Dump()
				fields = fields[:0]
				for _, name := range []string{
					stats.Connections, stats.ConnRunning, stats.ConnWaiting,
					stats.Queries, stats.StmtExecutes, stats.StmtPrepares,
					stats.FailedQueries, stats.FailedStmtExecutes, stats.FailedStmtPrepares,
				} {
					fields = append(fields, zap.Int64(name, metrics[name]))
				}
			}

			go func() {
				ticker := time.NewTicker(reportInterval)
				defer ticker.Stop()
				for {
					select {
					case <-done:
						return
					case <-ticker.C:
						loadFields()
						ctl.log.Info("stats", fields...)
					}
				}
			}()

			ctl.Play(context.Background())
			close(done)
			loadFields()
			ctl.log.Info("done", fields...)
			return nil
		},
	}
	cmd.Flags().StringVar(&targetDSN, "target-dsn", "", "target dsn")
	cmd.Flags().Float64Var(&config.Speed, "speed", 1, "speed ratio")
	cmd.Flags().BoolVar(&config.DryRun, "dry-run", false, "dry run mode (just print events)")
	cmd.Flags().IntVar(&config.MaxLineSize, "max-line-size", 16777216, "max line size")
	cmd.Flags().DurationVar(&config.QueryTimeout, "query-timeout", time.Minute, "timeout for a single query")
	cmd.Flags().DurationVar(&reportInterval, "report-interval", 5*time.Second, "report interval")
	return cmd
}

type playConfig struct {
	DryRun        bool
	Speed         float64
	PlayStartTime int64
	OrigStartTime int64
	MaxLineSize   int
	QueryTimeout  time.Duration
	MySQLConfig   *mysql.Config
}

func (opts playConfig) Ready(t int64) bool {
	if opts.Speed <= 0 {
		return true
	}
	return opts.Speed*float64(time.Now().UnixNano()/int64(time.Millisecond)-opts.PlayStartTime) >= float64(t-opts.OrigStartTime)
}

func (opts playConfig) WaitTime(t int64) time.Duration {
	if opts.Speed <= 0 {
		return 0
	}
	return time.Duration((float64(t-opts.OrigStartTime)/opts.Speed+float64(opts.PlayStartTime))*float64(time.Millisecond) - float64(time.Now().UnixNano()))
}

type playControl struct {
	playConfig

	log     *zap.Logger
	wg      *sync.WaitGroup
	workers []*playWorker
}

func newPlayControl(cfg playConfig, input string, target string) (*playControl, error) {
	files, err := ioutil.ReadDir(input)
	if err != nil {
		return nil, err
	}
	ctl := &playControl{playConfig: cfg, log: zap.L(), wg: new(sync.WaitGroup), workers: make([]*playWorker, 0, len(files))}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		info := strings.Split(filepath.Base(file.Name()), ".")
		if len(info) != 4 && info[3] != "tsv" {
			continue
		}
		ts, err := strconv.ParseInt(info[0], 10, 64)
		if err != nil {
			ctl.log.Warn("skip input file", zap.String("name", file.Name()), zap.Error(err))
			continue
		}
		id, err := strconv.ParseUint(info[2], 16, 64)
		if err != nil {
			ctl.log.Warn("skip input file", zap.String("name", file.Name()), zap.Error(err))
			continue
		}
		ctl.workers = append(ctl.workers, &playWorker{
			playConfig: ctl.playConfig,
			src:        filepath.Join(input, file.Name()),
			log:        ctl.log.Named(info[2]),
			wg:         ctl.wg,
			ts:         ts,
			id:         id,
			stmts:      make(map[uint64]statement),
		})
	}
	sort.Slice(ctl.workers, func(i, j int) bool { return ctl.workers[i].ts < ctl.workers[j].ts })
	if !ctl.DryRun {
		ctl.MySQLConfig, err = mysql.ParseDSN(target)
		if err != nil {
			return nil, err
		}
	}
	return ctl, nil
}

func (pc *playControl) Play(ctx context.Context) {
	pc.PlayStartTime = time.Now().UnixNano() / int64(time.Millisecond)
	if len(pc.workers) > 0 {
		pc.OrigStartTime = pc.workers[0].ts
	}
	for _, worker := range pc.workers {
		worker.playConfig = pc.playConfig
		d := worker.WaitTime(worker.ts)
		if d > 0 {
			<-time.After(d)
		}
		pc.wg.Add(1)
		go worker.start(ctx)
	}
	pc.wg.Wait()
	return
}

type statement struct {
	query  string
	handle *sql.Stmt
}

type playWorker struct {
	playConfig

	src string
	log *zap.Logger
	wg  *sync.WaitGroup

	ts     int64
	id     uint64
	schema string
	params []interface{}

	pool  *sql.DB
	conn  *sql.Conn
	stmts map[uint64]statement
}

func (pw *playWorker) start(ctx context.Context) {
	defer pw.wg.Done()

	e := event.MySQLEvent{Params: []interface{}{}}
	f, err := os.Open(pw.src)
	if err != nil {
		pw.log.Error("failed to open source file of the stream", zap.Error(err))
		return
	}
	defer func() {
		f.Close()
		pw.quit(false)
	}()
	in := bufio.NewScanner(f)
	if pw.MaxLineSize > 0 {
		buf := make([]byte, 0, 4096)
		in.Buffer(buf, pw.MaxLineSize)
	}
	for in.Scan() {
		_, err = event.ScanEvent(in.Text(), 0, e.Reset(e.Params[:0]))
		if err != nil {
			pw.log.Error("failed to scan event", zap.Error(err))
			return
		}

		if d := pw.WaitTime(e.Time); d > 0 {
			stats.Add(stats.ConnWaiting, 1)
			select {
			case <-ctx.Done():
				stats.Add(stats.ConnWaiting, -1)
				pw.log.Debug("exit due to context done")
				return
			case <-time.After(d):
				stats.Add(stats.ConnWaiting, -1)
			}
		} else {
			select {
			case <-ctx.Done():
				pw.log.Debug("exit due to context done")
				return
			default:
			}
		}
		if pw.DryRun {
			pw.log.Info(e.String())
			continue
		} else if pw.log.Core().Enabled(zap.DebugLevel) {
			pw.log.Debug(e.String())
		}

		switch e.Type {
		case event.EventQuery:
			err = pw.execute(ctx, e.Query)
		case event.EventStmtExecute:
			err = pw.stmtExecute(ctx, e.StmtID, e.Params)
		case event.EventStmtPrepare:
			err = pw.stmtPrepare(ctx, e.StmtID, e.Query)
		case event.EventStmtClose:
			pw.stmtClose(ctx, e.StmtID)
		case event.EventHandshake:
			pw.quit(false)
			err = pw.handshake(ctx, e.DB)
		case event.EventQuit:
			pw.quit(false)
		default:
			pw.log.Warn("unknown event", zap.Any("value", e))
			continue
		}
		if err != nil {
			if sqlErr := errors.Unwrap(err); sqlErr == context.DeadlineExceeded || sqlErr == sql.ErrConnDone || sqlErr == mysql.ErrInvalidConn {
				pw.log.Warn("reconnect after "+e.String(), zap.String("cause", sqlErr.Error()))
				pw.quit(true)
				err = pw.handshake(ctx, pw.schema)
				if err != nil {
					pw.log.Warn("reconnect error", zap.Error(err))
				}
			} else {
				pw.log.Warn("failed to apply "+e.String(), zap.Error(err))
			}
		}
	}
}

func (pw *playWorker) open(schema string) (*sql.DB, error) {
	cfg := pw.MySQLConfig
	if len(schema) > 0 && cfg.DBName != schema {
		cfg = cfg.Clone()
		cfg.DBName = schema
	}
	return sql.Open("mysql", cfg.FormatDSN())
}

func (pw *playWorker) handshake(ctx context.Context, schema string) error {
	pool, err := pw.open(schema)
	if err != nil {
		return err
	}
	pw.pool = pool
	pw.schema = schema
	_, err = pw.getConn(ctx)
	return err
}

func (pw *playWorker) quit(reconnect bool) {
	for id, stmt := range pw.stmts {
		if stmt.handle != nil {
			stmt.handle.Close()
			stmt.handle = nil
		}
		if reconnect {
			pw.stmts[id] = stmt
		} else {
			delete(pw.stmts, id)
		}
	}
	if pw.conn != nil {
		pw.conn.Close()
		pw.conn = nil
		stats.Add(stats.Connections, -1)
	}
	if pw.pool != nil {
		pw.pool.Close()
		pw.pool = nil
	}
}

func (pw *playWorker) execute(ctx context.Context, query string) error {
	conn, err := pw.getConn(ctx)
	if err != nil {
		return err
	}
	if pw.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, pw.QueryTimeout)
		defer cancel()
	}
	stats.Add(stats.Queries, 1)
	stats.Add(stats.ConnRunning, 1)
	_, err = conn.ExecContext(ctx, query)
	stats.Add(stats.ConnRunning, -1)
	if err != nil {
		stats.Add(stats.FailedQueries, 1)
		return errors.Trace(err)
	}
	return nil
}

func (pw *playWorker) stmtPrepare(ctx context.Context, id uint64, query string) error {
	stmt := pw.stmts[id]
	stmt.query = query
	if stmt.handle != nil {
		stmt.handle.Close()
		stmt.handle = nil
	}
	delete(pw.stmts, id)
	conn, err := pw.getConn(ctx)
	if err != nil {
		return err
	}
	stats.Add(stats.StmtPrepares, 1)
	stmt.handle, err = conn.PrepareContext(ctx, stmt.query)
	if err != nil {
		stats.Add(stats.FailedStmtPrepares, 1)
		return errors.Trace(err)
	}
	pw.stmts[id] = stmt
	return nil
}

func (pw *playWorker) stmtExecute(ctx context.Context, id uint64, params []interface{}) error {
	stmt, err := pw.getStmt(ctx, id)
	if err != nil {
		return err
	}
	if pw.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, pw.QueryTimeout)
		defer cancel()
	}
	stats.Add(stats.StmtExecutes, 1)
	stats.Add(stats.ConnRunning, 1)
	_, err = stmt.ExecContext(ctx, params...)
	stats.Add(stats.ConnRunning, -1)
	if err != nil {
		stats.Add(stats.FailedStmtExecutes, 1)
		return errors.Trace(err)
	}
	return nil
}

func (pw *playWorker) stmtClose(ctx context.Context, id uint64) {
	stmt, ok := pw.stmts[id]
	if !ok {
		return
	}
	if stmt.handle != nil {
		stmt.handle.Close()
		stmt.handle = nil
	}
	delete(pw.stmts, id)
}

func (pw *playWorker) getConn(ctx context.Context) (*sql.Conn, error) {
	var err error
	if pw.pool == nil {
		pw.pool, err = pw.open(pw.schema)
		if err != nil {
			return nil, err
		}
	}
	if pw.conn == nil {
		pw.conn, err = pw.pool.Conn(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		stats.Add(stats.Connections, 1)
	}
	return pw.conn, nil
}

func (pw *playWorker) getStmt(ctx context.Context, id uint64) (*sql.Stmt, error) {
	stmt, ok := pw.stmts[id]
	if ok && stmt.handle != nil {
		return stmt.handle, nil
	} else if !ok {
		return nil, errors.Errorf("no such statement #%d", id)
	}
	conn, err := pw.getConn(ctx)
	if err != nil {
		return nil, err
	}
	stmt.handle, err = conn.PrepareContext(ctx, stmt.query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pw.stmts[id] = stmt
	return stmt.handle, nil
}

func NewTextCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "text",
		Short: "Text format utilities",
	}
	cmd.AddCommand(NewTextDumpCommand())
	cmd.AddCommand(NewTextPlayCommand())
	return cmd
}
