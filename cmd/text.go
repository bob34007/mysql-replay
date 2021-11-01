package cmd

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/bobguo/mysql-replay/util"

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

var ERRORTIMEOUT = errors.New("replay runtime out")

func HandlePcapFile(name string, ts time.Time, rt int, ticker *time.Ticker,
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

func NewWriteFile() *WriteFile{
	wf :=new(WriteFile)
	wf.ch =make(chan stream.MySQLEvent,10000)
	wf.wg = new(sync.WaitGroup)
	wf.rrStartGoRuntine = false
	wf.once = new(sync.Once)
	return wf
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
		runTime   string
		outputDir string
		flushInterval time.Duration
	)
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay pcap files",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			var fileNameSeq  =0
			log.Info("process begin run at " + time.Now().String())
			if len(args) == 0 {
				return cmd.Help()
			}

			ts := time.Now()
			var ticker *time.Ticker
			rt, MySQLCfg, err := util.CheckParamValid(dsn, runTime, outputDir)
			if err != nil {
				log.Error("parse param error , " + err.Error())
				return nil
			}

			if rt > 0 {
				ticker = time.NewTicker(3 * time.Second)
				defer ticker.Stop()
			}

			factory := stream.NewFactoryFromEventHandler(func(conn stream.ConnID) stream.MySQLEventHandler {
				logger := conn.Logger("replay")
				fileName := conn.HashStr() + ":" + conn.SrcAddr()+GenerateFileSeqString(fileNameSeq)
				fileNameSeq++
				f, err := util.OpenFile(outputDir, fileName)
				logger.Info("open file for write result "+fileName)
				if err != nil {
					panic("create and open  file fail , " + outputDir + "/" + fileName + err.Error())
				}

				return &replayEventHandler{
					pconn:       conn,
					log:         logger,
					dsn:         dsn,
					MySQLConfig: MySQLCfg,
					ctx:         context.Background(),
					ch:          make(chan stream.MySQLEvent, 10000),
					wg:          new(sync.WaitGroup),
					stmts:       make(map[uint64]statement),
					file:        f,
					once :      new(sync.Once),
					wf  :       NewWriteFile(),
				}
			}, options)
			pool := reassembly.NewStreamPool(factory)
			assembler := reassembly.NewAssembler(pool)

			lastFlushTime := time.Time{}

			for _, in := range args {
				zap.L().Info("processing " + in)
				err = HandlePcapFile(in, ts, rt, ticker, assembler,&lastFlushTime,flushInterval)
				if err != nil && err != ERRORTIMEOUT {
					return err
				} else if err == ERRORTIMEOUT {
					break
				}
				assembler.FlushCloseOlderThan(factory.LastStreamTime().Add(-3 * time.Minute))
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
	cmd.Flags().StringVarP(&runTime, "runtime", "t", "0", "replay server run time")
	cmd.Flags().StringVarP(&outputDir, "output", "o", "./output", "directory used to write the result set")
	cmd.Flags().DurationVar(&flushInterval, "flush-interval", time.Minute, "flush interval")
	return cmd
}

//Store prepare statement and handle
type statement struct {
	query  string
	handle *sql.Stmt
}

//Used for replay  SQL
type replayEventHandler struct {
	pconn                       stream.ConnID
	dsn                         string
	fsm                         *stream.MySQLFSM
	log                         *zap.Logger
	MySQLConfig                 *mysql.Config
	schema                      string
	pool                        *sql.DB
	conn                        *sql.Conn
	stmts                       map[uint64]statement
	ctx                         context.Context
	filterStr                   string
	needCompareRes              bool
	needCompareExecTime         bool
	rrLastGetCheckPointTime     time.Time
	rrCheckPoint                time.Time
	rrGetCheckPointTimeInterval int64
	//rrContinueRun               bool
	rrNeedReplay     bool
	once     *sync.Once
	ch               chan stream.MySQLEvent
	wg               *sync.WaitGroup
	file             *os.File
	wf             *WriteFile
}

type WriteFile struct {
	ch chan stream.MySQLEvent
	rrStartGoRuntine bool
	wg  *sync.WaitGroup
	once *sync.Once
}

func (h *replayEventHandler) DoWriteResToFile(){

	h.log.Info("thread begin to run for write " + h.file.Name())
	for {
		e ,ok := <-h.wf.ch
		if ok {
			res ,err:= stream.NewResForWriteFile(e.Pr, e.Rr, &e, h.file)
			if err !=nil{
				if err != nil {
					h.log.Warn("new write compare result to file struct fail , " + err.Error())
				}
			} else {
				_, err = res.WriteResToFile()
				if err != nil {
					h.log.Warn("write compare result to file fail , " + err.Error())
				}
			}

		}else {
			h.wf.wg.Done()
			h.log.Info("thread end to run for write " + h.file.Name())
			h.log.Info("chan close ,func exit ")
			return
		}

	}

}

func (h *replayEventHandler) AsyncWriteResToFile(e stream.MySQLEvent){
	h.wf.once.Do(
		func (){
			h.wf.wg.Add(1)
			go h.DoWriteResToFile()
		})
	h.wf.ch <- e
}


func (h *replayEventHandler) ReplayEvent(ch chan stream.MySQLEvent, wg *sync.WaitGroup) {
	defer func() {
		if err := recover(); err != nil {
			h.log.Warn(err.(string))
		}

	}()
	h.log.Info("thread begin to run for apply mysql event " + h.file.Name())
	for {
		e, ok := <-ch

		if ok {
			if h.fsm == nil {
				h.fsm = e.Fsm
			}

			err := h.ApplyEvent(h.ctx, &e)
			if err != nil {
				if mysqlError, ok := err.(*mysql.MySQLError); ok {
					e.Rr.ErrNO = mysqlError.Number
					e.Rr.ErrDesc = mysqlError.Message
				} else {
					e.Rr.ErrNO = 20000
					e.Rr.ErrDesc = "Failed to execute SQL and failed to convert to mysql error"
				}
			}
			h.AsyncWriteResToFile(e)
		} else {
			wg.Done()
			h.log.Info("thread end to run for apply mysql event " + h.file.Name())
			h.log.Info("chan close ,func exit ")
			return
		}
	}

}

func (h *replayEventHandler) OnEvent(e stream.MySQLEvent) {
	//Process SQL events. Note that unlike the events in binlog,
	//this SQL event is raw and may involve multiple rows

	//e.Rr = new(stream.ReplayRes)
	e.NewReplayRes()
	//e.InitRr()
	h.once.Do(func () {
		h.wg.Add(1)
		go h.ReplayEvent(h.ch, h.wg)
	})
	h.ch <- e

}


func (h *replayEventHandler) OnClose() {
	close(h.ch)
	h.wg.Wait()
	//wait write goroutine end
	close(h.wf.ch)
	h.wf.wg.Wait()
	err := h.file.Sync()
	if err != nil {
		h.log.Warn("sync file fail , " + h.file.Name() + " " + err.Error())
	}
	err = h.file.Close()
	if err != nil {
		h.log.Warn("close file fail , " + h.file.Name() + " " + err.Error())
	}
	h.quit(false)
}

func (h *replayEventHandler) ApplyEvent(ctx context.Context, e *stream.MySQLEvent) error {
	//apply mysql event on replay server
	var err error
LOOP:
	switch e.Type {
	case stream.EventQuery:
		var mysqlError *mysql.MySQLError
		e.Rr.ColValues = make([][]driver.Value, 0)
		var ok bool
	RETRYCOMQUERY:
		err = h.execute(ctx, e.Query, e)
		//fmt.Println(err)
		if err != nil {
			if mysqlError, ok = err.(*mysql.MySQLError); ok {
				//If TiDB thrown 1205: Lock wait timeout exceeded; try restarting transaction
				//we try again until execute success
				if mysqlError.Number == 1205 {
					h.log.Warn(fmt.Sprintf("replay sql with lock wait timeout , try again %v",mysqlError))
					e.Rr.ColValues = e.Rr.ColValues[:0][:0]
					goto RETRYCOMQUERY
				}
			}
		}
	case stream.EventStmtPrepare:
		err = h.stmtPrepare(ctx, e.StmtID, e.Query)
		if err != nil {
			if mysqlError, ok := err.(*mysql.MySQLError); ok {
				logstr := fmt.Sprintf("prepare statment exec fail ,%s , %d ,%s ",
					e.Query, mysqlError.Number, mysqlError.Message)
				h.log.Error(logstr)
			} else {
				e.Rr.ErrNO = 20000
				e.Rr.ErrDesc = "exec sql fail and coverted to mysql errorstruct err"
			}
		}
	case stream.EventStmtExecute:
		_, ok := h.stmts[e.StmtID]
		if ok {
			var mysqlError *mysql.MySQLError
			e.Rr.ColValues = make([][]driver.Value, 0)
		RETRYCOMSTMTEXECUTE:
			err = h.stmtExecute(ctx, e.StmtID, e.Params, e)
			if err != nil {
				if mysqlError, ok = err.(*mysql.MySQLError); ok {
					//If TiDB thrown 1205: Lock wait timeout exceeded; try restarting transaction
					//we try again until execute success
					if mysqlError.Number == 1205 {
						e.Rr.ColValues = e.Rr.ColValues[:0][:0]
						goto RETRYCOMSTMTEXECUTE
					}
				}
			}
		} else {
			err := new(mysql.MySQLError)
			err.Number = 10000
			err.Message = fmt.Sprintf("%v is not exist , maybe prepare fail", e.StmtID)
			return err
		}
	case stream.EventStmtClose:
		h.stmtClose(e.StmtID)
	case stream.EventHandshake:
		h.quit(false)
		err = h.handshake(ctx, e.DB)
	case stream.EventQuit:
		h.quit(false)
	default:
		h.log.Warn("unknown event", zap.Any("value", e))
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

//connect to server and set autocommit on
func (h *replayEventHandler) open(schema string) (*sql.DB, error) {
	cfg := h.MySQLConfig
	if len(schema) > 0 && cfg.DBName != schema {
		cfg = cfg.Clone()
		cfg.DBName = schema
	}
	return sql.Open("mysql", cfg.FormatDSN())
}

//Handle Handshake messages, similar to Use Database
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

// Conn returns a single connection by either opening a new connection
// or returning an existing connection from the connection pool. Conn will
// block until either a connection is returned or ctx is canceled.
// Queries run on the same Conn will be run in the same database session.
//
// Every Conn must be returned to the database pool after use by
// calling Conn.Close.
func (h *replayEventHandler) getConn(ctx context.Context) (*sql.Conn, error) {
	var err error
	if h.pool == nil {
		h.pool, err = h.open(h.schema)
		//fmt.Println(477,h.pool,h.schema,err)
		if err != nil {
			return nil, err
		}
	}
	if h.conn == nil {
		h.conn, err = h.pool.Conn(ctx)
		if err != nil {
			//fmt.Println(485,err)
			return nil, err
		}
		//stats.Add(stats.Connections, 1)
	}
	return h.conn, nil
}

//Disconnect from replay server
func (h *replayEventHandler) quit(reconnect bool) {
	for id, stmt := range h.stmts {
		if stmt.handle != nil {
			if err := stmt.handle.Close(); err != nil {
				h.log.Warn("close stmt.handle fail ," + err.Error())
			}
			stmt.handle = nil
		}
		if reconnect {
			h.stmts[id] = stmt
		} else {
			delete(h.stmts, id)
		}
	}
	if h.conn != nil {
		if err := h.conn.Close(); err != nil {
			h.log.Warn("close conn fail ," + err.Error())
		}
		h.conn = nil
		//stats.Add(stats.Connections, -1)
	}
	if h.pool != nil {
		if err := h.pool.Close(); err != nil {
			h.log.Warn("close pool fail ," + err.Error())
		}
		h.pool = nil
	}
}

//Execute SQL on replay Server
func (h *replayEventHandler) execute(ctx context.Context, query string, e *stream.MySQLEvent) error {
	conn, err := h.getConn(ctx)
	//fmt.Println(526,err)
	if err != nil {
		return err
	}
	//stats.Add(stats.Queries, 1)
	//stats.Add(stats.ConnRunning, 1)
	e.Rr.SqlBeginTime = uint64(time.Now().UnixNano())
	e.Rr.SqlStatment = query
	//fmt.Println(query)
	rows, err := conn.QueryContext(ctx, query)
	e.Rr.SqlEndTime = uint64(time.Now().UnixNano())
	defer func() {
		if rows != nil {
			if rs := rows.Close(); rs != nil {
				h.log.Warn("close row fail," + rs.Error())
			}
		}
	}()
	//stats.Add(stats.ConnRunning, -1)
	if err != nil {
		//stats.Add(stats.FailedQueries, 1)
		return err
	}
	for rows.Next() {
		h.ReadRowValues(rows, e)
	}

	return nil
}

//Exec prepare statment on replay sql
func (h *replayEventHandler) stmtPrepare(ctx context.Context, id uint64, query string) error {
	stmt := h.stmts[id]
	stmt.query = query
	if stmt.handle != nil {
		if err := stmt.handle.Close(); err != nil {
			h.log.Warn("close stmt handle fail ," + err.Error())
		}
		stmt.handle = nil
	}
	delete(h.stmts, id)
	conn, err := h.getConn(ctx)
	if err != nil {
		return err
	}
	//stats.Add(stats.StmtPrepares, 1)
	stmt.handle, err = conn.PrepareContext(ctx, stmt.query)
	if err != nil {
		//stats.Add(stats.FailedStmtPrepares, 1)
		return err
	}
	h.stmts[id] = stmt
	h.log.Debug(fmt.Sprintf("%v id is %v", query, id))
	return nil
}

//Retrieve the prepare statement from SQL.Stmt
//via the unsafe and reflection mechanisms
func (h *replayEventHandler) getQuery(s *sql.Stmt) string {
	rs := reflect.ValueOf(s)
	foo := rs.Elem().FieldByName("query")
	rf := foo
	rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()
	z := rf.Interface().(string)
	return z
}

//Exec prepare on replay server
func (h *replayEventHandler) stmtExecute(ctx context.Context, id uint64, params []interface{}, e *stream.MySQLEvent) error {
	stmt, err := h.getStmt(ctx, id)
	if err != nil {
		return err
	}

	e.Rr.SqlStatment = h.getQuery(stmt)
	e.Rr.Values = params

	//fmt.Println(e.Rr.SqlStatment,e.Rr.Values)
	//stats.Add(stats.StmtExecutes, 1)
	//stats.Add(stats.ConnRunning, 1)
	e.Rr.SqlBeginTime = uint64(time.Now().UnixNano())
	rows, err := stmt.QueryContext(ctx, params...)
	e.Rr.SqlEndTime = uint64(time.Now().UnixNano())
	defer func() {
		if rows != nil {
			if err := rows.Close(); err != nil {
				h.log.Warn("close rows fail," + err.Error())
			}
		}
	}()
	//stats.Add(stats.ConnRunning, -1)
	if err != nil {
		//stats.Add(stats.FailedStmtExecutes, 1)
		return err
	}
	for rows.Next() {
		h.ReadRowValues(rows, e)
	}

	return nil
}

//Close prepare handle
func (h *replayEventHandler) stmtClose(id uint64) {
	stmt, ok := h.stmts[id]
	if !ok {
		return
	}
	if stmt.handle != nil {
		if err := stmt.handle.Close(); err != nil {
			h.log.Warn("close stmt handle fail," + err.Error())
		}
		stmt.handle = nil
	}
	delete(h.stmts, id)
}

//Get prepare handle ID
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
		return nil, err
	}
	h.stmts[id] = stmt
	return stmt.handle, nil
}

func (h *replayEventHandler) ReadRowValues(f *sql.Rows, e *stream.MySQLEvent) {
	//Get the lastcols value from the sql.Rows
	//structure using unsafe and reflection mechanisms
	//and load it into the cache

	rs := reflect.ValueOf(f)
	foo := rs.Elem().FieldByName("lastcols")
	rf := foo
	rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()
	z := rf.Interface().([]driver.Value)
	rr := make([]driver.Value, 0, len(z))
	var err error
	for i := range z {
		if z[i] == nil {
			rr = append(rr, nil)
			continue
		}
		var a string
		err = stream.ConvertAssignRows(z[i], &a)
		if err == nil {
			rr = append(rr, a)
		} else {
			h.log.Warn("get row values fail , covert column value to string fail ," + err.Error())
		}
	}
	if err == nil {
		e.Rr.ColValues = append(e.Rr.ColValues, rr)
	}
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
