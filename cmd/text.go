package cmd

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/bobguo/mysql-replay/util"
	"os"
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

var ERRORTIMEOUT = errors.New("replay runtime out")

func NewTextDumpReplayCommand() *cobra.Command {
	//Replay sql from pcap files，and compare reslut from pcap file and
	//replay server

	var (
		options   = stream.FactoryOptions{Synchronized: true}
		dsn       string
		runTime   string
		outputDir string
	)
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay pcap files",
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			log.Info("process begin run at " + time.Now().String())
			if len(args) == 0 {
				return cmd.Help()
			}

			ts := time.Now()
			var ticker *time.Ticker
			rt, MySQLConfig, err := util.CheckParamValid(runTime, dsn, outputDir)
			if err != nil {
				log.Error("parse param error , " + err.Error())
				return nil
			}

			if rt > 0 {
				ticker = time.NewTicker(3 * time.Second)
			}

			var wg sync.WaitGroup
			factory := stream.NewFactoryFromEventHandler(func(conn stream.ConnID) stream.MySQLEventHandler {
				logger := conn.Logger("replay")
				fileName := conn.HashStr() + ":" + conn.SrcAddr()
				f, err := util.OpenFile(outputDir, fileName)
				if err != nil {
					panic("create and open  file fail , " + outputDir + "/" + fileName)
				}
				return &replayEventHandler{
					pconn:       conn,
					log:         logger,
					dsn:         dsn,
					MySQLConfig: MySQLConfig,
					ctx:         context.Background(),
					ch:          make(chan stream.MySQLEvent, 100),
					wg:          &wg,
					stmts:       make(map[uint64]statement),
					file:        f,
				}
			}, options)
			pool := reassembly.NewStreamPool(factory)
			assembler := reassembly.NewAssembler(pool)

			handle := func(name string) error {
				var f *pcap.Handle
				f, err = pcap.OpenOffline(name)
				if err != nil {
					return errors.Annotate(err, "open "+name)
				}
				defer f.Close()
				//logger := zap.L().With(zap.String("conn", "compare"))
				src := gopacket.NewPacketSource(f, f.LinkType())
				for pkt := range src.Packets() {
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

			for _, in := range args {
				zap.L().Info("processing " + in)
				err = handle(in)
				if err != nil && err != ERRORTIMEOUT {
					return err
				} else if err == ERRORTIMEOUT {
					break
				}
				assembler.FlushCloseOlderThan(factory.LastStreamTime().Add(-3 * time.Minute))
			}

			assembler.FlushAll()
			if rt > 0 {
				ticker.Stop()
			}
			StaticPrint()
			log.Info("process end run at " + time.Now().String())
			return nil
		},
	}

	cmd.Flags().StringVarP(&dsn, "dsn", "d", "", "replay server dsn")
	cmd.Flags().BoolVar(&options.ForceStart, "force-start", false, "accept streams even if no SYN have been seen")
	cmd.Flags().StringVarP(&runTime, "runtime", "t", "0", "replay server run time")
	cmd.Flags().StringVarP(&outputDir, "output", "o", "./output", "directory used to write the result set")
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
	rrStartGoRuntine bool
	ch               chan stream.MySQLEvent
	wg               *sync.WaitGroup
	file             *os.File
	//Rr           *stream.ReplayRes
}

func (h *replayEventHandler) ReplayEvent(ch chan stream.MySQLEvent, wg *sync.WaitGroup) {
	defer func() {
		if err := recover(); err != nil {
			h.log.Warn(err.(string))
		}

	}()
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
					//fmt.Println(err.Error(), ok)
					e.Rr.ErrNO = 20000
					e.Rr.ErrDesc = "exec sql fail and coverted to mysql errorstruct err"
				}
			}

			res :=stream.NewResForWriteFile(e.Pr,e.Rr,&e,h.file)
			_,err = res.WriteResToFile()
			if err!=nil {
				h.log.Warn("write compare result to file fail , " +err.Error() )
			}
			/*
				res := h.fsm.CompareRes(e.Pr, e.Rr)
				if res.ErrCode != 0 {
					logstr, err := json.Marshal(res)
					if err != nil {
						h.log.Warn("compare result marshal to json error " + err.Error())
						continue
					}
					h.log.Info(string(logstr))
				}
			*/
		} else {
			wg.Done()
			h.log.Info("chan close ,func exit ")
			return
		}

	}
	//	return
}

func (h *replayEventHandler) OnEvent(e stream.MySQLEvent) {
	//Process SQL events. Note that unlike the events in binlog,
	//this SQL event is raw and may involve multiple rows

	e.Rr = new(stream.ReplayRes)
	e.InitRr()
	//e.Logger=h.log
	if h.rrStartGoRuntine == false {
		h.wg.Add(1)
		go h.ReplayEvent(h.ch, h.wg)
		h.rrStartGoRuntine = true
	} else {
		h.ch <- e
	}
}

func StaticPrint() {
	//print static message

	stream.Sm.Lock()
	defer stream.Sm.Unlock()
	fmt.Println("-------compare result -------------")
	fmt.Println("compare sql : ", stream.ExecSqlNum)
	fmt.Print("compare succ :", stream.ExecSuccNum, " ")
	if stream.ExecSqlNum > 0 {
		fmt.Print(stream.ExecSuccNum*100/stream.ExecSqlNum, "%")
	}
	fmt.Println()
	fmt.Print("compare fail :", stream.ExecFailNum, " ")
	if stream.ExecSqlNum > 0 {
		fmt.Print(stream.ExecFailNum*100/stream.ExecSqlNum, "%")
	}
	fmt.Println()
	fmt.Print("exec errno fail :", stream.ExecErrNoNotEqual, " ")
	if stream.ExecSqlNum > 0 {
		fmt.Print(stream.ExecErrNoNotEqual*100/stream.ExecSqlNum, "%")
	}
	fmt.Println()
	fmt.Print("exec time fail :", stream.ExecTimeNotEqual, " ")
	if stream.ExecSqlNum > 0 {
		fmt.Print(stream.ExecTimeNotEqual*100/stream.ExecSqlNum, "%")
	}
	fmt.Println()
	fmt.Print("row count fail :", stream.RowCountNotequal, " ")
	if stream.ExecSqlNum > 0 {
		fmt.Print(stream.RowCountNotequal*100/stream.ExecSqlNum, "%")
	}
	fmt.Println()
	fmt.Print("row detail fail :", stream.RowDetailNotEqual, " ")
	if stream.ExecSqlNum > 0 {
		fmt.Print(stream.RowDetailNotEqual*100/stream.ExecSqlNum, "%")
	}
	fmt.Println()
	fmt.Println("-------from packet -------------")
	fmt.Println("exec succ sql count :", stream.PrExecSuccCount)
	fmt.Println("exec fail sql count :", stream.PrExecFailCount)
	fmt.Print("exec time :",
		stream.PrExecTimeCount/uint64(time.Millisecond), "  ")
	fmt.Println("reslut rows :", stream.PrExecRowCount)
	if stream.ExecSqlNum > 0 {
		fmt.Printf("avg exec  time: %.2f \n",
			float64(stream.PrExecTimeCount)/float64(stream.ExecSqlNum)/float64(time.Millisecond))
	}
	fmt.Print("max exec time: ",
		stream.PrMaxExecTime/uint64(time.Millisecond), "  ")
	fmt.Println("min exec time: ", stream.PrMinExecTime/uint64(time.Millisecond))
	fmt.Print("exec in 10ms: ", stream.PrExecTimeIn10ms, "  ")
	fmt.Println("exec in 20ms: ", stream.PrExecTimeIn20ms)
	fmt.Print("exec in 30ms: ", stream.PrExecTimeIn30ms, "  ")
	fmt.Println("exec in 40ms: ", stream.PrExecTimeIn40ms)
	fmt.Print("exec in 50ms: ", stream.PrExecTimeIn50ms, "  ")
	fmt.Println("exec in 100ms: ", stream.PrExecTimeIn100ms)
	fmt.Println("exec out 100ms: ", stream.PrExecTimeOut100ms)
	fmt.Println("-------from replay server -------------")
	fmt.Println("exec succ sql count :", stream.RrExecSuccCount)
	fmt.Println("exec fail sql count :", stream.RrExecFailCount)
	fmt.Print("exec time  :",
		stream.RrExecTimeCount/uint64(time.Millisecond), "  ")
	fmt.Println("reslut rows :", stream.RrExecRowCount)
	if stream.ExecSqlNum > 0 {
		fmt.Printf("avg exec  time: %.2f \n",
			float64(stream.RrExecTimeCount)/float64(stream.ExecSqlNum)/float64(time.Millisecond))
	}
	fmt.Println()
	fmt.Print("max exec time: ", stream.RrMaxExecTime/uint64(time.Millisecond), "  ")
	fmt.Println("min exec time: ", stream.RrMinExecTime/uint64(time.Millisecond))
	fmt.Print("exec in 10ms: ", stream.RrExecTimeIn10ms, "  ")
	fmt.Println("exec in 20ms: ", stream.RrExecTimeIn20ms)
	fmt.Print("exec in 30ms: ", stream.RrExecTimeIn30ms, "  ")
	fmt.Println("exec in 40ms: ", stream.RrExecTimeIn40ms)
	fmt.Print("exec in 50ms: ", stream.RrExecTimeIn50ms, "  ")
	fmt.Println("exec in 100ms: ", stream.RrExecTimeIn100ms)
	fmt.Println("exec out 100ms: ", stream.RrExecTimeOut100ms)
	fmt.Println("-------compare result -------------")
}

func (h *replayEventHandler) OnClose() {
	//h.StaticPrint()
	close(h.ch)
	h.wg.Wait()
	err:=h.file.Close()
	if err!=nil{
		h.log.Warn("close file fail , " + h.file.Name()+" " + err.Error())
	}
	if h.fsm != nil {
		h.fsm.AddStatis()
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
		if err != nil {
			if mysqlError, ok = err.(*mysql.MySQLError); ok {
				//If TiDB thrown 1205: Lock wait timeout exceeded; try restarting transaction
				//we try again until execute success
				if mysqlError.Number == 1205 {
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
		if err != nil {
			return nil, err
		}
	}
	if h.conn == nil {
		h.conn, err = h.pool.Conn(ctx)
		if err != nil {
			return nil, err
		}
		stats.Add(stats.Connections, 1)
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
		stats.Add(stats.Connections, -1)
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
	if err != nil {
		return err
	}
	stats.Add(stats.Queries, 1)
	stats.Add(stats.ConnRunning, 1)
	e.Rr.SqlBeginTime = uint64(time.Now().UnixNano())
	e.Rr.SqlStatment = query
	rows, err := conn.QueryContext(ctx, query)
	e.Rr.SqlEndTime = uint64(time.Now().UnixNano())
	defer func() {
		if rows != nil {
			if rs := rows.Close(); rs != nil {
				h.log.Warn("close row fail," + rs.Error())
			}
		}
	}()
	stats.Add(stats.ConnRunning, -1)
	if err != nil {
		//h.Rr.SqlEndTime = time.Now().UnixNano() / 1000000
		stats.Add(stats.FailedQueries, 1)
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
	stats.Add(stats.StmtPrepares, 1)
	stmt.handle, err = conn.PrepareContext(ctx, stmt.query)
	if err != nil {
		stats.Add(stats.FailedStmtPrepares, 1)
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
	stats.Add(stats.StmtExecutes, 1)
	stats.Add(stats.ConnRunning, 1)
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
	stats.Add(stats.ConnRunning, -1)
	if err != nil {
		//h.Rr.SqlEndTime = time.Now().UnixNano() / 1000000
		stats.Add(stats.FailedStmtExecutes, 1)
		return err
	}
	//h.Rr.ColNames, _ = rows.Columns()
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
