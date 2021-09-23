package cmd

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/bobguo/mysql-replay/stats"
	"github.com/bobguo/mysql-replay/stream"
	"github.com/bobguo/mysql-replay/tso"
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

//dump sql event to files from pcap files
/*func NewTextDumpCommand() *cobra.Command {
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
*/

func NewTextDumpReplayCommand() *cobra.Command {
	//Replay sql from pcap files，and compare reslut from pcap file and
	//replay server

	var (
		options = stream.FactoryOptions{Synchronized: true}
		dsn     string
		runTime string
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
			var rt int
			rt, err = strconv.Atoi(runTime)
			if err != nil {
				log.Error("parse runTime error " + err.Error())
				return nil
			}

			if rt > 0 {
				ticker = time.NewTicker(3 * time.Second)
			}
			ticker1 := time.NewTicker(3 * time.Second)
			if len(dsn) == 0 {
				log.Error("need to specify DSN for replay sql ")
				return nil
			}
			var MySQLConfig *mysql.Config
			MySQLConfig, err = mysql.ParseDSN(dsn)
			if err != nil {
				log.Error("fail to parse DSN to MySQLConfig ,", zap.Error(err))
				return nil
			}
			factory := stream.NewFactoryFromEventHandler(func(conn stream.ConnID) stream.MySQLEventHandler {
				logger := conn.Logger("replay")
				return &replayEventHandler{
					pconn:       conn,
					log:         logger,
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
				var f *pcap.Handle
				f, err = pcap.OpenOffline(name)
				if err != nil {
					return errors.Annotate(err, "open "+name)
				}
				defer f.Close()
				logger := zap.L().With(zap.String("conn", "compare"))
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
						case <-ticker1.C:
							LogCompareResutTimer(logger)
						default:
							//
						}
					} else {
						select {
						case <-ticker1.C:
							LogCompareResutTimer(logger)
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
			ticker1.Stop()
			StaticPrintForSelect()
			log.Info("process end run at " + time.Now().String())
			return nil
		},
	}

	cmd.Flags().StringVarP(&dsn, "dsn", "d", "", "replay server dsn")
	cmd.Flags().BoolVar(&options.ForceStart, "force-start", false, "accept streams even if no SYN have been seen")
	cmd.Flags().StringVarP(&runTime, "runtime", "t", "0", "replay server run time")
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
	rrNeedReplay bool
	Rr           *stream.ReplayRes
}

//init values for replay new sql
func (h *replayEventHandler) rrInit() {
	rr := h.Rr
	rr.ErrNO = 0
	rr.ErrDesc = ""
	rr.Values = rr.Values[0:0]
	rr.ColumnNum = 0
	rr.ColNames = rr.ColNames[0:0]
	rr.ColValues = rr.ColValues[0:0][0:0]
	rr.SqlStatment = ""
	rr.SqlBeginTime = 0
	rr.SqlEndTime = 0

	//get checkpoint from tidb interval :1s
	//TODO: input as parameter
	h.rrGetCheckPointTimeInterval = 5

}

//Check whether the SQL needs replay on server
func (h *replayEventHandler) checkRunOrNot(e stream.MySQLEvent) {

	h.rrNeedReplay = true

	//Determine whether to obtain tSO again
	//If the tSO acquisition failed last time, it will be acquired again this time,
	//that is, the TSO acquisition time will not be updated this time
	if time.Since(h.rrLastGetCheckPointTime).Seconds() >
		float64(h.rrGetCheckPointTimeInterval) {
		ts := new(tso.TSO)
		conn, err := h.getConn(h.ctx)
		if err != nil {
			h.log.Error("get conn fail ," + err.Error())
			//conn db fail , the sql will replay on replay server
			//h.rrNeedReplay = false
			return
		}
		ullTs, err := ts.GetTSOFromDB(h.ctx, conn, h.log)
		if err != nil {
			h.log.Error("get tso fail ," + err.Error())
			//get tso physical time fail,the sql will replay on replay server
			//h.rrNeedReplay = false
			return
		}
		ts.ParseTS(ullTs)
		h.rrCheckPoint = ts.GetPhysicalTime()
		h.rrLastGetCheckPointTime = time.Now()
	}

	if h.rrCheckPoint.UnixNano()+
		int64(float64(h.rrGetCheckPointTimeInterval)/2*1000*1000000) < e.Time {
		return
	} else {
		/*logstr := fmt.Sprintf("%d", e.Time-h.rrCheckPoint.UnixNano())
		h.log.Info("replay run fastet than binlog ,wait " + logstr + "Nanosecond")
		time.Sleep(time.Duration(e.Time-h.rrCheckPoint.UnixNano()-7*1000*1000000) *
			time.Nanosecond)
		return*/
		h.rrNeedReplay = false
		return
	}

}

func (h *replayEventHandler) OnEvent(e stream.MySQLEvent) {
	//Process SQL events. Note that unlike the events in binlog,
	//this SQL event is raw and may involve multiple rows

	if h.fsm == nil {
		h.fsm = e.Fsm
	}
	h.rrInit()

	err := h.ApplyEvent(h.ctx, e)
	if err != nil {
		if mysqlError, ok := err.(*mysql.MySQLError); ok {
			h.Rr.ErrNO = mysqlError.Number
			h.Rr.ErrDesc = mysqlError.Message
		} else {
			//fmt.Println(err.Error(), ok)
			h.Rr.ErrNO = 20000
			h.Rr.ErrDesc = "exec sql fail and coverted to mysql errorstruct err"
		}
	}

	defer func() {
		if err := recover(); err != nil {
			h.log.Warn(err.(string))
		}

	}()

	res := h.fsm.CompareRes(h.Rr)
	if res.ErrCode != 0 {
		logstr, err := json.Marshal(res)
		if err != nil {
			h.log.Warn("compare result marshal to json error " + err.Error())
			return
		}
		h.log.Info(string(logstr))
	}
	return

}

func LogCompareResutTimer(log *zap.Logger) {
	stream.Sm.Lock()
	defer stream.Sm.Unlock()
	log.Info("exec sql :" + fmt.Sprintf("%v", stream.ExecSqlNum))
	log.Info("compare succ :" + fmt.Sprintf("%v", stream.ExecSuccNum))
	if stream.ExecSqlNum > 0 {
		log.Info("sompare succ proportion " + fmt.Sprintf("%v", stream.ExecSuccNum*100/stream.ExecSqlNum) + "%")
	}
}

func StaticPrintForSelect() {
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
	if h.fsm != nil {
		h.fsm.AddStatis()
	}
	h.quit(false)
}

func (h *replayEventHandler) ApplyEvent(ctx context.Context, e stream.MySQLEvent) error {
	//apply mysql event on replay server
	var err error
LOOP:
	switch e.Type {
	case stream.EventQuery:
		h.Rr.ColValues = make([][]driver.Value, 0)
		err = h.execute(ctx, e.Query)

	case stream.EventStmtPrepare:
		err = h.stmtPrepare(ctx, e.StmtID, e.Query)
		if err != nil {
			if mysqlError, ok := err.(*mysql.MySQLError); ok {
				logstr := fmt.Sprintf("prepare statment exec fail ,%s , %d ,%s ",
					e.Query, mysqlError.Number, mysqlError.Message)
				h.log.Error(logstr)
			} else {
				h.Rr.ErrNO = 20000
				h.Rr.ErrDesc = "exec sql fail and coverted to mysql errorstruct err"
			}
		}
	case stream.EventStmtExecute:
		_, ok := h.stmts[e.StmtID]
		if ok {
			h.Rr.ColValues = make([][]driver.Value, 0)
			err = h.stmtExecute(ctx, e.StmtID, e.Params)
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

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	//for sql "select ... for update "
	if h.fsm.IsSelectStmtOrSelectPrepare(h.filterStr) {
		_, err = db.Exec("set autocommit = on ;")
		if err != nil {
			rs := db.Close()
			if rs != nil {
				h.log.Warn("close db fail ," + rs.Error())
			}
			return nil, err
		}
	}
	return db, err
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
func (h *replayEventHandler) execute(ctx context.Context, query string) error {
	conn, err := h.getConn(ctx)
	if err != nil {
		return err
	}
	stats.Add(stats.Queries, 1)
	stats.Add(stats.ConnRunning, 1)
	h.Rr.SqlBeginTime = uint64(time.Now().UnixNano())
	h.Rr.SqlStatment = query
	rows, err := conn.QueryContext(ctx, query)
	h.Rr.SqlEndTime = uint64(time.Now().UnixNano())
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
		h.ReadRowValues(rows)
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
func (h *replayEventHandler) stmtExecute(ctx context.Context, id uint64, params []interface{}) error {
	stmt, err := h.getStmt(ctx, id)
	if err != nil {
		return err
	}

	h.Rr.SqlStatment = h.getQuery(stmt)
	h.Rr.Values = params
	stats.Add(stats.StmtExecutes, 1)
	stats.Add(stats.ConnRunning, 1)
	h.Rr.SqlBeginTime = uint64(time.Now().UnixNano())
	rows, err := stmt.QueryContext(ctx, params...)
	h.Rr.SqlEndTime = uint64(time.Now().UnixNano())
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
		h.ReadRowValues(rows)
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

func (h *replayEventHandler) GetColNames(f *sql.Rows) {
	//Get column from sql.Rows structure
	var err error
	h.Rr.ColNames, err = f.Columns()
	if err != nil {
		h.log.Warn("read column name err ,", zap.Error(err))
	}
}

func (h *replayEventHandler) ReadRowValues(f *sql.Rows) {
	//Get the lastcols value from the sql.Rows
	//structure using unsafe and reflection mechanisms
	//and load it into the cache

	rs := reflect.ValueOf(f)
	foo := rs.Elem().FieldByName("lastcols")
	rf := foo
	rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()
	z := rf.Interface().([]driver.Value)
	rr :=make([]driver.Value,0,len(z))
	var err error
	for i:= range z {
		if z[i] ==nil{
			rr=append(rr,nil)
			continue
		}
		var a string
		err = stream.ConvertAssignRows(z[i],&a)
		if err ==nil{
			rr=append(rr,a)
		} else{
			h.log.Warn("get row values fail , covert column value to string fail ,"+err.Error())
		}
	}
	if err == nil{
		h.Rr.ColValues = append(h.Rr.ColValues, rr)
	}
}

/*type textDumpHandler struct {
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
}*/

func NewTextCommand() *cobra.Command {
	//add sub command replay

	cmd := &cobra.Command{
		Use:   "text",
		Short: "Text format utilities",
	}
	//cmd.AddCommand(NewTextDumpCommand())
	cmd.AddCommand(NewTextDumpReplayCommand())
	return cmd
}
