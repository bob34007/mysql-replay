package stream

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/gopacket/reassembly"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

var (
	Sm                 sync.Mutex
	ExecSqlNum         uint64
	ExecSuccNum        uint64
	ExecFailNum        uint64
	ExecErrNoNotEqual  uint64
	ExecTimeNotEqual   uint64
	RowCountNotequal   uint64
	RowDetailNotEqual  uint64
	PrExecRowCount     uint64
	PrExecSuccCount    uint64
	PrExecFailCount    uint64
	PrExecTimeCount    uint64
	PrMaxExecTime      uint64
	PrMinExecTime      uint64
	PrExecTimeIn10ms   uint64
	PrExecTimeIn20ms   uint64
	PrExecTimeIn30ms   uint64
	PrExecTimeIn40ms   uint64
	PrExecTimeIn50ms   uint64
	PrExecTimeIn100ms  uint64
	PrExecTimeOut100ms uint64
	RrExecTimeCount    uint64
	RrExecRowCount     uint64
	RrExecSuccCount    uint64
	RrExecFailCount    uint64
	RrMaxExecTime      uint64
	RrMinExecTime      uint64
	RrExecTimeIn10ms   uint64
	RrExecTimeIn20ms   uint64
	RrExecTimeIn30ms   uint64
	RrExecTimeIn40ms   uint64
	RrExecTimeIn50ms   uint64
	RrExecTimeIn100ms  uint64
	RrExecTimeOut100ms uint64
)

func init() {
	ExecSqlNum = 0
	ExecSuccNum = 0
	ExecFailNum = 0
	ExecTimeNotEqual = 0
	ExecErrNoNotEqual = 0
	RowCountNotequal = 0
	RowDetailNotEqual = 0
	PrExecTimeCount = 0
	PrExecRowCount = 0
	PrExecSuccCount = 0
	PrExecFailCount = 0
	PrMaxExecTime = 0
	PrMinExecTime = 10000000
	RrExecTimeCount = 0
	RrExecRowCount = 0
	RrExecSuccCount = 0
	RrExecFailCount = 0
	RrMaxExecTime = 0
	RrMinExecTime = 10000000

	PrExecTimeIn10ms = 0
	PrExecTimeIn20ms = 0
	PrExecTimeIn30ms = 0
	PrExecTimeIn40ms = 0
	PrExecTimeIn50ms = 0
	PrExecTimeIn100ms = 0
	PrExecTimeOut100ms = 0
	RrExecTimeIn10ms = 0
	RrExecTimeIn20ms = 0
	RrExecTimeIn30ms = 0
	RrExecTimeIn40ms = 0
	RrExecTimeIn50ms = 0
	RrExecTimeIn100ms = 0
	RrExecTimeOut100ms = 0
}

const (
	StateInit = iota
	StateUnknown
	StateComQuery
	StateComStmtExecute
	StateComStmtClose
	StateComStmtPrepare0
	StateComStmtPrepare1
	StateComQuit
	StateHandshake0
	StateHandshake1
	StateComQuery1
	StateComQuery2
	StateComStmtExecute1
	StateComStmtExecute2
)

func StateName(state int) string {
	switch state {
	case StateInit:
		return "Init"
	case StateUnknown:
		return "Unknown"
	case StateComQuery:
		return "ComQuery"
	case StateComQuery1:
		return "ReadingComQueryRes"
	case StateComQuery2:
		return "ReadComQueryResEnd"
	case StateComStmtExecute:
		return "ComStmtExecute"
	case StateComStmtExecute1:
		return "ReadingComStmtExecuteRes"
	case StateComStmtExecute2:
		return "ReadingComStmtExecuteEnd"
	case StateComStmtClose:
		return "ComStmtClose"
	case StateComStmtPrepare0:
		return "ComStmtPrepare0"
	case StateComStmtPrepare1:
		return "ComStmtPrepare1"
	case StateComQuit:
		return "ComQuit"
	case StateHandshake0:
		return "Handshake0"
	case StateHandshake1:
		return "Handshake1"
	default:
		return "Invalid"
	}
}

type Stmt struct {
	ID        uint32
	Query     string
	NumParams int

	types []byte
}

func NewMySQLFSM(log *zap.Logger) *MySQLFSM {
	return &MySQLFSM{
		log:     log,
		state:   StateInit,
		data:    new(bytes.Buffer),
		stmts:   map[uint32]Stmt{},
		params:  []interface{}{},
		packets: []MySQLPacket{},
	}
}

//use for save result from replay server
type ReplayRes struct {
	ErrNO   uint16
	ErrDesc string
	//AffectedRows uint64
	//InsertId     uint64
	SqlStatment  string
	Values       []interface{}
	SqlBeginTime uint64
	SqlEndTime   uint64
	//	SqlExecTime  int64
	ColumnNum int
	ColNames  []string
	ColValues [][]driver.Value
}

//use for save result from packet (pcap)
type PacketRes struct {
	//save for query result
	errNo        uint16
	errDesc      string
	affectedRows uint64
	insertId     uint64
	status       statusFlag
	parseTime    bool
	packetnum    int
	sqlBeginTime uint64
	sqlEndTime   uint64
	columnNum    int
	bRows        *binaryRows
	tRows        *textRows
	readColEnd   bool
	//Use to ignore the EOF package following the Columns message package
	ifReadColEndEofPacket bool
	//Indicates whether the result set is finished reading
	ifReadResEnd bool
}

type FSMStatic struct {
	execSqlNum         uint64
	execSuccNum        uint64
	execFailNum        uint64
	execErrNoNotEqual  uint64
	execTimeNotEqual   uint64
	rowCountNotequal   uint64
	rowDetailNotEqual  uint64
	prRowNumCount      uint64
	prExecTimeCount    uint64
	prExecSuccCount    uint64
	prExecFailCount    uint64
	prMaxExecTime      uint64
	prMinExecTime      uint64
	prExecTimeIn10ms   uint64
	prExecTimeIn20ms   uint64
	prExecTimeIn30ms   uint64
	prExecTimeIn40ms   uint64
	prExecTimeIn50ms   uint64
	prExecTimeIn100ms  uint64
	prExecTimeOut100ms uint64
	rrExecTimeIn10ms   uint64
	rrExecTimeIn20ms   uint64
	rrExecTimeIn30ms   uint64
	rrExecTimeIn40ms   uint64
	rrExecTimeIn50ms   uint64
	rrExecTimeIn100ms  uint64
	rrExecTimeOut100ms uint64
	rrRowNumCount      uint64
	rrExecTimeCount    uint64
	rrExecSuccCount    uint64
	rrExecFailCount    uint64
	rrMaxExecTime      uint64
	rrMinExecTime      uint64
}

type MySQLFSM struct {
	log *zap.Logger

	// state info
	changed bool
	state   int
	query   string        // com_query
	stmt    Stmt          // com_stmt_prepare,com_stmt_execute,com_stmt_close
	params  []interface{} // com_stmt_execute

	// session info
	schema string          // handshake1
	stmts  map[uint32]Stmt // com_stmt_prepare,com_stmt_execute,com_stmt_close

	// current command
	data    *bytes.Buffer
	packets []MySQLPacket
	start   int
	count   int
	pr      *PacketRes
	//Rr          *ReplayRes
	FSMStatic
}

func (fsm *MySQLFSM) State() int { return fsm.state }

func (fsm *MySQLFSM) Query() string { return fsm.query }

func (fsm *MySQLFSM) Stmt() Stmt { return fsm.stmt }

func (fsm *MySQLFSM) Stmts() []Stmt {
	stmts := make([]Stmt, 0, len(fsm.stmts))
	for _, stmt := range fsm.stmts {
		stmts = append(stmts, stmt)
	}
	return stmts
}

func (fsm *MySQLFSM) StmtParams() []interface{} { return fsm.params }

func (fsm *MySQLFSM) Schema() string { return fsm.schema }

func (fsm *MySQLFSM) Changed() bool { return fsm.changed }

func (fsm *MySQLFSM) Ready() bool {
	n := len(fsm.packets)
	return n > 0 && fsm.packets[n-1].Len < maxPacketSize
}

//When a message packet with sequence number 0 is received,
//initialize some variables
func (fsm *MySQLFSM) InitValue() {
	fsm.set(StateInit, "recv packet with seq(0)")
	pr := new(PacketRes)
	fsm.pr = pr
	pr.readColEnd = false
	pr.packetnum = 0
	pr.columnNum = 0
	pr.sqlBeginTime = 0
	pr.sqlEndTime = 0
	pr.bRows = nil
	pr.tRows = nil
	pr.ifReadColEndEofPacket = false
	pr.ifReadResEnd = false
	fsm.packets = fsm.packets[:0]
}

func (fsm *MySQLFSM) Handle(pkt MySQLPacket) {
	fsm.changed = false
	if fsm.state == StateComQuit {
		return
	}
	//Message sequence numbers may reuse
	//serial number 0 for large result sets
	if pkt.Seq == 0 &&
		fsm.State() != StateComQuery1 &&
		fsm.State() != StateComStmtExecute1 {
		fsm.InitValue()
		fsm.pr.sqlBeginTime = uint64(pkt.Time.UnixNano())
		fsm.log.Debug("sql begin time is :" + fmt.Sprintf("%v", fsm.pr.sqlBeginTime))
		fsm.packets = append(fsm.packets, pkt)
	} else if fsm.nextSeq() == pkt.Seq {
		fsm.packets = append(fsm.packets, pkt)
	} else {
		return
	}

	if !fsm.Ready() {
		return
	}

	if fsm.state == StateInit {
		fsm.handleInitPacket()
	} else if fsm.state == StateComStmtPrepare0 {
		fsm.handleComStmtPrepareResponse()
	} else if fsm.state == StateHandshake0 {
		fsm.handleHandshakeResponse()
	} else if fsm.state == StateComQuery || fsm.state == StateComQuery1 {
		if fsm.state == StateComQuery {
			fsm.setStatusWithNoChange(StateComQuery1)
		}
		err := fsm.handleReadSQLResult()
		if err != nil {
			fsm.log.Warn("read packet fail ," + err.Error())
		}
		if fsm.pr.tRows != nil {
			if fsm.pr.tRows.rs.done {
				fsm.pr.ifReadResEnd = true
			}
		}
		if fsm.pr.ifReadResEnd {
			fsm.set(StateComQuery2)
			fsm.pr.sqlEndTime = uint64(pkt.Time.UnixNano())
			fsm.log.Debug("the query exec time is :" +
				fmt.Sprintf("%v", fsm.pr.sqlEndTime-fsm.pr.sqlBeginTime) +
				"ms")
		}
	} else if fsm.state == StateComStmtExecute || fsm.state == StateComStmtExecute1 {
		if fsm.state == StateComStmtExecute {
			fsm.setStatusWithNoChange(StateComStmtExecute1)
		}
		err := fsm.handleReadPrepareExecResult()
		if err != nil {
			fsm.log.Warn("read packet fail ," + err.Error())
		}
		if fsm.pr.bRows != nil {
			if fsm.pr.bRows.rs.done {
				fsm.pr.ifReadResEnd = true
			}
		}
		if fsm.pr.ifReadResEnd {
			fsm.set(StateComStmtExecute2)
			fsm.pr.sqlEndTime = uint64(pkt.Time.UnixNano())
			fsm.log.Debug("sql end time is :" + fmt.Sprintf("%v", fsm.pr.sqlEndTime))
			fsm.log.Debug("the query exec time is :" +
				fmt.Sprintf("%v", fsm.pr.sqlEndTime-fsm.pr.sqlBeginTime) +
				"ms")
		}
	}

	/*if (fsm.state == StateComQuery || fsm.state == StateComStmtExecute) &&
		fsm.pr.sqlBeginTime == 0 {
		fsm.pr.sqlBeginTime = pkt.Time.UnixNano() / int64(time.Millisecond)
		fsm.log.Debug("sql begin time is :" + fmt.Sprintf("%v", fsm.pr.sqlBeginTime))
	}*/
}

func (fsm *MySQLFSM) Packets() []MySQLPacket {
	if fsm.start+fsm.count > len(fsm.packets) {
		return nil
	}
	return fsm.packets[fsm.start : fsm.start+fsm.count]
}

func (fsm *MySQLFSM) nextSeq() int {
	n := len(fsm.packets)
	if n == 0 {
		return 0
	}
	return fsm.packets[n-1].Seq + 1
}

func (fsm *MySQLFSM) load(k int) bool {
	i, j := 0, 0
	for i < len(fsm.packets) {
		j = i
		for j < len(fsm.packets) && fsm.packets[j].Len == maxPacketSize {
			j += 1
		}
		if j == len(fsm.packets) {
			return false
		}
		if i == k {
			fsm.data.Reset()
			for k <= j {
				fsm.data.Write(fsm.packets[k].Data)
				k += 1
			}
			fsm.start, fsm.count = i, j-i+1
			return true
		}
		i = j + 1
	}
	return false
}

//Only change status ,do not modify fsm.changed
//Used in comQuery and comstmTexecut state
//for read result
func (fsm *MySQLFSM) setStatusWithNoChange(to int) {
	from := fsm.state
	fsm.state = to
	logstr := fmt.Sprintf("fsm.stsatus changed  %s -> %s ", StateName(from), StateName(to))
	fsm.log.Debug(logstr)
}

//set fsm state
func (fsm *MySQLFSM) set(to int, msg ...string) {
	from := fsm.state
	fsm.state = to
	fsm.changed = from != to
	if !fsm.changed || fsm.log == nil || !fsm.log.Core().Enabled(zap.DebugLevel) {
		return
	}
	tmpl := "mysql fsm(%s->%s)"
	query := fsm.query
	if to != StateComQuery {
		query = fsm.stmt.Query
	}
	if n := len(query); n > 500 {
		query = query[:300] + "..." + query[n-196:]
	}
	switch to {
	case StateComQuery:
		tmpl += fmt.Sprintf("{query:%q}", query)
	case StateComStmtExecute:
		tmpl += fmt.Sprintf("{query:%q,id:%d,params:%v}", query, fsm.stmt.ID, fsm.params)
	case StateComStmtPrepare0:
		tmpl += fmt.Sprintf("{query:%q}", query)
	case StateComStmtPrepare1:
		tmpl += fmt.Sprintf("{query:%q,id:%d,num-params:%d}", query, fsm.stmt.ID, fsm.stmt.NumParams)
	case StateComStmtClose:
		tmpl += fmt.Sprintf("{query:%q,id:%d,num-params:%d}", query, fsm.stmt.ID, fsm.stmt.NumParams)
	case StateHandshake1:
		tmpl += fmt.Sprintf("{schema:%q}", fsm.schema)
	}
	if len(msg) > 0 {
		tmpl += ": " + msg[0]
	}
	fsm.log.Debug(tmpl + StateName(from) + StateName(to))
}

func (fsm *MySQLFSM) assertDir(exp reassembly.TCPFlowDirection) bool {
	return fsm.start < len(fsm.packets) && fsm.packets[fsm.start].Dir == exp
}

func (fsm *MySQLFSM) assertDataByte(offset int, exp byte) bool {
	data := fsm.data.Bytes()
	if len(data) <= offset {
		return false
	}
	return data[offset] == exp
}

/*func (fsm *MySQLFSM) assertDataChunk(offset int, exp []byte) bool {
	data := fsm.data.Bytes()
	if len(data) < offset+len(exp) {
		return false
	}
	return bytes.Equal(data[offset:offset+len(exp)], exp)
}*/

func (fsm *MySQLFSM) isClientCommand(cmd byte) bool {
	if !fsm.assertDir(reassembly.TCPDirClientToServer) {
		return false
	}
	return fsm.assertDataByte(0, cmd)
}

func (fsm *MySQLFSM) isHandshakeRequest() bool {
	if !fsm.assertDir(reassembly.TCPDirServerToClient) {
		return false
	}
	data := fsm.data.Bytes()
	if len(data) < 6 {
		return false
	}
	return data[0] == handshakeV9 || data[0] == handshakeV10
}

func (fsm *MySQLFSM) handleInitPacket() {
	if !fsm.load(0) {
		fsm.set(StateUnknown, "init: cannot load packet")
		fsm.log.Warn("init :load packet fail")
		return
	}
	if fsm.isClientCommand(comQuery) {
		fsm.handleComQueryNoLoad()

	} else if fsm.isClientCommand(comStmtExecute) {
		fsm.handleComStmtExecuteNoLoad()
	} else if fsm.isClientCommand(comStmtPrepare) {
		fsm.handleComStmtPrepareRequestNoLoad()
	} else if fsm.isClientCommand(comStmtClose) {
		fsm.handleComStmtCloseNoLoad()
	} else if fsm.isClientCommand(comQuit) {
		fsm.set(StateComQuit)
	} else if fsm.isHandshakeRequest() {
		fsm.set(StateHandshake0)
	} else {
		if fsm.assertDir(reassembly.TCPDirClientToServer) && fsm.data.Len() > 0 {
			fsm.set(StateUnknown, fmt.Sprintf("init: skip client command(0x%02x)", fsm.data.Bytes()[0]))
		} else {
			fsm.set(StateUnknown, "init: unsupported packet")
			//The first character indicates the current command type
			fsm.log.Warn("unsupported command :" + string(fsm.data.Bytes()[:1]))
		}
	}
}

func (fsm *MySQLFSM) handleComQueryNoLoad() {
	fsm.query = string(fsm.data.Bytes()[1:])
	fsm.set(StateComQuery)
}

func (fsm *MySQLFSM) handleComStmtExecuteNoLoad() {
	var (
		ok     bool
		id     uint32
		stmt   Stmt
		params []interface{}
	)
	data := fsm.data.Bytes()[1:]
	if id, data, ok = readUint32(data); !ok {
		fsm.set(StateUnknown, "stmt execute: cannot read stmt id")
		var n int = 4
		if len(data) < 4 {
			n = len(data)
		}
		fsm.log.Warn("can not read stmt id from data :" + string(data[:n]))
		return
	}
	if stmt, ok = fsm.stmts[id]; !ok {
		fsm.set(StateUnknown, "stmt execute: unknown stmt id")
		fsm.log.Info("unknown stmt id " + fmt.Sprintf("%v", id))
		return
	}
	if _, data, ok = readBytesN(data, 5); !ok {
		fsm.set(StateUnknown, "stmt execute: cannot read flag and iteration-count")
		var n int = 5
		if len(data) < 5 {
			n = len(data)
		}
		fsm.log.Warn("can not read flag and iteration-count from ," + string(data[:n]))
		return
	}
	if stmt.NumParams > 0 {
		var (
			nullBitmaps []byte
			paramTypes  []byte
			paramValues []byte
			err         error
		)
		if nullBitmaps, data, ok = readBytesN(data, (stmt.NumParams+7)>>3); !ok {
			fsm.set(StateUnknown, "stmt execute: cannot read null-bitmap")
			var n int = stmt.NumParams + 7>>3
			if len(data) < (stmt.NumParams + 7>>3) {
				n = len(data)
			}
			fsm.log.Warn("can not read null bitmap from " + string(data[:n]))
			return
		}
		if len(data) < 1+2*stmt.NumParams {
			fsm.set(StateUnknown, "stmt execute: cannot read params")
			fsm.log.Warn("can not read params ,Package is not complete " +
				fmt.Sprintf("%v-%v", len(data), 1+2*stmt.NumParams))
			return
		}
		if data[0] == 1 {
			paramTypes = data[1 : 1+(stmt.NumParams<<1)]
			paramValues = data[1+(stmt.NumParams<<1):]
			stmt.types = make([]byte, len(paramTypes))
			copy(stmt.types, paramTypes)
			fsm.stmts[id] = stmt
		} else {
			if stmt.types == nil {
				fsm.set(StateUnknown, "stmt execute: param types is missing")
				fsm.log.Warn("can get stmt param type ")
				return
			}
			paramTypes = stmt.types
			paramValues = data[1:]
		}
		params, err = parseExecParams(stmt, nullBitmaps, paramTypes, paramValues)
		if err != nil {
			fsm.set(StateUnknown, "stmt execute: "+err.Error())
			fsm.log.Warn("parse exec params fail " + err.Error())
			return
		}
	}
	fsm.stmt = stmt
	fsm.params = params
	fsm.set(StateComStmtExecute)
}

//Check whether the statement is a SELECT statement
//or a SELECT prepare statement
func (fsm *MySQLFSM) IsSelectStmtOrSelectPrepare(query string) bool {
	/*s := strings.ToLower(query)
	s1 := strings.TrimSpace(s)
	return strings.HasPrefix(s1, "select")*/
	fsm.log.Debug(query)
	if len(query) < 6 {
		return false
	}
	for i, x := range query {
		if x == ' ' {
			continue
		} else {
			if len(query)-i < 6 {
				return false
			} else {
				if (query[i] == 'S' || query[i] == 's') &&
					(query[i+1] == 'E' || query[i+1] == 'e') &&
					(query[i+2] == 'L' || query[i+2] == 'l') &&
					(query[i+3] == 'E' || query[i+3] == 'e') &&
					(query[i+4] == 'C' || query[i+4] == 'c') &&
					(query[i+5] == 'T' || query[i+5] == 't') {
					return true
				}
				return false
			}
		}

	}
	return false
}

//handle prepare close
func (fsm *MySQLFSM) handleComStmtCloseNoLoad() {
	stmtID, _, ok := readUint32(fsm.data.Bytes()[1:])
	if !ok {
		fsm.set(StateUnknown, "stmt close: cannot read stmt id")
		var n int = 4
		if len(fsm.data.Bytes()[1:]) < 4 {
			n = len(fsm.data.Bytes()[1:])
		}

		fsm.log.Warn("can not read stmt id from data ," + string(fsm.data.Bytes()[1:][:n]))
		return
	}
	fsm.stmt = fsm.stmts[stmtID]
	delete(fsm.stmts, stmtID)
	fsm.set(StateComStmtClose)
}

func (fsm *MySQLFSM) handleComStmtPrepareRequestNoLoad() {
	fsm.stmt = Stmt{Query: string(fsm.data.Bytes()[1:])}
	fsm.set(StateComStmtPrepare0)
}

//handle prepare response
func (fsm *MySQLFSM) handleComStmtPrepareResponse() {
	if !fsm.load(1) {
		fsm.set(StateUnknown, "stmt prepare: cannot load packet")
		fsm.log.Warn("parse prepare reaponse fail , can not load packet " +
			fmt.Sprintf("%v", len(fsm.packets)))
		return
	}
	if !fsm.assertDir(reassembly.TCPDirServerToClient) {
		fsm.set(StateUnknown, "stmt prepare: unexpected packet direction")
		fsm.log.Warn("parse prepare reaponse fail , unexpected packet direction")
		return
	}
	if !fsm.assertDataByte(0, 0) {
		fsm.set(StateUnknown, "stmt prepare: not ok")
		fsm.log.Info("prepare fail on server ")
		return
	}
	var (
		stmtID    uint32
		numParams uint16
		ok        bool
	)
	data := fsm.data.Bytes()[1:]
	if stmtID, data, ok = readUint32(data); !ok {
		fsm.set(StateUnknown, "stmt prepare: cannot read stmt id")
		var n int = 4
		if len(data) < 4 {
			n = len(data)
		}
		fsm.log.Warn("can not read stmt id from prepare response packet," +
			string(data[:n]))
		return
	}
	if _, data, ok = readUint16(data); !ok {
		fsm.set(StateUnknown, "stmt prepare: cannot read number of columns")
		var n int = 2
		if len(data) < 2 {
			n = len(data)
		}
		fsm.log.Warn("can not read number of colunms  from prepare response packet," +
			string(data[:n]))
		return
	}
	if numParams, _, ok = readUint16(data); !ok {
		fsm.set(StateUnknown, "stmt prepare: cannot read number of params")
		var n int = 2
		if len(data) < 2 {
			n = len(data)
		}
		fsm.log.Warn("can not read number of params  from prepare response packet," +
			string(data[:n]))
		return
	}
	fsm.stmt.ID = stmtID
	fsm.stmt.NumParams = int(numParams)
	fsm.stmts[stmtID] = fsm.stmt
	fsm.set(StateComStmtPrepare1)
}

//handle handshake response
func (fsm *MySQLFSM) handleHandshakeResponse() {
	if !fsm.load(1) {
		fsm.set(StateUnknown, "handshake: cannot load packet")
		fsm.log.Warn("parse prepare reaponse fail , can not load packet " +
			fmt.Sprintf("%v", len(fsm.packets)))
		return
	}
	if !fsm.assertDir(reassembly.TCPDirClientToServer) {
		fsm.set(StateUnknown, "handshake: unexpected packet direction")
		fsm.log.Warn("parse prepare reaponse fail , unexpected packet direction")
		return
	}
	var (
		flags clientFlag
		bs    []byte
		ok    bool
	)
	data := fsm.data.Bytes()
	if bs, data, ok = readBytesN(data, 2); !ok {
		fsm.set(StateUnknown, "handshake: cannot read capability flags")
		var n int = 2
		if len(data) < 2 {
			n = len(data)
		}
		fsm.log.Warn("cannot read capability flags from packet ," + string(data[:n]))
		return
	}
	flags |= clientFlag(bs[0])
	flags |= clientFlag(bs[1]) << 8
	if flags&clientProtocol41 > 0 {
		if bs, data, ok = readBytesN(data, 2); !ok {
			fsm.set(StateUnknown, "handshake: cannot read extended capability flags")
			var n int = 2
			if len(data) < 2 {
				n = len(data)
			}
			fsm.log.Warn("cannot read extended capability flags from packet ," + string(data[:n]))
			return
		}
		flags |= clientFlag(bs[0]) << 16
		flags |= clientFlag(bs[1]) << 24
		if _, data, ok = readBytesN(data, 28); !ok {
			fsm.set(StateUnknown, "handshake: cannot read max-packet size, character set and reserved")
			return
		}
		if _, data, ok = readBytesNUL(data); !ok {
			fsm.set(StateUnknown, "handshake: cannot read username")
			return
		}
		if flags&clientPluginAuthLenEncClientData > 0 {
			var n uint64
			if n, data, ok = readLenEncUint(data); !ok {
				fsm.set(StateUnknown, "handshake: cannot read length of auth-response")
				return
			}
			if _, data, ok = readBytesN(data, int(n)); !ok {
				fsm.set(StateUnknown, "handshake: cannot read auth-response")
				return
			}
		} else if flags&clientSecureConn > 0 {
			var n []byte
			if n, data, ok = readBytesN(data, 1); !ok {
				fsm.set(StateUnknown, "handshake: cannot read length of auth-response")
				return
			}
			if _, data, ok = readBytesN(data, int(n[0])); !ok {
				fsm.set(StateUnknown, "handshake: cannot read auth-response")
				return
			}
		} else {
			if _, data, ok = readBytesNUL(data); !ok {
				fsm.set(StateUnknown, "handshake: cannot read auth-response")
				return
			}
		}
		if flags&clientConnectWithDB > 0 {
			var db []byte
			if db, data, ok = readBytesNUL(data); !ok {
				fsm.set(StateUnknown, "handshake: cannot read database")
				return
			}
			fsm.schema = string(db)
		}
	} else {
		if _, data, ok = readBytesN(data, 3); !ok {
			fsm.set(StateUnknown, "handshake: cannot read max-packet size")
			return
		}
		if _, data, ok = readBytesNUL(data); !ok {
			fsm.set(StateUnknown, "handshake: cannot read username")
			return
		}
		if flags&clientConnectWithDB > 0 {
			var db []byte
			if _, data, ok = readBytesNUL(data); !ok {
				fsm.set(StateUnknown, "handshake: cannot read auth-response")
				return
			}
			if db, data, ok = readBytesNUL(data); !ok {
				fsm.set(StateUnknown, "handshake: cannot read database")
				return
			}
			fsm.schema = string(db)
		}
	}
	fsm.set(StateHandshake1)
}

//parse  prepare params
func parseExecParams(stmt Stmt, nullBitmap []byte, paramTypes []byte, paramValues []byte) (params []interface{}, err error) {
	defer func() {
		if x := recover(); x != nil {
			params = nil
			err = errors.New("malformed packet")
		}
	}()
	pos := 0
	params = make([]interface{}, stmt.NumParams)
	for i := 0; i < stmt.NumParams; i++ {
		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			params[i] = nil
			continue
		}
		if (i<<1)+1 >= len(paramTypes) {
			return nil, errors.New("malformed types")
		}
		tp := fieldType(paramTypes[i<<1])
		unsigned := (paramTypes[(i<<1)+1] & 0x80) > 0
		switch tp {
		case fieldTypeNULL:
			params[i] = nil
		case fieldTypeTiny:
			if len(paramValues) < pos+1 {
				return nil, errors.New("malformed values")
			}
			if unsigned {
				params[i] = uint64(paramValues[pos])
			} else {
				params[i] = int64(int8(paramValues[pos]))
			}
			pos += 1
		case fieldTypeShort, fieldTypeYear:
			if len(paramValues) < pos+2 {
				return nil, errors.New("malformed values")
			}
			val := binary.LittleEndian.Uint16(paramValues[pos : pos+2])
			if unsigned {
				params[i] = uint64(val)
			} else {
				params[i] = int64(int16(val))
			}
			pos += 2
		case fieldTypeInt24, fieldTypeLong:
			if len(paramValues) < pos+4 {
				return nil, errors.New("malformed values")
			}
			val := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
			if unsigned {
				params[i] = uint64(val)
			} else {
				params[i] = int64(int32(val))
			}
			pos += 4
		case fieldTypeLongLong:
			if len(paramValues) < pos+8 {
				return nil, errors.New("malformed values")
			}
			val := binary.LittleEndian.Uint64(paramValues[pos : pos+8])
			if unsigned {
				params[i] = val
			} else {
				params[i] = int64(val)
			}
			pos += 8
		case fieldTypeFloat:
			if len(paramValues) < pos+4 {
				return nil, errors.New("malformed values")
			}
			params[i] = math.Float32frombits(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			pos += 4
		case fieldTypeDouble:
			if len(paramValues) < pos+8 {
				return nil, errors.New("malformed values")
			}
			params[i] = math.Float64frombits(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			pos += 8
		case fieldTypeDate, fieldTypeTimestamp, fieldTypeDateTime:
			if len(paramValues) < pos+1 {
				return nil, errors.New("malformed values")
			}
			length := paramValues[pos]
			pos += 1
			switch length {
			case 0:
				params[i] = "0000-00-00 00:00:00"
			case 4:
				pos, params[i] = parseBinaryDate(pos, paramValues)
			case 7:
				pos, params[i] = parseBinaryDateTimeReply(pos, paramValues)
			case 11:
				pos, params[i] = parseBinaryTimestamp(pos, paramValues)
			default:
				return nil, errors.New("malformed values")
			}
		case fieldTypeTime:
			if len(paramValues) < pos+1 {
				return nil, errors.New("malformed values")
			}
			length := paramValues[pos]
			pos += 1
			switch length {
			case 0:
			case 8:
				if paramValues[pos] > 1 {
					return nil, errors.New("malformed values")
				}
				pos += 1
				pos, params[i] = parseBinaryTime(pos, paramValues, paramValues[pos-1])
			case 12:
				if paramValues[pos] > 1 {
					return nil, errors.New("malformed values")
				}
				pos += 1
				pos, params[i] = parseBinaryTimeWithMS(pos, paramValues, paramValues[pos-1])
			default:
				return nil, errors.New("malformed values")
			}
		case fieldTypeNewDecimal, fieldTypeDecimal, fieldTypeVarChar, fieldTypeVarString, fieldTypeString, fieldTypeEnum, fieldTypeSet, fieldTypeGeometry, fieldTypeBit:
			if len(paramValues) < pos+1 {
				return nil, errors.New("malformed values")
			}
			v, isNull, n, err := parseLengthEncodedBytes(paramValues[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			if isNull {
				params[i] = nil
			} else {
				params[i] = string(v)
			}
		case fieldTypeBLOB, fieldTypeTinyBLOB, fieldTypeMediumBLOB, fieldTypeLongBLOB:
			if len(paramValues) < pos+1 {
				return nil, errors.New("malformed values")
			}
			v, isNull, n, err := parseLengthEncodedBytes(paramValues[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			if isNull {
				params[i] = nil
			} else {
				params[i] = v
			}
		default:
			return nil, errors.New("unknown field type")
		}
	}

	return params, nil
}

//parse data
func parseBinaryDate(pos int, paramValues []byte) (int, string) {
	year := binary.LittleEndian.Uint16(paramValues[pos : pos+2])
	pos += 2
	month := paramValues[pos]
	pos++
	day := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func parseBinaryDateTimeReply(pos int, paramValues []byte) (int, string) {
	pos, date := parseBinaryDate(pos, paramValues)
	hour := paramValues[pos]
	pos++
	minute := paramValues[pos]
	pos++
	second := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%s %02d:%02d:%02d", date, hour, minute, second)
}

func parseBinaryTimestamp(pos int, paramValues []byte) (int, string) {
	pos, dateTime := parseBinaryDateTimeReply(pos, paramValues)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dateTime, microSecond)
}

func parseBinaryTime(pos int, paramValues []byte, isNegative uint8) (int, string) {
	sign := ""
	if isNegative == 1 {
		sign = "-"
	}
	days := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	hours := paramValues[pos]
	pos++
	minutes := paramValues[pos]
	pos++
	seconds := paramValues[pos]
	pos++
	return pos, fmt.Sprintf("%s%d %02d:%02d:%02d", sign, days, hours, minutes, seconds)
}

func parseBinaryTimeWithMS(pos int, paramValues []byte, isNegative uint8) (int, string) {
	pos, dur := parseBinaryTime(pos, paramValues, isNegative)
	microSecond := binary.LittleEndian.Uint32(paramValues[pos : pos+4])
	pos += 4
	return pos, fmt.Sprintf("%s.%06d", dur, microSecond)
}

//read packet len
//https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
func parseLengthEncodedInt(b []byte) (num uint64, isNull bool, n int) {
	switch b[0] {
	// 251: NULL
	case 0xfb:
		n = 1
		isNull = true
		return

	// 252: value of following 2
	case 0xfc:
		num = uint64(b[1]) | uint64(b[2])<<8
		n = 3
		return

	// 253: value of following 3
	case 0xfd:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16
		n = 4
		return

	// 254: value of following 8
	case 0xfe:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
			uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
			uint64(b[7])<<48 | uint64(b[8])<<56
		n = 9
		return
	}

	// https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer: If the first byte of a packet is a length-encoded integer and its byte value is 0xfe, you must check the length of the packet to verify that it has enough space for a 8-byte integer.
	// TODO: 0xff is undefined

	// 0-250: value of first byte
	num = uint64(b[0])
	n = 1
	return
}

//parse packet length
func parseLengthEncodedBytes(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := parseLengthEncodedInt(b)
	if num < 1 {
		return nil, isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return b[n-int(num) : n], false, n, nil
	}

	return nil, false, n, io.EOF
}

//read uint16 from byte
func readUint16(data []byte) (uint16, []byte, bool) {
	if len(data) < 2 {
		return 0, data, false
	}
	return binary.LittleEndian.Uint16(data), data[2:], true
}

//read uint32 from byte
func readUint32(data []byte) (uint32, []byte, bool) {
	if len(data) < 4 {
		return 0, data, false
	}
	return binary.LittleEndian.Uint32(data), data[4:], true
}

//read n bytes
func readBytesN(data []byte, n int) ([]byte, []byte, bool) {
	if len(data) < n {
		return nil, data, false
	}
	return data[:n], data[n:], true
}

//read byte until 0
func readBytesNUL(data []byte) ([]byte, []byte, bool) {
	for i, b := range data {
		if b == 0 {
			return data[:i], data[i+1:], true
		}
	}
	return nil, data, false
}

//read packet len
//https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
func readLenEncUint(data []byte) (uint64, []byte, bool) {
	if len(data) < 1 {
		return 0, data, false
	}
	if data[0] < 0xfb {
		return uint64(data[0]), data[1:], true
	} else if data[0] == 0xfc {
		if len(data) < 3 {
			return 0, data, false
		}
		return uint64(data[2]) | uint64(data[1])<<8, data[3:], true
	} else if data[0] == 0xfd {
		if len(data) < 4 {
			return 0, data, false
		}
		return uint64(data[3]) | uint64(data[2])<<8 | uint64(data[1])<<16, data[4:], true
	} else if data[0] == 0xfe {
		if len(data) < 9 {
			return 0, data, false
		}
		return binary.BigEndian.Uint64(data[1:]), data[9:], true
	} else {
		return 0, data, false
	}
}

//read sql result from packets
func (fsm *MySQLFSM) handleReadSQLResult() error { //ColumnNum() error {
	var err error
	var rows *textRows

	if fsm.pr.columnNum == 0 {
		//read cloumn num from packet
		fsm.pr.columnNum, err = fsm.readResultSetHeaderPacket()
		if err != nil {
			fsm.log.Warn("read column from packet fail " + err.Error() +
				fmt.Sprintf("%d", fsm.pr.packetnum) +
				fmt.Sprintf("%d", len(fsm.packets)))
			fsm.pr.ifReadResEnd = true
			if mysqlError, ok := err.(*mysql.MySQLError); ok {
				fsm.pr.errNo = mysqlError.Number
				fsm.pr.errDesc = mysqlError.Message
			}
			return err
		}
		if fsm.pr.columnNum == 0 {
			fsm.pr.ifReadResEnd = true
		}
		fsm.log.Debug("read " + fmt.Sprintf("%d", fsm.pr.columnNum) + " columns from packets")
		fsm.log.Debug(fmt.Sprintf("%v", fsm.pr.ifReadResEnd))
		return nil
	}

	if fsm.pr.columnNum > 0 {
		//read column from packet
		if fsm.pr.tRows == nil {
			rows := new(textRows)
			fsm.pr.tRows = rows
			rows.rs.columnValue = make([][]driver.Value, 0)
			rows.rs.columns = make([]mysqlField, 0)
			rows.fsm = fsm
		}
		rows = fsm.pr.tRows
		if !fsm.pr.readColEnd {
			columns, err := fsm.readColumns(1)
			if err != nil {
				fsm.log.Warn("read columns from packet fail " +
					err.Error() + fmt.Sprintf("%d", fsm.pr.packetnum) +
					fmt.Sprintf("%d", len(fsm.packets)))
				return err
			}
			rows.rs.columns = append(rows.rs.columns, columns...)
			fsm.log.Debug(fmt.Sprintf("%d", len(rows.rs.columns)))
			if len(rows.rs.columns) == fsm.pr.columnNum {
				fsm.pr.readColEnd = true
			}
			return nil
		}
		//confirm if it is a  EOF pcaket after column message
		res := fsm.load(fsm.pr.packetnum)
		if res {
			data := fsm.data.Bytes()
			if data[0] == iEOF && !fsm.pr.ifReadColEndEofPacket {
				fsm.pr.packetnum++
				fsm.pr.ifReadColEndEofPacket = true
				fsm.log.Debug("read packet reach EOF , process will ignore EOF ,wait next packet ")
				return nil
			}
		}

		if fsm.pr.columnNum == len(rows.rs.columns) {
			//now begin to read rows
			if !rows.rs.done {
				values := make([]driver.Value, fsm.pr.columnNum)
				err = rows.Next(values)
				if err == nil {
					rows.rs.columnValue = append(rows.rs.columnValue, values)
				}
				if err == io.EOF {
					fsm.log.Debug("read repose end ")
					return nil
				} else if err != nil {
					fsm.log.Warn("resd rows from packet error" + err.Error())
					return err
				}
			}
		}
	}
	return nil
}

//read prepare execute result from packet
func (fsm *MySQLFSM) handleReadPrepareExecResult() error {
	var err error
	var rows *binaryRows
	if fsm.pr.columnNum == 0 {
		fsm.pr.columnNum, err = fsm.readResultSetHeaderPacket()
		if err != nil {
			fsm.log.Warn("read column from packet fail , " +
				err.Error() +
				fmt.Sprintf("%d", fsm.pr.packetnum) +
				fmt.Sprintf("%d", len(fsm.packets)))
			fsm.pr.ifReadResEnd = true
			if mysqlError, ok := err.(*mysql.MySQLError); ok {
				fsm.pr.errNo = mysqlError.Number
				fsm.pr.errDesc = mysqlError.Message
			}
			return err
		}
		if fsm.pr.columnNum == 0 {
			fsm.pr.ifReadResEnd = true
		}
		fsm.log.Debug("read " + fmt.Sprintf("%d", fsm.pr.columnNum) + " columns from packets")
		return nil
	}

	if fsm.pr.columnNum > 0 {
		if fsm.pr.bRows == nil {
			rows = new(binaryRows)
			fsm.pr.bRows = rows
			rows.rs.columns = make([]mysqlField, 0)
			rows.rs.columnValue = make([][]driver.Value, 0)
			rows.fsm = fsm
		}
		rows = fsm.pr.bRows
		if !fsm.pr.readColEnd {
			columns, err := fsm.readColumns(1)
			if err != nil {
				fsm.log.Warn("read columns from packet fail " + err.Error() +
					fmt.Sprintf("%d", fsm.pr.packetnum) +
					fmt.Sprintf("%d", len(fsm.packets)))
				fsm.pr.readColEnd = true
				return err
			}
			rows.rs.columns = append(rows.rs.columns, columns...)
			if len(rows.rs.columns) == fsm.pr.columnNum {
				fsm.pr.readColEnd = true
			}
			fsm.log.Debug("the column number is " +
				fmt.Sprintf("%d", fsm.pr.columnNum) +
				", and read " +
				fmt.Sprintf("%d", len(rows.rs.columns)) +
				" columns ")
			return nil
		}

		//confirm if it is a  EOF pcaket
		res := fsm.load(fsm.pr.packetnum)
		if res {
			data := fsm.data.Bytes()
			if data[0] == iEOF && !fsm.pr.ifReadColEndEofPacket {
				fsm.pr.packetnum++
				fsm.pr.ifReadColEndEofPacket = true
				fsm.log.Debug("read packet reach EOF , process will ignore EOF ,wait next packet ")
				return nil
			}
		}
		if fsm.pr.columnNum == len(rows.rs.columns) {
			//now begin to read column values
			if !rows.rs.done {
				values := make([]driver.Value, fsm.pr.columnNum)
				err = rows.Next(values)
				if err == nil {
					rows.rs.columnValue = append(rows.rs.columnValue, values)
				}
				if err == io.EOF {
					fsm.log.Debug("read respose end ")
					return nil
				}
				if err != nil {
					fsm.log.Debug("resd rows from packet error" +
						err.Error())
					return err
				}
			}
		}
	}
	return nil
}

// Result Set Header Packet
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
func (fsm *MySQLFSM) readResultSetHeaderPacket() (int, error) {
	//data, err := mc.readPacket()
	fsm.pr.packetnum = 1
	res := fsm.load(fsm.pr.packetnum)
	if !res {
		return 0, ErrLoadBuffer
	}
	fsm.pr.packetnum++

	data := fsm.data.Bytes()

	switch data[0] {

	case iOK:
		return 0, fsm.handleOkPacket(data)

	case iERR:
		return 0, fsm.handleErrorPacket(data)

	case iLocalInFile:
		//TODO
		//pcap not contain file text ,so ignore it
		return 0, nil //mc.handleInFileRequest(string(data[1:]))
	}

	// column count
	num, _, n := parseLengthEncodedInt(data)
	if n-len(data) == 0 {
		return int(num), nil
	}
	return 0, ErrMalformPkt
}

//read server status
func readStatus(b []byte) statusFlag {
	return statusFlag(b[0]) | statusFlag(b[1])<<8
}

// Ok Packet
// http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-OK_Packet
func (fsm *MySQLFSM) handleOkPacket(data []byte) error {
	var n, m int

	// 0x00 [1 byte]

	// Affected rows [Length Coded Binary]
	fsm.pr.affectedRows, _, n = readLengthEncodedInteger(data[1:])

	// Insert id [Length Coded Binary]
	fsm.pr.insertId, _, m = readLengthEncodedInteger(data[1+n:])

	// server_status [2 bytes]
	fsm.pr.status = readStatus(data[1+n+m : 1+n+m+2])
	if fsm.pr.status&statusMoreResultsExists != 0 {
		return nil
	}
	// warning count [2 bytes]
	return nil
}

// Error Packet
// http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-ERR_Packet
func (fsm *MySQLFSM) handleErrorPacket(data []byte) error {
	if data[0] != iERR {
		return ErrMalformPkt
	}

	// 0xff [1 byte]

	// Error Number [16 bit uint]
	errno := binary.LittleEndian.Uint16(data[1:3])

	pos := 3

	// SQL State [optional: # + 5bytes string]
	if data[3] == 0x23 {
		//sqlstate := string(data[4 : 4+5])
		pos = 9
	}

	// Error Message [string]
	return &MySQLError{
		Number:  errno,
		Message: string(data[pos:]),
	}
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41
func (fsm *MySQLFSM) readColumns(count int) ([]mysqlField, error) {
	//for i := 0; ; i++ {
	i := 0
	res := fsm.load(fsm.pr.packetnum)
	if !res {
		return nil, ErrLoadBuffer //errors.New("read packet from pcap error ")
	}
	fsm.pr.packetnum++
	data := fsm.data.Bytes()

	// EOF Packet
	if data[0] == iEOF && (len(data) == 5 || len(data) == 1) {
		/*if i == count {
			return columns, nil
		}*/
		return nil, fmt.Errorf("column count mismatch n:%d len:%d", count, 0)
	}
	columns := make([]mysqlField, count)

	// Catalog
	pos, err := skipLengthEncodedString(data)
	if err != nil {
		return nil, err
	}

	// Database [len coded string]
	n, err := skipLengthEncodedString(data[pos:])
	if err != nil {
		return nil, err
	}
	pos += n

	// Table [len coded string]
	/*if mc.cfg.ColumnsWithAlias {
		tableName, _, n, err := readLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n
		columns[i].tableName = string(tableName)
	} else */{
		n, err = skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n
	}

	// Original table [len coded string]
	n, err = skipLengthEncodedString(data[pos:])
	if err != nil {
		return nil, err
	}
	pos += n

	// Name [len coded string]
	name, _, n, err := readLengthEncodedString(data[pos:])
	if err != nil {
		return nil, err
	}
	columns[i].name = string(name)
	pos += n

	// Original name [len coded string]
	n, err = skipLengthEncodedString(data[pos:])
	if err != nil {
		return nil, err
	}
	pos += n

	// Filler [uint8]
	pos++

	// Charset [charset, collation uint8]
	columns[i].charSet = data[pos]
	pos += 2

	// Length [uint32]
	columns[i].length = binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	// Field type [uint8]
	columns[i].fieldType = fieldType(data[pos])
	pos++

	// Flags [uint16]
	columns[i].flags = fieldFlag(binary.LittleEndian.Uint16(data[pos : pos+2]))
	pos += 2

	// Decimals [uint8]
	columns[i].decimals = data[pos]
	//pos++

	//Default value [len coded binary]
	//if pos < len(data) {
	//	defaultVal, _, err = bytesToLengthCodedBinary(data[pos:])
	//}
	//}
	return columns, nil
}

// Reads Packets until EOF-Packet or an Error appears. Returns count of Packets read
func (fsm *MySQLFSM) readUntilEOF() error {

	for {
		res := fsm.load(fsm.pr.packetnum)
		if !res {
			return ErrLoadBuffer
		}
		fsm.pr.packetnum++
		data := fsm.data.Bytes()
		switch data[0] {
		case iERR:
			return fsm.handleErrorPacket(data)
		case iEOF:
			if len(data) == 5 {
				fsm.pr.status = readStatus(data[3:])
			}
			return nil
		}
	}
}

type SqlCompareRes struct {
	Sql     string        `json:"sql"`
	Values  []interface{} `json:"values"`
	ErrCode int           `json:"errcode"`
	ErrDesc string        `json:"errdesc"`
}

//Compare the value of each column in the result set
//* converting the column value to a string
func CompareValue(a driver.Value, b driver.Value) (bool, error) {
	var as string
	err := convertAssignRows(&as, a)
	if err != nil {
		return false, err
	}
	var bs string
	err = convertAssignRows(&bs, b)
	if err != nil {
		return false, err
	}
	if as != bs {
		return false, nil
	}
	return true, nil
}

//Summarize statistics to the global  var
func (fsm *MySQLFSM) AddStatis() {
	Sm.Lock()
	defer Sm.Unlock()
	ExecSqlNum += fsm.execSqlNum
	ExecSuccNum += fsm.execSuccNum
	ExecFailNum += fsm.execFailNum
	PrExecRowCount += fsm.prRowNumCount
	PrExecTimeCount += fsm.prExecTimeCount
	PrExecSuccCount += fsm.prExecSuccCount
	PrExecFailCount += fsm.prExecFailCount
	RrExecRowCount += fsm.rrRowNumCount
	RrExecTimeCount += fsm.rrExecTimeCount
	RrExecFailCount += fsm.rrExecFailCount
	RrExecSuccCount += fsm.rrExecSuccCount
	ExecTimeNotEqual += fsm.execTimeNotEqual
	RowCountNotequal += fsm.rowCountNotequal
	RowDetailNotEqual += fsm.rowDetailNotEqual
	ExecErrNoNotEqual += fsm.execErrNoNotEqual
	if PrMaxExecTime < fsm.prMaxExecTime {
		PrMaxExecTime = fsm.prMaxExecTime
	}
	if PrMinExecTime > fsm.prMinExecTime {
		PrMinExecTime = fsm.prMinExecTime
	}
	if RrMaxExecTime < fsm.rrMaxExecTime {
		RrMaxExecTime = fsm.rrMaxExecTime
	}
	if RrMinExecTime > fsm.rrMinExecTime {
		RrMinExecTime = fsm.rrMinExecTime
	}

	PrExecTimeIn10ms += fsm.prExecTimeIn10ms
	PrExecTimeIn20ms += fsm.prExecTimeIn20ms
	PrExecTimeIn30ms += fsm.prExecTimeIn30ms
	PrExecTimeIn40ms += fsm.prExecTimeIn40ms
	PrExecTimeIn50ms += fsm.prExecTimeIn50ms
	PrExecTimeIn100ms += fsm.prExecTimeIn100ms
	PrExecTimeOut100ms += fsm.prExecTimeOut100ms
	RrExecTimeIn10ms += fsm.rrExecTimeIn10ms
	RrExecTimeIn20ms += fsm.rrExecTimeIn20ms
	RrExecTimeIn30ms += fsm.rrExecTimeIn30ms
	RrExecTimeIn40ms += fsm.rrExecTimeIn40ms
	RrExecTimeIn50ms += fsm.rrExecTimeIn50ms
	RrExecTimeIn100ms += fsm.rrExecTimeIn100ms
	RrExecTimeOut100ms += fsm.rrExecTimeOut100ms

	fsm.prExecTimeIn10ms = 0
	fsm.prExecTimeIn20ms = 0
	fsm.prExecTimeIn30ms = 0
	fsm.prExecTimeIn40ms = 0
	fsm.prExecTimeIn50ms = 0
	fsm.prExecTimeIn100ms = 0
	fsm.prExecTimeOut100ms = 0
	fsm.rrExecTimeIn10ms = 0
	fsm.rrExecTimeIn20ms = 0
	fsm.rrExecTimeIn30ms = 0
	fsm.rrExecTimeIn40ms = 0
	fsm.rrExecTimeIn50ms = 0
	fsm.rrExecTimeIn100ms = 0
	fsm.rrExecTimeOut100ms = 0

	fsm.prRowNumCount = 0
	fsm.prExecTimeCount = 0
	fsm.rrRowNumCount = 0
	fsm.rrExecTimeCount = 0
	fsm.execSqlNum = 0
	fsm.execSuccNum = 0
	fsm.execFailNum = 0
	fsm.prExecFailCount = 0
	fsm.prExecSuccCount = 0
	fsm.rrExecFailCount = 0
	fsm.rrExecSuccCount = 0
	fsm.execErrNoNotEqual = 0
	fsm.execTimeNotEqual = 0
	fsm.rowCountNotequal = 0
	fsm.rowDetailNotEqual = 0
	fsm.prMaxExecTime = 0
	fsm.prMinExecTime = 10000000
	fsm.rrMaxExecTime = 0
	fsm.rrMinExecTime = 10000000
}

//type 0 from packet
//type 1 from replay server
func (fsm *MySQLFSM) setBucketNum(execTime uint64, serverType int8) {
	execTimeMS := execTime / uint64(time.Millisecond)
	switch true {
	case execTimeMS < 10:
		if serverType == 0 {
			fsm.prExecTimeIn10ms++
		} else {
			fsm.rrExecTimeIn10ms++
		}
	case execTimeMS >= 10 && execTimeMS < 20:
		if serverType == 0 {
			fsm.prExecTimeIn20ms++
		} else {
			fsm.rrExecTimeIn20ms++
		}
	case execTimeMS >= 20 && execTimeMS < 30:
		if serverType == 0 {
			fsm.prExecTimeIn30ms++
		} else {
			fsm.rrExecTimeIn30ms++
		}
	case execTimeMS >= 30 && execTimeMS < 40:
		if serverType == 0 {
			fsm.prExecTimeIn40ms++
		} else {
			fsm.rrExecTimeIn40ms++
		}
	case execTimeMS >= 40 && execTimeMS < 50:
		if serverType == 0 {
			fsm.prExecTimeIn50ms++
		} else {
			fsm.rrExecTimeIn50ms++
		}
	case execTimeMS >= 50 && execTimeMS < 100:
		if serverType == 0 {
			fsm.prExecTimeIn100ms++
		} else {
			fsm.rrExecTimeIn100ms++
		}
	default:
		if serverType == 0 {
			fsm.prExecTimeOut100ms++
		} else {
			fsm.rrExecTimeOut100ms++
		}

	}

}

//compare result from packet and result from tidb server
// errcode 1: errcode not equal
// errcode 2: exec time difference is doubled
// errcode 3: result rownum is not equal
// errcode 4: row detail is not equal
func (fsm *MySQLFSM) CompareRes(rr *ReplayRes) *SqlCompareRes {

	res := new(SqlCompareRes)
	pr := fsm.pr
	res.Sql = rr.SqlStatment
	res.Values = rr.Values
	fsm.execSqlNum++
	var prSqlExecTime uint64
	if pr.sqlBeginTime < pr.sqlEndTime {
		prSqlExecTime = pr.sqlEndTime - pr.sqlBeginTime
	} else {
		prSqlExecTime = 0
	}
	fsm.prExecTimeCount += uint64(prSqlExecTime)

	var rrSqlExecTime uint64
	if rr.SqlBeginTime < rr.SqlEndTime {
		rrSqlExecTime = rr.SqlEndTime - rr.SqlBeginTime
	} else {
		rrSqlExecTime = 0
	}
	fsm.rrExecTimeCount += uint64(rrSqlExecTime)
	fsm.prExecTimeCount += uint64(prSqlExecTime)
	fsm.setBucketNum(rrSqlExecTime, 1)
	fsm.setBucketNum(prSqlExecTime, 0)
	if fsm.prMaxExecTime < uint64(prSqlExecTime) {
		fsm.prMaxExecTime = uint64(prSqlExecTime)
	}
	if fsm.prMinExecTime > uint64(prSqlExecTime) {
		fsm.prMinExecTime = uint64(prSqlExecTime)
	}

	if fsm.rrMaxExecTime < uint64(rrSqlExecTime) {
		fsm.rrMaxExecTime = uint64(rrSqlExecTime)
	}
	if fsm.rrMinExecTime > uint64(rrSqlExecTime) {
		fsm.rrMinExecTime = uint64(rrSqlExecTime)
	}
	var prlen int = 0
	var rrlen int = 0
	if pr.bRows != nil {
		prlen = len(pr.bRows.rs.columnValue)
	} else if pr.tRows != nil {
		prlen = len(pr.tRows.rs.columnValue)
	}
	rrlen = len(rr.ColValues)
	fsm.prRowNumCount += uint64(prlen)
	fsm.rrRowNumCount += uint64(rrlen)

	if fsm.execSqlNum/10 == 0 {
		defer fsm.AddStatis()
	}

	if rr.ErrNO != 0 {
		fsm.rrExecFailCount++
	} else {
		fsm.rrExecSuccCount++
	}

	if pr.errNo != 0 {
		fsm.prExecFailCount++
	} else {
		fsm.prExecSuccCount++
	}
	//compare errcode
	if rr.ErrNO != pr.errNo {
		res.ErrCode = 1
		res.ErrDesc = fmt.Sprintf("%v-%v", pr.errNo, rr.ErrNO)
		fsm.execErrNoNotEqual++
		fsm.execFailNum++
		return res
	}

	//compare exec time

	if rrSqlExecTime > (10 * prSqlExecTime) {
		//From http://en.wikipedia.org/wiki/Order_of_magnitude: "We say two
		//numbers have the same order of magnitude of a number if the big
		//one divided by the little one is less than 10. For example, 23 and
		//82 have the same order of magnitude, but 23 and 820 do not."
		res.ErrCode = 2
		res.ErrDesc = fmt.Sprintf("%v us-%v us",
			prSqlExecTime,
			rrSqlExecTime)
		fsm.execFailNum++
		fsm.execTimeNotEqual++
		return res
	}

	//compare  result row num

	if prlen != rrlen {
		res.ErrCode = 3
		res.ErrDesc = fmt.Sprintf("%v-%v", prlen, rrlen)
		fsm.execFailNum++
		fsm.rowCountNotequal++
		return res
	}

	//compare result row detail
	var prrows [][]driver.Value
	var rrrows [][]driver.Value
	if pr.tRows == nil {
		prrows = pr.bRows.rs.columnValue
	} else {
		prrows = pr.tRows.rs.columnValue
	}
	rrrows = rr.ColValues
	i := len(prrows)
	for j := 0; j < i; j++ {
		if len(rrrows[j]) != len(prrows[j]) {
			res.ErrCode = 4
			fsm.execFailNum++
			fsm.rowDetailNotEqual++
			return res
		}
		for k := 0; k < len(rrrows[j]); k++ {
			r, e := CompareValue(rrrows[j][k], prrows[j][k])
			if e != nil || !r {
				res.ErrCode = 4
				if e != nil {
					res.ErrDesc = "data is considered inconsistent because the" +
						" value failed to be resolved ," + e.Error()
					fsm.log.Warn(res.ErrDesc)
				}
				fsm.execFailNum++
				fsm.rowDetailNotEqual++
				return res
			}
		}
	}

	res.ErrCode = 0
	fsm.execSuccNum++

	return res
}

type SqlCompareExecTimeRes struct {
	Sql     string `json:"sql"`
	ErrCode int    `json:"errcode"`
	ErrDesc string `json:"errdesc"`
}

//compare result from packet and result from tidb server
// errcode 1: errcode not equal and not dead lock or lock wait time out or duplicate key
// errcode 2: exec time difference is doubled
func (fsm *MySQLFSM) CompareExecTime(rr *ReplayRes) *SqlCompareExecTimeRes {

	var prSqlExecTime uint64
	var rrSqlExecTime uint64
	res := new(SqlCompareExecTimeRes)
	pr := fsm.pr
	res.Sql = rr.SqlStatment
	fsm.execSqlNum++
	if pr.sqlEndTime <= pr.sqlBeginTime {
		prSqlExecTime = 0
	} else {
		prSqlExecTime = pr.sqlEndTime - pr.sqlBeginTime
	}
	fsm.prExecTimeCount += uint64(prSqlExecTime)
	if fsm.prMaxExecTime < uint64(prSqlExecTime) {
		fsm.prMaxExecTime = uint64(prSqlExecTime)
	}
	if fsm.prMinExecTime > uint64(prSqlExecTime) {
		fsm.prMinExecTime = uint64(prSqlExecTime)
	}
	if rr.SqlEndTime <= rr.SqlBeginTime {
		rrSqlExecTime = 0
	} else {
		rrSqlExecTime = rr.SqlEndTime - rr.SqlBeginTime
	}
	fsm.rrExecTimeCount += uint64(rrSqlExecTime)
	if fsm.rrMaxExecTime < uint64(rrSqlExecTime) {
		fsm.rrMaxExecTime = uint64(rrSqlExecTime)
	}
	if fsm.rrMinExecTime > uint64(rrSqlExecTime) {
		fsm.rrMinExecTime = uint64(rrSqlExecTime)
	}

	fsm.setBucketNum(rrSqlExecTime, 1)
	fsm.setBucketNum(prSqlExecTime, 0)

	if rr.ErrNO != 0 {
		fsm.rrExecFailCount++
		//fmt.Println(res.Sql, rr.ErrNO)
	} else {
		fsm.rrExecSuccCount++
	}

	if pr.errNo != 0 {
		fsm.prExecFailCount++
	} else {
		fsm.prExecSuccCount++
	}

	if fsm.execSqlNum/10 == 0 {
		defer fsm.AddStatis()
	}

	//fmt.Println(rr.ErrNO, pr.errNo, rr.SqlStatment)

	//compare errcode
	if rr.ErrNO != pr.errNo {
		//ignore errcodes list
		//1032:HA_ERR_KEY_NOT_FOUND
		//1062:ER_DUP_KEY
		//1025:ER_LOCK_WAIT_TIMEOUT
		//1213:ER_LOCK_DEADLOCK
		if rr.ErrNO != 1032 && rr.ErrNO != 1062 &&
			rr.ErrNO != 1025 && rr.ErrNO != 1213 {
			res.ErrCode = 1
			res.ErrDesc = fmt.Sprintf("%v-%v", pr.errNo, rr.ErrNO)
			fsm.execErrNoNotEqual++
			fsm.execFailNum++
			return res
		} else {
			fsm.log.Info(fmt.Sprintf("%s:%v-%v", res.Sql, pr.errNo, rr.ErrNO))
		}
	}

	//compare exec time

	if rrSqlExecTime > (5 * prSqlExecTime) {
		//From http://en.wikipedia.org/wiki/Order_of_magnitude: "We say two
		//numbers have the same order of magnitude of a number if the big
		//one divided by the little one is less than 10. For example, 23 and
		//82 have the same order of magnitude, but 23 and 820 do not."
		res.ErrCode = 2
		res.ErrDesc = fmt.Sprintf("%v us-%v us",
			prSqlExecTime,
			rrSqlExecTime)
		fsm.execFailNum++
		fsm.execTimeNotEqual++
		return res
	}

	res.ErrCode = 0
	fsm.execSuccNum++

	return res
}
