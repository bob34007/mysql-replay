package stream

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"github.com/agiledragon/gomonkey"
	"github.com/bobguo/mysql-replay/util"

	//"github.com/gobwas/glob/syntax/ast"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var logger *zap.Logger

func init() {
	cfg := zap.NewDevelopmentConfig()
	//cfg.Level = zap.NewAtomicLevelAt()
	cfg.DisableStacktrace = !cfg.Level.Enabled(zap.DebugLevel)
	logger, _ = cfg.Build()
	zap.ReplaceGlobals(logger)
	logger = zap.L().With(zap.String("conn", "test-mysql.go"))
	logger = logger.Named("test")
}





func TestMysql_InitValue(t *testing.T) {
	fsm := new(MySQLFSM)
	fsm.InitValue()
	ast := assert.New(t)
	ast.Equal(len(fsm.packets), 0)
}

func TestMysql_Handle_StateComQuit(t *testing.T) {
	fsm := new(MySQLFSM)
	fsm.state = util.StateComQuit
	pkt := new(MySQLPacket)
	fsm.Handle(*pkt)
}

func TestMysql_State(t *testing.T) {
	fsm := new(MySQLFSM)
	fsm.state = util.StateComQuit
	i := fsm.State()
	ast := assert.New(t)
	ast.Equal(i, util.StateComQuit)
}

func TestMysql_Query(t *testing.T) {
	fsm := new(MySQLFSM)
	query := "select * from test"
	fsm.query = query
	q := fsm.Query()
	ast := assert.New(t)
	ast.Equal(query, q)
}

func TestMysql_Stmt(t *testing.T) {
	fsm := new(MySQLFSM)
	stmt := new(Stmt)
	stmt.ID = 100
	fsm.stmt = *stmt
	s := fsm.Stmt()

	ast := assert.New(t)
	ast.Equal(s.ID, stmt.ID)
}

func TestMysql_Stmts(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.stmts = make(map[uint32]Stmt)

	stmt := new(Stmt)
	stmt.ID = 100
	fsm.stmts[stmt.ID] = *stmt

	stmt1 := new(Stmt)
	stmt1.ID = 99
	fsm.stmts[stmt1.ID] = *stmt1

	s := fsm.Stmts()

	ast := assert.New(t)
	ast.Equal(len(s), 2)
}

func TestMysql_StmtParams(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.params = make([]interface{}, 0)
	fsm.params = append(fsm.params, "abc")
	fsm.params = append(fsm.params, "def")

	p := fsm.StmtParams()
	ast := assert.New(t)
	ast.Equal(len(p), 2)
}

func TestMysql_Schema(t *testing.T) {
	fsm := new(MySQLFSM)

	db := "test"
	fsm.schema = db

	schema := fsm.Schema()
	ast := assert.New(t)
	ast.Equal(schema, db)
}

func TestMysql_Changed(t *testing.T) {
	fsm := new(MySQLFSM)

	b := false
	fsm.changed = b

	changed := fsm.Changed()
	ast := assert.New(t)
	ast.Equal(changed, b)
}

func TestMysql_Ready(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.packets = make([]MySQLPacket, 0)

	packet := new(MySQLPacket)
	packet.Time = time.Now()
	packet.Len = 7
	packet.Seq = 0
	packet.Data = []byte("05abcde")

	fsm.packets = append(fsm.packets, *packet)

	b := true

	changed := fsm.Ready()
	ast := assert.New(t)
	ast.Equal(changed, b)
}

func TestMysql_nextSeq(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.packets = make([]MySQLPacket, 0)

	packet := new(MySQLPacket)
	packet.Time = time.Now()
	packet.Len = 7
	packet.Seq = 0
	packet.Data = []byte("05abcde")

	fsm.packets = append(fsm.packets, *packet)

	seq := fsm.nextSeq()
	ast := assert.New(t)
	ast.Equal(seq, 1)
}

func TestMysql_load(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.data = new(bytes.Buffer)

	fsm.packets = make([]MySQLPacket, 0)

	packet := new(MySQLPacket)
	packet.Time = time.Now()
	packet.Len = 7
	packet.Seq = 0
	packet.Data = []byte("05abcde")

	fsm.packets = append(fsm.packets, *packet)

	fsm.load(0)
	data := fsm.data.Bytes()
	ast := assert.New(t)
	ast.Equal(len(data), 7)
}

func TestMysql_setStatusWithNoChange(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.state = util.StateComQuery
	fsm.log = logger
	to := util.StateComQuery1

	fsm.setStatusWithNoChange(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, util.StateComQuery1)
}

func TestMysql_set_changed_false(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.state = util.StateComQuery
	fsm.log = logger
	to := util.StateComQuery1
	fsm.changed = false

	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, util.StateComQuery1)
}

func TestMysql_set_log_nil(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.state = util.StateComQuery
	fsm.log = nil
	to := util.StateComQuery1
	fsm.changed = true

	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, util.StateComQuery1)
}

func TestMysql_set_StateComQuery(t *testing.T) {
	fsm := new(MySQLFSM)

	query := "select * from test"
	fsm.query = query
	fsm.state = util.StateUnknown
	fsm.log = logger
	to := util.StateComQuery
	fsm.changed = true

	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, util.StateComQuery)
}

func TestMysql_set_StateComStmtExecute(t *testing.T) {
	fsm := new(MySQLFSM)

	query := "select * from test where id =?"
	fsm.query = query
	fsm.state = util.StateUnknown
	fsm.log = logger
	to := util.StateComStmtExecute
	fsm.changed = true

	stmt := new(Stmt)
	stmt.ID = 100
	stmt.Query = query
	fsm.stmt = *stmt

	fsm.params = make([]interface{}, 0)
	fsm.params = append(fsm.params, 10)

	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, util.StateComStmtExecute)
}

func TestMysql_set_StateComStmtPrepare0(t *testing.T) {
	fsm := new(MySQLFSM)

	query := "select * from test where id =?"
	fsm.query = query
	fsm.state = util.StateUnknown
	fsm.log = logger
	to := util.StateComStmtPrepare0
	fsm.changed = true

	stmt := new(Stmt)
	stmt.ID = 100
	stmt.Query = query

	fsm.stmt = *stmt

	fsm.params = make([]interface{}, 0)
	fsm.params = append(fsm.params, 10)

	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, util.StateComStmtPrepare0)
}

func TestMysql_set_StateComStmtPrepare1(t *testing.T) {
	fsm := new(MySQLFSM)

	query := "select * from test where id =?"
	fsm.query = query
	fsm.state = util.StateUnknown
	fsm.log = logger
	to := util.StateComStmtPrepare1
	fsm.changed = true

	stmt := new(Stmt)
	stmt.ID = 100
	stmt.Query = query
	stmt.NumParams = 1
	fsm.stmt = *stmt

	fsm.params = make([]interface{}, 0)
	fsm.params = append(fsm.params, 10)

	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, util.StateComStmtPrepare1)
}

func TestMysql_set_StateComStmtClose(t *testing.T) {
	fsm := new(MySQLFSM)

	query := "select * from test where id =?"
	fsm.query = query
	fsm.state = util.StateUnknown
	fsm.log = logger
	to := util.StateComStmtClose
	fsm.changed = true

	stmt := new(Stmt)
	stmt.ID = 100
	stmt.Query = query
	stmt.NumParams = 1
	fsm.stmt = *stmt

	fsm.params = make([]interface{}, 0)
	fsm.params = append(fsm.params, 10)

	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, util.StateComStmtClose)
}

func TestMysql_set_StateHandshake1(t *testing.T) {
	fsm := new(MySQLFSM)
	schema := "test"
	fsm.query = schema
	fsm.state = util.StateUnknown
	fsm.log = logger
	to := util.StateHandshake1
	fsm.changed = true
	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, util.StateHandshake1)
}

func TestMysql_assertDataByte_fail(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.data = new(bytes.Buffer)

	fsm.packets = make([]MySQLPacket, 0)

	packet := new(MySQLPacket)
	packet.Time = time.Now()
	packet.Len = 7
	packet.Seq = 0
	packet.Data = []byte("05abcde")

	fsm.packets = append(fsm.packets, *packet)

	fsm.load(0)
	b := fsm.assertDataByte(10, 'a')

	ast := assert.New(t)
	ast.False(b)
}

func TestMysql_assertDataByte_succ(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.data = new(bytes.Buffer)

	fsm.packets = make([]MySQLPacket, 0)

	packet := new(MySQLPacket)
	packet.Time = time.Now()
	packet.Len = 7
	packet.Seq = 0
	packet.Data = []byte("05abcde")

	fsm.packets = append(fsm.packets, *packet)

	fsm.load(0)
	b := fsm.assertDataByte(5, 'd')

	ast := assert.New(t)
	ast.True(b)
}

func TestMysql_handleComQueryNoLoad(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.data = new(bytes.Buffer)

	fsm.packets = make([]MySQLPacket, 0)

	packet := new(MySQLPacket)
	packet.Time = time.Now()
	packet.Len = 7
	packet.Seq = 0
	packet.Data = []byte("3select * from test")
	query := "select * from test"

	fsm.packets = append(fsm.packets, *packet)

	fsm.load(0)
	fsm.handleComQueryNoLoad()

	ast := assert.New(t)
	ast.Equal(fsm.query, query)
	ast.Equal(fsm.state, util.StateComQuery)
}

func TestMysql_IsSelectStmtOrSelectPrepare_succ(t *testing.T) {
	query := " select * from test"
	fsm := new(MySQLFSM)
	fsm.log = logger
	b := fsm.IsSelectStmtOrSelectPrepare(query)

	ast := assert.New(t)

	ast.True(b)

}

func TestMysql_IsSelectStmtOrSelectPrepare_fail(t *testing.T) {
	query := " selet * from test"
	fsm := new(MySQLFSM)
	fsm.log = logger
	b := fsm.IsSelectStmtOrSelectPrepare(query)

	ast := assert.New(t)

	ast.False(b)

}

func TestMysql_handleComStmtCloseNoLoad_succ(t *testing.T) {
	fsm := new(MySQLFSM)
	fsm.data = new(bytes.Buffer)
	fsm.packets = make([]MySQLPacket, 0)
	packet := new(MySQLPacket)
	fsm.stmts = make(map[uint32]Stmt, 0)

	packet.Time = time.Now()
	packet.Len = 5
	packet.Seq = 0

	packet.Data = make([]byte, 0)
	packet.Data = append(packet.Data, uint8(4))
	stmtID := uint32(1000)
	h := make([]byte, 4)
	binary.LittleEndian.PutUint32(h, stmtID)
	packet.Data = append(packet.Data, h...)

	fsm.packets = append(fsm.packets, *packet)

	fsm.load(0)

	fsm.handleComStmtCloseNoLoad()

}

func TestMysql_handleComStmtCloseNoLoad_fail(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.log = logger

	fsm.data = new(bytes.Buffer)
	fsm.packets = make([]MySQLPacket, 0)
	packet := new(MySQLPacket)
	fsm.stmts = make(map[uint32]Stmt, 0)

	packet.Time = time.Now()
	packet.Len = 5
	packet.Seq = 0

	packet.Data = make([]byte, 0)
	packet.Data = append(packet.Data, uint8(4))
	//stmtID := uint32(1000)
	h := make([]byte, 0)
	h = append(h, 'a', 'b', 'c', 'd')
	packet.Data = append(packet.Data, h...)

	fsm.packets = append(fsm.packets, *packet)

	fsm.load(0)

	patch1 := gomonkey.ApplyFunc(readUint32, func(data []byte) (uint32, []byte, bool) {
		return 0, nil, false
	})
	defer patch1.Reset()

	fsm.handleComStmtCloseNoLoad()

}

func TestMysql_parseBinaryDate(t *testing.T) {

	h := uint16(2021)
	a := make([]byte, 2)

	binary.LittleEndian.PutUint16(a[0:], h)

	a = append(a, uint8(10))

	a = append(a, uint8(8))

	pos, s := parseBinaryDate(0, a)

	ast := assert.New(t)
	ast.Equal(pos, 4)
	ast.Equal(s, "2021-10-08")
}

func TestMysql_parseBinaryDateTimeReply(t *testing.T) {

	h := uint16(2021)
	a := make([]byte, 2)

	binary.LittleEndian.PutUint16(a[0:], h)

	a = append(a, uint8(10))

	a = append(a, uint8(8))

	a = append(a, uint8(15))
	a = append(a, uint8(28))
	a = append(a, uint8(20))

	pos, s := parseBinaryDateTimeReply(0, a)

	ast := assert.New(t)
	ast.Equal(pos, 7)
	ast.Equal(s, "2021-10-08 15:28:20")
}

func TestMysql_parseBinaryTimestamp(t *testing.T) {

	h := uint16(2021)
	a := make([]byte, 2)

	binary.LittleEndian.PutUint16(a[0:], h)

	a = append(a, uint8(10))

	a = append(a, uint8(8))

	a = append(a, uint8(15))
	a = append(a, uint8(28))
	a = append(a, uint8(20))
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(20000000))

	a = append(a, b...)

	pos, s := parseBinaryTimestamp(0, a)

	ast := assert.New(t)
	ast.Equal(pos, 11)
	ast.Equal(s, "2021-10-08 15:28:20.20000000")
}

func TestMysql_parseBinaryTime_isNegative_1(t *testing.T) {

	h := uint32(2021)
	a := make([]byte, 4)

	binary.LittleEndian.PutUint32(a[0:], h)

	a = append(a, uint8(15))
	a = append(a, uint8(28))
	a = append(a, uint8(20))

	pos, date := parseBinaryTime(0, a, 1)

	ast := assert.New(t)
	ast.Equal(pos, 7)
	ast.Equal(date, "-2021 15:28:20")
}

func TestMysql_parseBinaryTime_isNegative_0(t *testing.T) {

	h := uint32(2021)
	a := make([]byte, 4)

	binary.LittleEndian.PutUint32(a[0:], h)

	a = append(a, uint8(15))
	a = append(a, uint8(28))
	a = append(a, uint8(20))

	pos, date := parseBinaryTime(0, a, 0)

	ast := assert.New(t)
	ast.Equal(pos, 7)
	ast.Equal(date, "2021 15:28:20")
}

func TestMysql_parseBinaryTimeWithMS_isNegative_1(t *testing.T) {

	a := make([]byte, 4)

	binary.LittleEndian.PutUint32(a[0:], uint32(2021))

	a = append(a, uint8(15))
	a = append(a, uint8(28))
	a = append(a, uint8(20))

	//fmt.Println(a)

	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b[0:], uint32(2000000))

	a = append(a, b...)

	pos, date := parseBinaryTimeWithMS(0, a, 1)

	ast := assert.New(t)
	ast.Equal(pos, 11)
	ast.Equal(date, "-2021 15:28:20.2000000")
}

func TestMysql_parseBinaryTimeWithMS_isNegative_0(t *testing.T) {

	a := make([]byte, 4)

	binary.LittleEndian.PutUint32(a[0:], uint32(2021))

	a = append(a, uint8(15))
	a = append(a, uint8(28))
	a = append(a, uint8(20))

	//fmt.Println(a)

	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b[0:], uint32(2000000))

	a = append(a, b...)

	pos, date := parseBinaryTimeWithMS(0, a, 0)

	ast := assert.New(t)
	ast.Equal(pos, 11)
	ast.Equal(date, "2021 15:28:20.2000000")
}

func TestMysql_parseLengthEncodedInt_0xfb(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfb)
	num, isNull, n := parseLengthEncodedInt(b[0:])

	ast := assert.New(t)
	ast.Equal(num, uint64(0))
	ast.True(isNull)
	ast.Equal(n, 1)
}

func TestMysql_parseLengthEncodedInt_0xfc(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfc)
	b = append(b, 10)
	b = append(b, 11)
	num, isNull, n := parseLengthEncodedInt(b[0:])

	ast := assert.New(t)
	ast.Equal(num, uint64(2826))
	ast.False(isNull)
	ast.Equal(n, 3)
}

func TestMysql_parseLengthEncodedInt_0xfd(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfd)
	b = append(b, 10)
	b = append(b, 11)
	b = append(b, 12)
	num, isNull, n := parseLengthEncodedInt(b[0:])

	//fmt.Println(num,isNull,n)

	ast := assert.New(t)
	ast.Equal(num, uint64(789258))
	ast.False(isNull)
	ast.Equal(n, 4)
}

func TestMysql_parseLengthEncodedInt_0xfe(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfe)
	b = append(b, 10)
	b = append(b, 11)
	b = append(b, 12)
	b = append(b, 13)
	b = append(b, 14)
	b = append(b, 15)
	b = append(b, 16)
	b = append(b, 17)
	num, isNull, n := parseLengthEncodedInt(b[0:])

	//fmt.Println(num,isNull,n)

	ast := assert.New(t)
	ast.Equal(num, uint64(1229499251294997258))
	ast.False(isNull)
	ast.Equal(n, 9)
}

func TestMysql_parseLengthEncodedInt_0xfa(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfa)

	num, isNull, n := parseLengthEncodedInt(b[0:])

	ast := assert.New(t)
	ast.Equal(num, uint64(250))
	ast.False(isNull)
	ast.Equal(n, 1)
}

func TestMysql_readUint16_false(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfa)

	num, _, a := readUint16(b[0:])

	ast := assert.New(t)
	ast.Equal(num, uint16(0))
	ast.False(a)

}

func TestMysql_readUint16_true(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfa)
	b = append(b, 0xff)
	num, _, a := readUint16(b[0:])

	//fmt.Println(num,a)

	ast := assert.New(t)
	ast.Equal(num, uint16(65530))
	ast.True(a)

}

func TestMysql_readUint32_false(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfa)

	num, _, a := readUint32(b[0:])

	ast := assert.New(t)
	ast.Equal(num, uint32(0))
	ast.False(a)

}

func TestMysql_readUint32_true(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfa)
	b = append(b, 0xfa)
	b = append(b, 0xfa)
	b = append(b, 0xfa)

	num, _, a := readUint32(b[0:])

	//fmt.Println(num,a)

	ast := assert.New(t)
	ast.Equal(num, uint32(4210752250))
	ast.True(a)

}

func TestMysql_readBytesN_false(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfa)

	a, _, c := readBytesN(b, 7)
	ast := assert.New(t)
	ast.Nil(a)
	ast.False(c)

}

func TestMysql_readBytesN_true(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfa)
	b = append(b, 0xfa)

	a, _, c := readBytesN(b, 1)
	ast := assert.New(t)
	ast.NotNil(a)
	ast.True(c)

}

func TestMysql_readBytesNUL_true(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfa)
	b = append(b, 0x00)
	b = append(b, 0xfa)

	a, d, c := readBytesNUL(b)
	ast := assert.New(t)
	ast.NotNil(a)
	ast.True(c)
	ast.NotNil(d)
}

func TestMysql_readBytesNUL_false(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfa)
	b = append(b, 0xfa)

	a, d, c := readBytesNUL(b)
	ast := assert.New(t)
	ast.Nil(a)
	ast.False(c)
	ast.NotNil(d)
}

func TestMysql_readLenEncUint_Len_0(t *testing.T) {

	b := make([]byte, 0)

	c, _, e := readLenEncUint(b)

	ast := assert.New(t)
	ast.Equal(c, uint64(0))
	ast.False(e)
}

func TestMysql_readLenEncUint_Len_1_0xfa(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfa)

	c, _, e := readLenEncUint(b)

	ast := assert.New(t)
	ast.Equal(c, uint64(250))
	ast.True(e)
}

func TestMysql_readLenEncUint_Len_1_0xfb(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfb)

	c, _, e := readLenEncUint(b)

	ast := assert.New(t)
	ast.Equal(c, uint64(0))
	ast.False(e)
}

func TestMysql_readLenEncUint_Len_1_0xfc(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfc)

	c, _, e := readLenEncUint(b)

	ast := assert.New(t)
	ast.Equal(c, uint64(0))
	ast.False(e)
}

func TestMysql_readLenEncUint_Len_3_0xfc(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfc)
	b = append(b, 0xff)
	b = append(b, 0xff)

	c, _, e := readLenEncUint(b)

	ast := assert.New(t)
	ast.Equal(c, uint64(65535))
	ast.True(e)
}

func TestMysql_readLenEncUint_Len_1_0xfd(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfd)

	c, _, e := readLenEncUint(b)

	ast := assert.New(t)
	ast.Equal(c, uint64(0))
	ast.False(e)
}

func TestMysql_readLenEncUint_Len_4_0xfd(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfd)
	b = append(b, 0xff)
	b = append(b, 0xff)
	b = append(b, 0xff)

	c, _, e := readLenEncUint(b)

	//fmt.Println(c, e)

	ast := assert.New(t)
	ast.Equal(c, uint64(16777215))
	ast.True(e)
}

func TestMysql_readLenEncUint_Len_1_0xfe(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfe)

	c, _, e := readLenEncUint(b)

	ast := assert.New(t)
	ast.Equal(c, uint64(0))
	ast.False(e)
}

func TestMysql_readLenEncUint_Len_9_0xfe(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0xfe)
	b = append(b, 0xff)
	b = append(b, 0xff)
	b = append(b, 0xff)
	b = append(b, 0xff)
	b = append(b, 0xff)
	b = append(b, 0xff)
	b = append(b, 0xff)
	b = append(b, 0xff)
	b = append(b, 0xff)

	c, _, e := readLenEncUint(b)
	//fmt.Println(c, e)
	ast := assert.New(t)
	ast.Equal(c, uint64(18446744073709551615))
	ast.True(e)
}

func TestMysql_readStatus(t *testing.T) {

	b := make([]byte, 0)
	b = append(b, 0x01)
	b = append(b, 0x00)

	c := readStatus(b)
	ast := assert.New(t)
	ast.Equal(c, statusFlag(1))
}

func TestStateName(t *testing.T) {
	type args struct {
		state int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name:"StateInit",
			args:args{
				state:util.StateInit,
			},
			want:"Init",
		},
		{
			name:"StateUnknown",
			args:args{
				state:util.StateUnknown,
			},
			want:"Unknown",
		},
		{
			name:"StateComQuery",
			args:args{
				state:util.StateComQuery,
			},
			want:"ComQuery",
		},
		{
			name:"StateComQuery1",
			args:args{
				state:util.StateComQuery1,
			},
			want:"ReadingComQueryRes",
		},
		{
			name:"StateComQuery2",
			args:args{
				state:util.StateComQuery2,
			},
			want:"ReadComQueryResEnd",
		},
		{
			name:"StateComStmtExecute",
			args:args{
				state:util.StateComStmtExecute,
			},
			want:"ComStmtExecute",
		},
		{
			name:"StateComStmtExecute1",
			args:args{
				state:util.StateComStmtExecute1,
			},
			want:"ReadingComStmtExecuteRes",
		},
		{
			name:"StateComStmtExecute2",
			args:args{
				state:util.StateComStmtExecute2,
			},
			want:"ReadingComStmtExecuteEnd",
		},
		{
			name:"StateComStmtClose",
			args:args{
				state:util.StateComStmtClose,
			},
			want:"ComStmtClose",
		},
		{
			name:"StateComStmtPrepare0",
			args:args{
				state:util.StateComStmtPrepare0,
			},
			want:"ComStmtPrepare0",
		},
		{
			name:"StateComStmtPrepare1",
			args:args{
				state:util.StateComStmtPrepare1,
			},
			want:"ComStmtPrepare1",
		},
		{
			name:"StateComQuit",
			args:args{
				state:util.StateComQuit,
			},
			want:"ComQuit",
		},
		{
			name:"StateHandshake0",
			args:args{
				state:util.StateHandshake0,
			},
			want:"Handshake0",
		},
		{
			name:"StateHandshake1",
			args:args{
				state:util.StateHandshake1,
			},
			want:"Handshake1",
		},
		{
			name:"StateSkipPacket",
			args:args{
				state:util.StateSkipPacket,
			},
			want:"StateSkipPacket",
		},
		{
			name:"UNKnown",
			args:args{
				state:100,
			},
			want:"Invalid",
		},
	}
		for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StateName(tt.args.state); got != tt.want {
				t.Errorf("StateName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPacketRes_GetSqlBeginTime (t *testing.T){
	pr:=new(PacketRes)
	ts:=uint64(time.Now().Unix())
	pr.sqlBeginTime = ts
	ts1:=pr.GetSqlBeginTime()
	assert.New(t).Equal(ts,ts1)
}

func TestPacketRes_GetSqlEndTime (t *testing.T) {
	pr:=new(PacketRes)
	ts:=uint64(time.Now().Unix())
	pr.sqlEndTime = ts
	ts1:=pr.GetSqlEndTime()
	assert.New(t).Equal(ts,ts1)
}

func TestPacketRes_GetErrNo (t *testing.T) {
	pr:=new(PacketRes)
	pr.errNo=1062
	errno:=pr.GetErrNo()
	assert.New(t).Equal(errno,uint16(1062))
}

func TestPacketRes_GetErrDesc (t *testing.T) {
	pr:=new(PacketRes)
	pr.errDesc="lock wait timeout "
	errdesc:=pr.GetErrDesc()
	assert.New(t).Equal(errdesc,"lock wait timeout ")
}

func TestPacketRes_GetColumnVal_bRows (t *testing.T) {
	pr:=new(PacketRes)
	pr.bRows = new(binaryRows )
	pr.bRows.rs = resultSet{}
	pr.bRows.rs.columnValue = make([][]driver.Value,0)
	value1:=make([]driver.Value,0)
	value1 = append(value1,"aaaa")
	pr.bRows.rs.columnValue=append(pr.bRows.rs.columnValue,value1)
	vol:=pr.GetColumnVal()
	assert.New(t).NotNil(vol)
}

func TestPacketRes_GetColumnVal_tRows (t *testing.T) {
	pr:=new(PacketRes)
	pr.bRows=nil
	pr.tRows = new(textRows )
	pr.tRows.rs = resultSet{}
	pr.tRows.rs.columnValue = make([][]driver.Value,0)
	value1:=make([]driver.Value,0)
	value1 = append(value1,"aaaa")
	pr.tRows.rs.columnValue=append(pr.tRows.rs.columnValue,value1)
	vol:=pr.GetColumnVal()
	assert.New(t).NotNil(vol)
}

func TestPacketRes_GetColumnVal_nil (t *testing.T) {
	pr:=new(PacketRes)
	pr.bRows=nil
	pr.tRows=nil
	vol:=pr.GetColumnVal()
	assert.New(t).Nil(vol)
}

func TestFSM_NextSeq_len_zero(t *testing.T){
	fsm:=NewMySQLFSM(logger)
	fsm.packets=fsm.packets[0:0]
	n := fsm.nextSeq()
	assert.New(t).Equal(n,0)
}

func TestFSM_NextSeq(t *testing.T){
	fsm:=NewMySQLFSM(logger)
	fsm.packets=append(fsm.packets,MySQLPacket{
		Seq:1,
	})
	n := fsm.nextSeq()
	assert.New(t).Equal(n,2)
}
/*
func TestFSM_isClientCommand_client(t *testing.T){
	fsm :=NewMySQLFSM(logger)
	patches := gomonkey.ApplyMethod(reflect.TypeOf(fsm), "assertDir",
		func(_ *MySQLFSM , exp reassembly.TCPFlowDirection) bool{
			return false
		})
	defer patches.Reset()
	cmd := uint8(0)
	res:=fsm.isClientCommand(cmd)

	assert.New(t).False(res)
}


func TestFSM_isClientCommand_server(t *testing.T){
	fsm :=NewMySQLFSM(logger)
	patches := gomonkey.ApplyMethod(reflect.TypeOf(fsm), "assertDir",
		func(_ *MySQLFSM,exp reassembly.TCPFlowDirection) bool {
			return true
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(fsm), "assertDataByte",
		func(_ *MySQLFSM,offset int, exp byte) bool {
			return true
		})
	defer patches1.Reset()

	cmd := uint8(0)
	res:=fsm.isClientCommand(cmd)

	assert.New(t).True(res)
}


 */
