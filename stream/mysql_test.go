package stream

import (
	"bytes"
	"encoding/binary"

	"github.com/agiledragon/gomonkey"

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
	fsm.state = StateComQuit
	pkt := new(MySQLPacket)
	fsm.Handle(*pkt)
}

func TestMysql_State(t *testing.T) {
	fsm := new(MySQLFSM)
	fsm.state = StateComQuit
	i := fsm.State()
	ast := assert.New(t)
	ast.Equal(i, StateComQuit)
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

	fsm.state = StateComQuery
	fsm.log = logger
	to := StateComQuery1

	fsm.setStatusWithNoChange(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, StateComQuery1)
}

func TestMysql_set_changed_false(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.state = StateComQuery
	fsm.log = logger
	to := StateComQuery1
	fsm.changed = false

	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, StateComQuery1)
}

func TestMysql_set_log_nil(t *testing.T) {
	fsm := new(MySQLFSM)

	fsm.state = StateComQuery
	fsm.log = nil
	to := StateComQuery1
	fsm.changed = true

	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, StateComQuery1)
}

func TestMysql_set_StateComQuery(t *testing.T) {
	fsm := new(MySQLFSM)

	query := "select * from test"
	fsm.query = query
	fsm.state = StateUnknown
	fsm.log = logger
	to := StateComQuery
	fsm.changed = true

	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, StateComQuery)
}

func TestMysql_set_StateComStmtExecute(t *testing.T) {
	fsm := new(MySQLFSM)

	query := "select * from test where id =?"
	fsm.query = query
	fsm.state = StateUnknown
	fsm.log = logger
	to := StateComStmtExecute
	fsm.changed = true

	stmt := new(Stmt)
	stmt.ID = 100
	stmt.Query = query
	fsm.stmt = *stmt

	fsm.params = make([]interface{}, 0)
	fsm.params = append(fsm.params, 10)

	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, StateComStmtExecute)
}

func TestMysql_set_StateComStmtPrepare0(t *testing.T) {
	fsm := new(MySQLFSM)

	query := "select * from test where id =?"
	fsm.query = query
	fsm.state = StateUnknown
	fsm.log = logger
	to := StateComStmtPrepare0
	fsm.changed = true

	stmt := new(Stmt)
	stmt.ID = 100
	stmt.Query = query

	fsm.stmt = *stmt

	fsm.params = make([]interface{}, 0)
	fsm.params = append(fsm.params, 10)

	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, StateComStmtPrepare0)
}

func TestMysql_set_StateComStmtPrepare1(t *testing.T) {
	fsm := new(MySQLFSM)

	query := "select * from test where id =?"
	fsm.query = query
	fsm.state = StateUnknown
	fsm.log = logger
	to := StateComStmtPrepare1
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
	ast.Equal(fsm.state, StateComStmtPrepare1)
}

func TestMysql_set_StateComStmtClose(t *testing.T) {
	fsm := new(MySQLFSM)

	query := "select * from test where id =?"
	fsm.query = query
	fsm.state = StateUnknown
	fsm.log = logger
	to := StateComStmtClose
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
	ast.Equal(fsm.state, StateComStmtClose)
}

func TestMysql_set_StateHandshake1(t *testing.T) {
	fsm := new(MySQLFSM)
	schema := "test"
	fsm.query = schema
	fsm.state = StateUnknown
	fsm.log = logger
	to := StateHandshake1
	fsm.changed = true
	fsm.set(to)

	ast := assert.New(t)
	ast.Equal(fsm.state, StateHandshake1)
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
	ast.Equal(fsm.state, StateComQuery)
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
