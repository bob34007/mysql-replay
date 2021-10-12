package stream

import (
	"encoding/json"
	"fmt"
	"github.com/agiledragon/gomonkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestEventCodec(t *testing.T) {
	var (
		buf   = make([]byte, 0, 1024)
		err   error
		n     int
		event MySQLEvent
	)
	for i, tt := range []struct {
		event  MySQLEvent
		expect string
		ok     bool
	}{
		{MySQLEvent{
			Time: 0,
			Type: EventHandshake,
		}, "0\t0\t\"\"", true},
		{MySQLEvent{
			Time: 1,
			Type: EventHandshake,
			DB:   "test",
		}, "1\t0\t\"test\"", true},
		{MySQLEvent{
			Time: 2,
			Type: EventQuit,
		}, "2\t1", true},
		{MySQLEvent{
			Time:  3,
			Type:  EventQuery,
			Query: "select * from t where id = 1",
		}, "3\t2\t\"select * from t where id = 1\"", true},
		{MySQLEvent{
			Time:   4,
			Type:   EventStmtPrepare,
			StmtID: 1,
			Query:  "select * from t where id = ?",
		}, "4\t3\t1\t\"select * from t where id = ?\"", true},
		{MySQLEvent{
			Time:   5,
			Type:   EventStmtExecute,
			StmtID: 1,
			Params: []interface{}{int64(1)},
		}, "5\t4\t1\t[i\t1", true},
		{MySQLEvent{
			Time:   6,
			Type:   EventStmtExecute,
			StmtID: 1,
			Params: []interface{}{},
		}, "6\t4\t1\t[", true},
		{MySQLEvent{
			Time:   7,
			Type:   EventStmtExecute,
			StmtID: 1,
		}, "7\t4\t1\t[", true},
		{MySQLEvent{
			Time:   8,
			Type:   EventStmtClose,
			StmtID: 1,
		}, "8\t5\t1", true},
	} {
		t.Run(t.Name()+strconv.Itoa(i), func(t *testing.T) {
			buf = buf[:0]
			buf, err = AppendEvent(buf, tt.event)
			if tt.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, tt.expect, string(buf), fmt.Sprintf("encode %v", tt.event))
			n, err = ScanEvent(tt.expect, 0, event.Reset(nil))
			require.NoError(t, err)
			require.Equal(t, len(tt.expect), n)
			if len(tt.event.Params) == 0 && !reflect.DeepEqual(tt.event.Params, event.Params) {
				tt.event.Params = event.Params
			}
			require.Equal(t, tt.event, event)
			n, err = ScanEvent(tt.expect+"\t...", 0, event.Reset(nil))
			require.NoError(t, err)
			require.Equal(t, len(tt.expect), n)
		})
	}
}

func TestStmtParamsCodec(t *testing.T) {
	var (
		buf    = make([]byte, 0, 1024)
		err    error
		n      int
		params []interface{}
	)
	for i, tt := range []struct {
		params []interface{}
		expect string
		ok     bool
	}{
		{nil, "[", true},
		{[]interface{}{}, "[", true},
		{[]interface{}{nil}, "[0\tnil", true},
		{[]interface{}{[]byte{}, []byte(nil)}, "[bb\t\"\"\t\"\"", true},
		{[]interface{}{int64(0), int64(-1), uint64(0), uint64(1), uint64(math.MaxInt64) + 1}, "[iiuuu\t0\t-1\t0\t1\t9223372036854775808", true},
		{[]interface{}{float32(0), float32(math.MaxFloat32), float64(0), math.MaxFloat64, math.Pi}, "[ffddd\t0\t3.4028235e+38\t0\t1.7976931348623157e+308\t3.141592653589793", true},
		{[]interface{}{"", "\t", "\n"}, "[sss\t\"\"\t\"\\t\"\t\"\\n\"", true},
	} {
		t.Run(t.Name()+strconv.Itoa(i), func(t *testing.T) {
			buf = buf[:0]
			buf, err = AppendStmtParams(buf, tt.params)
			if tt.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, tt.expect, string(buf), fmt.Sprintf("encode %v", tt.params))
			params, n, err = ScanStmtParams(tt.expect, 0, params[:0])
			require.NoError(t, err)
			require.Equal(t, len(tt.expect), n)
			if len(tt.params) == 0 {
				require.Len(t, params, 0)
			} else {
				for j, param := range tt.params {
					if bs, ok := param.([]byte); ok {
						if len(bs) == 0 {
							require.IsType(t, []byte{}, params[j])
							require.Empty(t, params[j])
						} else {
							require.Equal(t, param, params[j])
						}
					}
				}
			}
			params, n, err = ScanStmtParams(tt.expect+"\t...", 0, params[:0])
			require.NoError(t, err)
			require.Equal(t, len(tt.expect), n)
		})
	}
}

func BenchmarkScanEventQuery(b *testing.B) {
	raw, _ := AppendEvent(make([]byte, 0, 4096), MySQLEvent{
		Time:  time.Now().UnixNano() / int64(time.Millisecond),
		Type:  EventQuery,
		Query: "INSERT INTO sbtest1 (id, k, c, pad) VALUES (0, 4855, '26859969401-32022045049-36802759049-57581620716-25566497596-81077101714-43815129390-50670555126-74015418324-70781354462', '78370658245-88835010182-54392836759-10863319425-91771424474')",
	})
	s := string(raw)
	var event MySQLEvent
	for i := 0; i < b.N; i++ {
		ScanEvent(s, 0, &event)
	}
}

func BenchmarkScanEventQueryJson(b *testing.B) {
	raw, _ := json.Marshal(MySQLEvent{
		Time:  time.Now().UnixNano() / int64(time.Millisecond),
		Type:  EventQuery,
		Query: "INSERT INTO sbtest1 (id, k, c, pad) VALUES (0, 4855, '26859969401-32022045049-36802759049-57581620716-25566497596-81077101714-43815129390-50670555126-74015418324-70781354462', '78370658245-88835010182-54392836759-10863319425-91771424474')",
	})
	var event MySQLEvent
	for i := 0; i < b.N; i++ {
		json.Unmarshal(raw, &event)
	}
}

func BenchmarkScanEventStmtExecute(b *testing.B) {
	raw, _ := AppendEvent(make([]byte, 0, 4096), MySQLEvent{
		Time:   time.Now().UnixNano() / int64(time.Millisecond),
		Type:   EventStmtExecute,
		StmtID: 1,
		Params: []interface{}{
			int64(0),
			int64(4855),
			"26859969401-32022045049-36802759049-57581620716-25566497596-81077101714-43815129390-50670555126-74015418324-70781354462",
			"78370658245-88835010182-54392836759-10863319425-91771424474",
		},
	})
	s := string(raw)
	var (
		event  MySQLEvent
		params = make([]interface{}, 0, 4)
	)
	for i := 0; i < b.N; i++ {
		ScanEvent(s, 0, event.Reset(params[:0]))
	}
}

func BenchmarkScanEventStmtExecuteJson(b *testing.B) {
	raw, _ := json.Marshal(MySQLEvent{
		Time:   time.Now().UnixNano() / int64(time.Millisecond),
		Type:   EventStmtExecute,
		StmtID: 1,
		Params: []interface{}{
			int64(0),
			int64(4855),
			"26859969401-32022045049-36802759049-57581620716-25566497596-81077101714-43815129390-50670555126-74015418324-70781354462",
			"78370658245-88835010182-54392836759-10863319425-91771424474",
		},
	})
	var event MySQLEvent
	for i := 0; i < b.N; i++ {
		json.Unmarshal(raw, &event)
	}
}

func TestStream_ParsePacket_EventQuery(t *testing.T){
	pkt := new(MySQLPacket)
	h:=new(eventHandler)
	h.fsm = new(MySQLFSM)
	pkt.Time= time.Now()

	query := "select * from test.test"
	patches := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Handle",
		func  (_ *MySQLFSM,pkt MySQLPacket) {
			return
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Ready",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Changed",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "State",
		func  (_ *MySQLFSM) int{
			return StateComQuery2
		})
	defer patches3.Reset()

	patches4 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Query",
		func  (_ *MySQLFSM) string{
			return query
		})
	defer patches4.Reset()

	e := h.ParsePacket(*pkt)

	ast := assert.New(t)

	ast.Equal(e.Type ,EventQuery)
	ast.Equal(e.Query,query)

}

func TestStream_ParsePacket_EventStmtExecute(t *testing.T){
	pkt := new(MySQLPacket)
	h:=new(eventHandler)
	h.fsm = new(MySQLFSM)
	pkt.Time= time.Now()
	stmt:=new(Stmt)
	stmt.ID=10

	patches := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Handle",
		func  (_ *MySQLFSM,pkt MySQLPacket) {
			return
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Ready",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Changed",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "State",
		func  (_ *MySQLFSM) int{
			return StateComStmtExecute2
		})
	defer patches3.Reset()

	patches4 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Stmt",
		func  (_ *MySQLFSM) Stmt{
			return *stmt
		})
	defer patches4.Reset()

	patches5 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "StmtParams",
		func  (_ *MySQLFSM)  []interface{}{
			return nil
		})
	defer patches5.Reset()

	e := h.ParsePacket(*pkt)

	ast := assert.New(t)

	ast.Equal(e.Type ,EventStmtExecute)
	ast.Equal(e.StmtID,uint64(stmt.ID))
	ast.Nil(e.Params)

}

func TestStream_ParsePacket_StateComStmtPrepare1(t *testing.T){
	query:="select * from test.test"
	pkt := new(MySQLPacket)
	h:=new(eventHandler)
	h.fsm = new(MySQLFSM)
	pkt.Time= time.Now()
	stmt:=new(Stmt)
	stmt.ID=10
	stmt.Query=query

	patches := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Handle",
		func  (_ *MySQLFSM,pkt MySQLPacket) {
			return
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Ready",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Changed",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "State",
		func  (_ *MySQLFSM) int{
			return StateComStmtPrepare1
		})
	defer patches3.Reset()

	patches4 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Stmt",
		func  (_ *MySQLFSM) Stmt{
			return *stmt
		})
	defer patches4.Reset()

	e := h.ParsePacket(*pkt)

	ast := assert.New(t)

	ast.Equal(e.Type ,EventStmtPrepare)
	ast.Equal(e.StmtID,uint64(stmt.ID))
	ast.Equal(e.Query ,query)

}

func TestStream_ParsePacket_StateComStmtClose(t *testing.T){

	pkt := new(MySQLPacket)
	h:=new(eventHandler)
	h.fsm = new(MySQLFSM)
	pkt.Time= time.Now()
	stmt:=new(Stmt)
	stmt.ID=10


	patches := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Handle",
		func  (_ *MySQLFSM,pkt MySQLPacket) {
			return
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Ready",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Changed",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "State",
		func  (_ *MySQLFSM) int{
			return StateComStmtClose
		})
	defer patches3.Reset()

	patches4 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Stmt",
		func  (_ *MySQLFSM) Stmt{
			return *stmt
		})
	defer patches4.Reset()

	e := h.ParsePacket(*pkt)

	ast := assert.New(t)

	ast.Equal(e.Type ,EventStmtClose)
	ast.Equal(e.StmtID,uint64(stmt.ID))

}

func TestStream_ParsePacket_StateHandshake1(t *testing.T){

	pkt := new(MySQLPacket)
	h:=new(eventHandler)
	h.fsm = new(MySQLFSM)
	pkt.Time= time.Now()
	db:="test"


	patches := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Handle",
		func  (_ *MySQLFSM,pkt MySQLPacket) {
			return
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Ready",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Changed",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "State",
		func  (_ *MySQLFSM) int{
			return StateHandshake1
		})
	defer patches3.Reset()

	patches4 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Schema",
		func  (_ *MySQLFSM) string{
			return db
		})
	defer patches4.Reset()

	e := h.ParsePacket(*pkt)

	ast := assert.New(t)

	ast.Equal(e.Type ,EventHandshake)
	ast.Equal(e.DB,db)

}

func TestStream_ParsePacket_StateComQuit(t *testing.T){

	pkt := new(MySQLPacket)
	h:=new(eventHandler)
	h.fsm = new(MySQLFSM)
	pkt.Time= time.Now()


	patches := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Handle",
		func  (_ *MySQLFSM,pkt MySQLPacket) {
			return
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Ready",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Changed",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "State",
		func  (_ *MySQLFSM) int{
			return StateComQuit
		})
	defer patches3.Reset()


	e := h.ParsePacket(*pkt)

	ast := assert.New(t)

	ast.Equal(e.Type ,EventQuit)


}

func TestStream_ParsePacket_100(t *testing.T){

	pkt := new(MySQLPacket)
	h:=new(eventHandler)
	h.fsm = new(MySQLFSM)
	pkt.Time= time.Now()



	patches := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Handle",
		func  (_ *MySQLFSM,pkt MySQLPacket) {
			return
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Ready",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Changed",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "State",
		func  (_ *MySQLFSM) int{
			return 100
		})
	defer patches3.Reset()


	e := h.ParsePacket(*pkt)

	ast := assert.New(t)

	ast.Nil(e)

}

func TestStream_ParsePacket_NotReady(t *testing.T){

	pkt := new(MySQLPacket)
	h:=new(eventHandler)
	h.fsm = new(MySQLFSM)
	pkt.Time= time.Now()



	patches := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Handle",
		func  (_ *MySQLFSM,pkt MySQLPacket) {
			return
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Ready",
		func  (_ *MySQLFSM) bool{
			return false
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Changed",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches2.Reset()


	e := h.ParsePacket(*pkt)

	ast := assert.New(t)

	ast.Nil(e)

}

func TestStream_ParsePacket_NotChange(t *testing.T){

	pkt := new(MySQLPacket)
	h:=new(eventHandler)
	h.fsm = new(MySQLFSM)
	pkt.Time= time.Now()



	patches := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Handle",
		func  (_ *MySQLFSM,pkt MySQLPacket) {
			return
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Ready",
		func  (_ *MySQLFSM) bool{
			return true
		})
	defer patches1.Reset()

	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(h.fsm), "Changed",
		func  (_ *MySQLFSM) bool{
			return false
		})
	defer patches2.Reset()


	e := h.ParsePacket(*pkt)

	ast := assert.New(t)

	ast.Nil(e)

}

