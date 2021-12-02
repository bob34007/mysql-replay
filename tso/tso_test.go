package tso

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/agiledragon/gomonkey"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"reflect"
	"testing"
	"time"
)

func TestTSO_GetPhysicalTime(t *testing.T) {
	//add test for GetPhysicalTime

	type fields struct {
		physicalTime time.Time
		logical      uint64
	}

	ts := time.Now()

	tests := []struct {
		name   string
		fields fields
		want   time.Time
	}{
		{
			name: "test GetPhysicalTime",
			fields: fields{
				physicalTime: ts,
				logical:      0,
			},
			want: ts,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tso := &TSO{
				physicalTime: tt.fields.physicalTime,
				logical:      tt.fields.logical,
			}
			if got := tso.GetPhysicalTime(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TSO.GetPhysicalTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTSO_ParseTS(t *testing.T) {
	type fields struct {
		physicalTime time.Time
		logical      uint64
	}
	type args struct {
		ts uint64
	}

	var ullTs uint64 = 427779224058986498

	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "test for ParseTS",
			fields: fields{
				physicalTime: time.Now(),
				logical:      2,
			},
			args: args{
				ts: ullTs,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tso := &TSO{
				physicalTime: tt.fields.physicalTime,
				logical:      tt.fields.logical,
			}
			tso.ParseTS(tt.args.ts)

			var llTsD int64 = 0

			timeTemplate1 := "2006-01-02 15:04:05"
			strTsD := "2021-09-17 11:10:36" //"2021-09-17 11:10:36.309"
			ts, err := time.ParseInLocation(timeTemplate1, strTsD, time.Local)
			if err == nil {
				llTsD = ts.UnixNano() / 1000000000
			} else {
				fmt.Println(err.Error())
			}

			llTsP := tso.physicalTime.UnixNano() / 1000000000
			//fmt.Println(ts,tso.physicalTime,llTsP,llTsD)
			assert.Equal(t, llTsD, llTsP, "the two time string should be the same")
		})
	}
}


//conn QueryContext fail
func TestTSO_GetTSOFromTiDB(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		panic("new mock db fail," + err.Error())
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		panic("new conn fail," + err.Error())
	}
	cfg := zap.NewDevelopmentConfig()
	cfg.DisableStacktrace = !cfg.Level.Enabled(zap.DebugLevel)
	logger, _ := cfg.Build()
	zap.ReplaceGlobals(logger)
	tso := &TSO{
		physicalTime: time.Now(),
		logical:      2,
	}
	defer db.Close()
	defer conn.Close()

	rs := &mysql.MySQLError{
		Number:  1062,
		Message: "get data fail",
	}

	patches := gomonkey.ApplyMethod(reflect.TypeOf(conn), "QueryContext", func(_ *sql.Conn, ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
		return nil, rs
	})
	defer patches.Reset()
	ullTs, err := tso.GetTSOFromTiDB(ctx, conn, logger)

	ast := assert.New(t)
	ast.Equal(ullTs, uint64(0))
	ast.Equal(err.(*mysql.MySQLError).Number, uint16(1062))

}

//row nex fail
func TestTSO_GetTSOFromTiDB1(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		panic("new mock db fail," + err.Error())
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		panic("new conn fail," + err.Error())
	}
	cfg := zap.NewDevelopmentConfig()
	cfg.DisableStacktrace = !cfg.Level.Enabled(zap.DebugLevel)
	logger, _ := cfg.Build()
	zap.ReplaceGlobals(logger)
	tso := &TSO{
		physicalTime: time.Now(),
		logical:      2,
	}

	defer db.Close()
	defer conn.Close()

	rows := new(sql.Rows)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(conn), "QueryContext",
		func(_ *sql.Conn, ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
			return rows, nil
		})
	defer patches.Reset()

	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(rows), "Next",
		func(_ *sql.Rows) bool {
			return false
		})
	defer patches1.Reset()

	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(rows), "Close",
		func(_ *sql.Rows) error {
			return nil
		})
	defer patches3.Reset()

	ullTs, err := tso.GetTSOFromTiDB(ctx, conn, logger)

	ast := assert.New(t)
	ast.Equal(ullTs, uint64(0))
	//ast.Equal(err, nil)

}


//all ok
func TestTSO_GetTSOFromTiDB2(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		panic("new mock db fail," + err.Error())
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		panic("new conn fail," + err.Error())
	}
	cfg := zap.NewDevelopmentConfig()
	cfg.DisableStacktrace = !cfg.Level.Enabled(zap.DebugLevel)
	logger, _ := cfg.Build()
	zap.ReplaceGlobals(logger)
	tso := &TSO{
		physicalTime: time.Now(),
		logical:      2,
	}

	defer db.Close()
	defer conn.Close()

	rows := new(sql.Rows)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(conn), "QueryContext",
		func(_ *sql.Conn, ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
			return rows, nil
		})
	defer patches.Reset()

	var ret bool = false
	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(rows), "Next",
		func(_ *sql.Rows) bool {
			ret = !ret
			return ret
		})
	defer patches1.Reset()


	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(rows), "Scan",
		func(_ *sql.Rows, dest ...interface{}) error {
			return nil
		})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(rows), "Close",
		func(_ *sql.Rows) error {
			return nil
		})
	defer patches3.Reset()


	patch := gomonkey.ApplyFunc(json.Unmarshal, func (data []byte, v interface{}) error{
		return nil
	})
	defer patch.Reset()


	ullTs, err := tso.GetTSOFromTiDB(ctx, conn, logger)

	//fmt.Println(ullTs, err)
	ast:=assert.New(t)
	ast.Equal(ullTs,uint64(0))
}


//json Unmarshal fail
func TestTSO_GetTSOFromTiDB3(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		panic("new mock db fail," + err.Error())
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		panic("new conn fail," + err.Error())
	}
	cfg := zap.NewDevelopmentConfig()
	cfg.DisableStacktrace = !cfg.Level.Enabled(zap.DebugLevel)
	logger, _ := cfg.Build()
	zap.ReplaceGlobals(logger)
	tso := &TSO{
		physicalTime: time.Now(),
		logical:      2,
	}

	defer db.Close()
	defer conn.Close()

	rows := new(sql.Rows)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(conn), "QueryContext",
		func(_ *sql.Conn, ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
			return rows, nil
		})
	defer patches.Reset()

	var ret bool = false
	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(rows), "Next",
		func(_ *sql.Rows) bool {
			ret = !ret
			return ret
		})
	defer patches1.Reset()


	var checkpoint string = "{\"consistent\":false,\"commitTS\":427779224058986498,\"ts-map\":{\"primary-ts\":427778572080381953,\"secondary-ts\":427778632089862165},\"schema-version\":61}"
	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(rows), "Scan",
		func(_ *sql.Rows, dest ...interface{}) error {
			dest[0]=&checkpoint
			return nil
		})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(rows), "Close",
		func(_ *sql.Rows) error {
			return nil
		})
	defer patches3.Reset()

	rs :=errors.New("deconde json fail")

	patch := gomonkey.ApplyFunc(json.Unmarshal, func (data []byte, v interface{}) error{
		return rs
	})
	defer patch.Reset()


	ullTs, err := tso.GetTSOFromTiDB(ctx, conn, logger)

	//fmt.Println(ullTs, err)
	ast:=assert.New(t)
	ast.Equal(ullTs,uint64(0))
	ast.Equal(rs,err)
}


//row scan return fail
func TestTSO_GetTSOFromTiDB4(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		panic("new mock db fail," + err.Error())
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		panic("new conn fail," + err.Error())
	}
	cfg := zap.NewDevelopmentConfig()
	cfg.DisableStacktrace = !cfg.Level.Enabled(zap.DebugLevel)
	logger, _ := cfg.Build()
	zap.ReplaceGlobals(logger)
	tso := &TSO{
		physicalTime: time.Now(),
		logical:      2,
	}

	defer db.Close()
	defer conn.Close()

	rows := new(sql.Rows)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(conn), "QueryContext",
		func(_ *sql.Conn, ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
			return rows, nil
		})
	defer patches.Reset()

	var ret bool = false
	patches1 := gomonkey.ApplyMethod(reflect.TypeOf(rows), "Next",
		func(_ *sql.Rows) bool {
			ret = !ret
			return ret
		})
	defer patches1.Reset()

	rs :=errors.New("scan data fail")
	patches2 := gomonkey.ApplyMethod(reflect.TypeOf(rows), "Scan",
		func(_ *sql.Rows, dest ...interface{}) error{
			return rs
		})
	defer patches2.Reset()

	patches3 := gomonkey.ApplyMethod(reflect.TypeOf(rows), "Close",
		func(_ *sql.Rows) error {
			return nil
		})
	defer patches3.Reset()



	patch := gomonkey.ApplyFunc(json.Unmarshal, func (data []byte, v interface{}) error{
		return nil
	})
	defer patch.Reset()


	ullTs, err := tso.GetTSOFromTiDB(ctx, conn, logger)

	//fmt.Println(ullTs, err)
	ast:=assert.New(t)
	ast.Equal(ullTs,uint64(0))
	ast.Equal(rs,err)
}

