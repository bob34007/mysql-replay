package stream

import (
	"database/sql/driver"
	"github.com/agiledragon/gomonkey"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
)

var logger *zap.Logger

func init (){
	cfg := zap.NewDevelopmentConfig()
	//cfg.Level = zap.NewAtomicLevelAt()
	cfg.DisableStacktrace = !cfg.Level.Enabled(zap.DebugLevel)
	logger, _ = cfg.Build()
	zap.ReplaceGlobals(logger)
	logger = zap.L().With(zap.String("conn","test-mysql.go"))
	logger = logger.Named("test")
}

//test a,b with nil
func TestMysql_CompareValue(t *testing.T) {
	var a driver.Value =nil
	var b driver.Value =nil

	res,err:=CompareValue(a,b,logger)

	ast := assert.New(t)

	ast.Equal(res,true)
	ast.Nil(err)
}

//test a not nil,b nil
func TestMysql_CompareValue1(t *testing.T) {
	var a driver.Value ="abc"
	var b driver.Value =nil

	res,err:=CompareValue(a,b,logger)

	ast := assert.New(t)

	ast.Equal(res,false)
	ast.Nil(err)
}

//test a  nil,b not nil
func TestMysql_CompareValue2(t *testing.T) {
	var a driver.Value =nil
	var b driver.Value ="abc"

	res,err:=CompareValue(a,b,logger)

	ast := assert.New(t)

	ast.Equal(res,false)
	ast.Nil(err)
}

//test a ,b not nil ,and equal
func TestMysql_CompareValue3(t *testing.T) {
	var a driver.Value ="abc"
	var b driver.Value ="abc"

	res,err:=CompareValue(a,b,logger)

	ast := assert.New(t)

	ast.Equal(res,true)
	ast.Nil(err)
}

func TestMysql_CompareValue4(t *testing.T){
	var a driver.Value ="abc"
	var b driver.Value="abc"
	var ret error = errors.New("convert a to string fail")
	patch := gomonkey.ApplyFunc(convertAssignRows, func (dest interface{}, src interface{}) error{
		return ret
	})
	defer patch.Reset()
	res,err:=CompareValue(a,b,logger)
	ast := assert.New(t)

	ast.Equal(res,false)
	ast.Equal(err,ret)
}