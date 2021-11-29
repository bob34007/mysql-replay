/*
 * Copyright (c)  2021 PingCAP, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 * @Author: guobob
 * @Description:
 * @File:  parse_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/29 15:48
 */

package parse

import (
	"github.com/agiledragon/gomonkey"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func Test_Parse_fail(t *testing.T){
	sql:= "select * from t"
	h := parser.New()
	patches := gomonkey.ApplyMethod(reflect.TypeOf(h), "Parse",
		func  (_ *parser.Parser,sql, charset, collation string) (stmt []ast.StmtNode, warns []error, err error){
			return nil,nil,errors.New("parse sql fail")
		})
	defer patches.Reset()
	_ , err1:= Parse(sql)
	assert.New(t).NotNil(err1)
}

func Test_Parse_len_zero(t *testing.T){
	sql:= "select * from t"
	h := parser.New()
	patches := gomonkey.ApplyMethod(reflect.TypeOf(h), "Parse",
		func  (_ *parser.Parser,sql, charset, collation string) (stmt []ast.StmtNode, warns []error, err error){
			return nil,nil,nil
		})
	defer patches.Reset()
	_ , err1:= Parse(sql)
	assert.New(t).NotNil(err1)
}

func Test_Parse_succ(t *testing.T){
	sql:= "select * from t ;"
	_ , err1:= Parse(sql)
	assert.New(t).Nil(err1)
}

func Test_Extract(t *testing.T) {
	sqls := []string{
		"select * from t;",
		"select id,name from t into outfile \"a.txt\";",
		"set tidb_general_log=on;",
		"update t set id =1 where id =2;",
		"insert into t (id,name) values (1,'abc');",
		"delete from t where id =1;",
		"create table a (id int );",
		"alter table a drop column id;",
		"alter table add index idx1(id);",
		"use tpcc",
		"alter table a drop index idx1;",
	}

	for _, sql := range sqls {
		stmtNode, err := Parse(sql)
		if err != nil {
			continue
		}
		_ = Extract(stmtNode)
		//fmt.Printf("sql %s is %d\n", sql, kind)
	}
}