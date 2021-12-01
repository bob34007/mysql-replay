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
 * @File:  parse.go
 * @Version: 1.0.0
 * @Date: 2021/11/29 15:39
 */

package parse

import (
	"errors"
	"github.com/bobguo/mysql-replay/util"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
	"go.uber.org/zap"
)



type CheckIsSelectOrNot struct {
	ns []ast.Node
	//t  bool
}

func (v *CheckIsSelectOrNot) Enter(in ast.Node) (ast.Node, bool) {
	v.ns = append(v.ns, in)
	return in, false
}

func (v *CheckIsSelectOrNot) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func Extract(rootNode *ast.StmtNode) uint16 {

	v := &CheckIsSelectOrNot{}

	(*rootNode).Accept(v)

	//we think of the ast with the root node SelectStmt as a select statement
	//except select into outfile statement
	if len(v.ns) > 0 {
		switch v.ns[0].(type) {
		case *ast.SelectStmt:
			if v.ns[0].(*ast.SelectStmt).SelectIntoOpt == nil {
				return util.SelectStmt
			} else {
				return util.OutfileStmt
			}
		case *ast.SetStmt:
			return util.SetStmt
		case *ast.UseStmt:
			return util.UseStmt
		case *ast.UpdateStmt:
			return util.UpdateStmt
		case *ast.InsertStmt:
			return util.InsertStmt
		case *ast.DeleteStmt:
			return util.DeleteStmt
		case *ast.AlterTableStmt, *ast.AlterSequenceStmt,
			*ast.CreateDatabaseStmt, *ast.CreateIndexStmt,
			*ast.CreateTableStmt, *ast.CreateViewStmt,
			*ast.CreateSequenceStmt, *ast.DropDatabaseStmt,
			*ast.DropIndexStmt, *ast.DropTableStmt,
			*ast.DropSequenceStmt, *ast.DropPlacementPolicyStmt,
			*ast.RenameTableStmt, *ast.TruncateTableStmt,
			*ast.RepairTableStmt:
			return util.DDLStmt
		default:
			return util.UnknownStmt
		}
	}

	return util.UnknownStmt
}

func Parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	if len(stmtNodes) == 0 {
		return nil, errors.New("parse sql result is nil , " + sql)
	}

	return &stmtNodes[0], nil
}


func GetSQLStmtType(sql string) (uint16,error){
	stmtNode,err := Parse(sql)
	if err!=nil{
		return util.UnknownStmt,err
	}
	stmtType :=Extract(stmtNode)
	return stmtType,nil
}

func CheckNeedReplay(sql string,log *zap.Logger) bool{
	stmtType ,err := GetSQLStmtType(sql)
	if err != nil {
		log.Warn("get sql stmtType fail , "+ err.Error())
		return false
	}
	switch stmtType{
	case util.UseStmt,util.SetStmt,util.DDLStmt:
		return true
	default:
		return false
	}
	return false
}