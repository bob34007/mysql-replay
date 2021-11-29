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
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
)

const (
	SelectStmt uint16 = iota
	OutfileStmt
	SetStmt
	UseStmt
	UpdateStmt
	InsertStmt
	DeleteStmt
	DDLStmt
	UnknownStmt
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
				return SelectStmt
			} else {
				return OutfileStmt
			}
		case *ast.SetStmt:
			return SetStmt
		case *ast.UseStmt:
			return UseStmt
		case *ast.UpdateStmt:
			return UpdateStmt
		case *ast.InsertStmt:
			return InsertStmt
		case *ast.DeleteStmt:
			return DeleteStmt
		case *ast.AlterTableStmt, *ast.AlterSequenceStmt,
			*ast.CreateDatabaseStmt, *ast.CreateIndexStmt,
			*ast.CreateTableStmt, *ast.CreateViewStmt,
			*ast.CreateSequenceStmt, *ast.DropDatabaseStmt,
			*ast.DropIndexStmt, *ast.DropTableStmt,
			*ast.DropSequenceStmt, *ast.DropPlacementPolicyStmt,
			*ast.RenameTableStmt, *ast.TruncateTableStmt,
			*ast.RepairTableStmt:
			return DDLStmt
		default:
			//fmt.Printf("%T\n", v.ns[0])
			return UnknownStmt
		}
	}

	return UnknownStmt
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
