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
 * @File:  const.go
 * @Version: 1.0.0
 * @Date: 2021/11/30 09:58
 */

package util

var UINT64MAX uint64= 1<<64 -1

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
	StateSkipPacket
)
const (
	EventHandshake uint64 = iota
	EventQuit
	EventQuery
	EventStmtPrepare
	EventStmtExecute
	EventStmtClose
)
const (
	RunText = iota
	RunDir
	RunOnline
)

const (
	NotWriteLog uint16 = iota
	NeedWriteLog
	NeedReplaySQL
)