// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2012 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package stream

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"
)

type resultSet struct {
	columns     []mysqlField
	columnNames []string
	columnValue [][]driver.Value
	done        bool
}

type mysqlRows struct {
	fsm    *MySQLFSM
	rs     resultSet
	finish func()
}

type binaryRows struct {
	mysqlRows
}

type textRows struct {
	mysqlRows
}

/*func (rows *mysqlRows) Columns() []string {
	if rows.rs.columnNames != nil {
		return rows.rs.columnNames
	}

	columns := make([]string, len(rows.rs.columns))
	if rows.mc != nil && rows.mc.cfg.ColumnsWithAlias {
		for i := range columns {
			if tableName := rows.rs.columns[i].tableName; len(tableName) > 0 {
				columns[i] = tableName + "." + rows.rs.columns[i].name
			} else {
				columns[i] = rows.rs.columns[i].name
			}
		}
	} else {
		for i := range columns {
			columns[i] = rows.rs.columns[i].name
		}
	}

	rows.rs.columnNames = columns
	return columns
}*/

func (rows *mysqlRows) ColumnTypeDatabaseTypeName(i int) string {
	return rows.rs.columns[i].typeDatabaseName()
}

// func (rows *mysqlRows) ColumnTypeLength(i int) (length int64, ok bool) {
// 	return int64(rows.rs.columns[i].length), true
// }

func (rows *mysqlRows) ColumnTypeNullable(i int) (nullable, ok bool) {
	return rows.rs.columns[i].flags&flagNotNULL == 0, true
}

func (rows *mysqlRows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	column := rows.rs.columns[i]
	decimals := int64(column.decimals)

	switch column.fieldType {
	case fieldTypeDecimal, fieldTypeNewDecimal:
		if decimals > 0 {
			return int64(column.length) - 2, decimals, true
		}
		return int64(column.length) - 1, decimals, true
	case fieldTypeTimestamp, fieldTypeDateTime, fieldTypeTime:
		return decimals, decimals, true
	case fieldTypeFloat, fieldTypeDouble:
		if decimals == 0x1f {
			return math.MaxInt64, math.MaxInt64, true
		}
		return math.MaxInt64, decimals, true
	}

	return 0, 0, false
}

func (rows *mysqlRows) ColumnTypeScanType(i int) reflect.Type {
	return rows.rs.columns[i].scanType()
}

func (rows *mysqlRows) Close() (err error) {
	/*if f := rows.finish; f != nil {
		f()
		rows.finish = nil
	}
	*/
	/*mc := rows.mc
	if mc == nil {
		return nil
	}
	if err := mc.error(); err != nil {
		return err
	}*/

	// flip the buffer for this connection if we need to drain it.
	// note that for a successful query (i.e. one where rows.next()
	// has been called until it returns false), `rows.mc` will be nil
	// by the time the user calls `(*Rows).Close`, so we won't reach this
	// see: https://github.com/golang/go/commit/651ddbdb5056ded455f47f9c494c67b389622a47
	/*mc.buf.flip()

	// Remove unread packets from stream
	if !rows.rs.done {
		err = mc.readUntilEOF()
	}
	if err == nil {
		if err = mc.discardResults(); err != nil {
			return err
		}
	}

	//rows.mc = nil
	return err*/
	return nil
}

func (rows *mysqlRows) HasNextResultSet() (b bool) {
	/*if rows.mc == nil {
		return false
	}*/
	return rows.fsm.status&statusMoreResultsExists != 0
}

func (rows *mysqlRows) nextResultSet() (int, error) {
	/*if rows.mc == nil {
		return 0, io.EOF
	}
	if err := rows.mc.error(); err != nil {
		return 0, err
	}*/

	// Remove unread packets from stream
	if !rows.rs.done {
		if err := rows.fsm.readUntilEOF(); err != nil {
			return 0, err
		}
		rows.rs.done = true
	}

	if !rows.HasNextResultSet() {
		//rows.mc = nil
		return 0, io.EOF
	}
	rows.rs = resultSet{}
	return rows.fsm.readResultSetHeaderPacket()
}

func (rows *mysqlRows) nextNotEmptyResultSet() (int, error) {
	for {
		resLen, err := rows.nextResultSet()
		if err != nil {
			return 0, err
		}

		if resLen > 0 {
			return resLen, nil
		}

		rows.rs.done = true
	}
}

func (rows *binaryRows) NextResultSet() error {
	resLen, err := rows.nextNotEmptyResultSet()
	if err != nil {
		return err
	}

	rows.rs.columns, err = rows.fsm.readColumns(resLen)
	return err
}

func (rows *binaryRows) Next(dest []driver.Value) error {
	/*if mc := rows.mc; mc != nil {
	if err := mc.error(); err != nil {
		return err
	}

	// Fetch next row from stream*/
	return rows.readRow(dest)
	/*}
	return io.EOF*/
}

func (rows *textRows) NextResultSet() (err error) {
	resLen, err := rows.nextNotEmptyResultSet()
	if err != nil {
		return err
	}

	rows.rs.columns, err = rows.fsm.readColumns(resLen)
	return err
}

func (rows *textRows) Next(dest []driver.Value) error {
	/*if mc := rows.mc; mc != nil {
	if err := mc.error(); err != nil {
		return err
	}

	// Fetch next row from stream*/
	return rows.readRow(dest)
	/*}
	return io.EOF*/
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
func (rows *textRows) readRow(dest []driver.Value) error {
	fsm := rows.fsm
	var err error
	if rows.rs.done {
		return io.EOF
	}

	/*data, err := mc.readPacket()
	if err != nil {
		return err
	}*/
	res := rows.fsm.load(fsm.packetnum)
	if !res {
		return ErrLoadBuffer
	}
	fsm.packetnum++
	data := rows.fsm.data.Bytes()
	// EOF Packet
	if data[0] == iEOF && len(data) == 5 {
		// server_status [2 bytes]
		fsm.status = readStatus(data[3:])
		rows.rs.done = true
		if !rows.HasNextResultSet() {
			//rows.mc = nil
		}
		return io.EOF
	}
	if data[0] == iERR {
		//rows.mc = nil
		return fsm.handleErrorPacket(data)
	}

	// RowSet Packet
	var (
		n      int
		isNull bool
		pos    int = 0
	)

	for i := range dest {
		// Read bytes and convert to string
		dest[i], isNull, n, err = readLengthEncodedString(data[pos:])
		pos += n

		if err != nil {
			return err
		}

		if isNull {
			dest[i] = nil
			continue
		}

		if !fsm.parseTime {
			continue
		}

		// Parse time field
		switch rows.rs.columns[i].fieldType {
		case fieldTypeTimestamp,
			fieldTypeDateTime,
			fieldTypeDate,
			fieldTypeNewDate:
			if dest[i], err = parseDateTime(dest[i].([]byte), time.UTC); err != nil {
				return err
			}
		}
	}

	return nil
}

// http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html
func (rows *binaryRows) readRow(dest []driver.Value) error {
	fsm := rows.fsm
	var err error
	res := fsm.load(fsm.packetnum)
	if !res {
		return ErrLoadBuffer
	}
	fsm.packetnum++
	data := fsm.data.Bytes()
	// packet indicator [1 byte]
	if data[0] != iOK {
		// EOF Packet
		if data[0] == iEOF && len(data) == 5 {
			rows.fsm.status = readStatus(data[3:])
			rows.rs.done = true
			if !rows.HasNextResultSet() {
				//rows.mc = nil
			}
			return io.EOF
		}
		//mc := rows.mc
		//rows.mc = nil

		// Error otherwise
		return fsm.handleErrorPacket(data)
	}

	// NULL-bitmap,  [(column-count + 7 + 2) / 8 bytes]
	pos := 1 + (len(dest)+7+2)>>3
	nullMask := data[1:pos]

	for i := range dest {
		// Field is NULL
		// (byte >> bit-pos) % 2 == 1
		if ((nullMask[(i+2)>>3] >> uint((i+2)&7)) & 1) == 1 {
			dest[i] = nil
			continue
		}

		// Convert to byte-coded string
		switch rows.rs.columns[i].fieldType {
		case fieldTypeNULL:
			dest[i] = nil
			continue

		// Numeric Types
		case fieldTypeTiny:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(data[pos])
			} else {
				dest[i] = int64(int8(data[pos]))
			}
			pos++
			continue

		case fieldTypeShort, fieldTypeYear:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(binary.LittleEndian.Uint16(data[pos : pos+2]))
			} else {
				dest[i] = int64(int16(binary.LittleEndian.Uint16(data[pos : pos+2])))
			}
			pos += 2
			continue

		case fieldTypeInt24, fieldTypeLong:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				dest[i] = int64(binary.LittleEndian.Uint32(data[pos : pos+4]))
			} else {
				dest[i] = int64(int32(binary.LittleEndian.Uint32(data[pos : pos+4])))
			}
			pos += 4
			continue

		case fieldTypeLongLong:
			if rows.rs.columns[i].flags&flagUnsigned != 0 {
				val := binary.LittleEndian.Uint64(data[pos : pos+8])
				if val > math.MaxInt64 {
					dest[i] = uint64ToString(val)
				} else {
					dest[i] = int64(val)
				}
			} else {
				dest[i] = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			}
			pos += 8
			continue

		case fieldTypeFloat:
			dest[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4
			continue

		case fieldTypeDouble:
			dest[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[pos : pos+8]))
			pos += 8
			continue

		// Length coded Binary Strings
		case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
			fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
			fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
			fieldTypeVarString, fieldTypeString, fieldTypeGeometry, fieldTypeJSON:
			var isNull bool
			var n int
			dest[i], isNull, n, err = readLengthEncodedString(data[pos:])
			pos += n
			if err == nil {
				if !isNull {
					continue
				} else {
					dest[i] = nil
					continue
				}
			}
			return err

		case
			fieldTypeDate, fieldTypeNewDate, // Date YYYY-MM-DD
			fieldTypeTime,                         // Time [-][H]HH:MM:SS[.fractal]
			fieldTypeTimestamp, fieldTypeDateTime: // Timestamp YYYY-MM-DD HH:MM:SS[.fractal]

			num, isNull, n := readLengthEncodedInteger(data[pos:])
			pos += n

			switch {
			case isNull:
				dest[i] = nil
				continue
			case rows.rs.columns[i].fieldType == fieldTypeTime:
				// database/sql does not support an equivalent to TIME, return a string
				var dstlen uint8
				switch decimals := rows.rs.columns[i].decimals; decimals {
				case 0x00, 0x1f:
					dstlen = 8
				case 1, 2, 3, 4, 5, 6:
					dstlen = 8 + 1 + decimals
				default:
					return fmt.Errorf(
						"protocol error, illegal decimals value %d",
						rows.rs.columns[i].decimals,
					)
				}
				dest[i], err = formatBinaryTime(data[pos:pos+int(num)], dstlen)
			case rows.fsm.parseTime:
				dest[i], err = parseBinaryDateTime(num, data[pos:], time.UTC)
			default:
				var dstlen uint8
				if rows.rs.columns[i].fieldType == fieldTypeDate {
					dstlen = 10
				} else {
					switch decimals := rows.rs.columns[i].decimals; decimals {
					case 0x00, 0x1f:
						dstlen = 19
					case 1, 2, 3, 4, 5, 6:
						dstlen = 19 + 1 + decimals
					default:
						return fmt.Errorf(
							"protocol error, illegal decimals value %d",
							rows.rs.columns[i].decimals,
						)
					}
				}
				dest[i], err = formatBinaryDateTime(data[pos:pos+int(num)], dstlen)
			}

			if err == nil {
				pos += int(num)
				continue
			} else {
				return err
			}

		// Please report if this happens!
		default:
			return fmt.Errorf("unknown field type %d", rows.rs.columns[i].fieldType)
		}
	}

	return nil
}
