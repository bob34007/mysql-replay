/**
 * @Author: guobob
 * @Description:
 * @File:  fields_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/22 15:05
 */

package stream

import (
	"reflect"
	"testing"
)

func Test_mysqlField_typeDatabaseName(t *testing.T) {
	type fields struct {
		tableName string
		name      string
		length    uint32
		flags     fieldFlag
		fieldType fieldType
		decimals  byte
		charSet   uint8
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:"fieldTypeBit",
			fields:fields{
				fieldType:fieldTypeBit,
			},
			want:"BIT",
		},
		{
			name:"fieldTypeBLOB",
			fields:fields{
				fieldType:fieldTypeBLOB,
				charSet: 63,
			},
			want:"BLOB",
		},
		{
			name:"fieldTypeBLOB-1",
			fields:fields{
				fieldType:fieldTypeBLOB,
				charSet: 64,
			},
			want:"TEXT",
		},
		{
			name:"fieldTypeDate",
			fields:fields{
				fieldType:fieldTypeDate,
			},
			want:"DATE",
		},
		{
			name:"fieldTypeDateTime",
			fields:fields{
				fieldType:fieldTypeDateTime,
			},
			want:"DATETIME",
		},
		{
			name:"fieldTypeDecimal",
			fields:fields{
				fieldType:fieldTypeDecimal,
			},
			want:"DECIMAL",
		},
		{
			name:"fieldTypeDouble",
			fields:fields{
				fieldType:fieldTypeDouble,
			},
			want:"DOUBLE",
		},
		{
			name:"fieldTypeEnum",
			fields:fields{
				fieldType:fieldTypeEnum,
			},
			want:"ENUM",
		},
		{
			name:"fieldTypeFloat",
			fields:fields{
				fieldType:fieldTypeFloat,
			},
			want:"FLOAT",
		},
		{
			name:"fieldTypeGeometry",
			fields:fields{
				fieldType:fieldTypeGeometry,
			},
			want:"GEOMETRY",
		},
		{
			name:"fieldTypeInt24",
			fields:fields{
				fieldType:fieldTypeInt24,
			},
			want:"MEDIUMINT",
		},
		{
			name:"fieldTypeJSON",
			fields:fields{
				fieldType:fieldTypeJSON,
			},
			want:"JSON",
		},
		{
			name:"fieldTypeLong",
			fields:fields{
				fieldType:fieldTypeLong,
				flags:flagUnsigned,
			},
			want:"UNSIGNED INT",
		},
		{
			name:"fieldTypeLong",
			fields:fields{
				fieldType:fieldTypeLong,
				flags:0,
			},
			want:"INT",
		},
		{
			name:"fieldTypeLongBLOB",
			fields:fields{
				fieldType:fieldTypeLongBLOB,
				charSet:63,
			},
			want:"LONGBLOB",
		},
		{
			name:"fieldTypeLongBLOB",
			fields:fields{
				fieldType:fieldTypeLongBLOB,
				charSet:64,
			},
			want:"LONGTEXT",
		},
		{
			name:"fieldTypeLongLong",
			fields:fields{
				fieldType:fieldTypeLongLong,
				flags:flagUnsigned,
			},
			want:"UNSIGNED BIGINT",
		},
		{
			name:"fieldTypeLongLong",
			fields:fields{
				fieldType:fieldTypeLongLong,
				flags:0,
			},
			want:"BIGINT",
		},
		{
			name:"fieldTypeMediumBLOB",
			fields:fields{
				fieldType:fieldTypeMediumBLOB,
				charSet:64,
			},
			want:"MEDIUMTEXT",
		},
		{
			name:"fieldTypeMediumBLOB",
			fields:fields{
				fieldType:fieldTypeMediumBLOB,
				charSet:63,
			},
			want:"MEDIUMBLOB",
		},

		{
			name:"fieldTypeNewDate",
			fields:fields{
				fieldType:fieldTypeNewDate,
			},
			want:"DATE",
		},
		{
			name:"fieldTypeNewDecimal",
			fields:fields{
				fieldType:fieldTypeNewDecimal,
			},
			want:"DECIMAL",
		},
		{
			name:"fieldTypeNULL",
			fields:fields{
				fieldType:fieldTypeNULL,
			},
			want:"NULL",
		},
		{
			name:"fieldTypeSet",
			fields:fields{
				fieldType:fieldTypeSet,
			},
			want:"SET",
		},
		{
			name:"fieldTypeShort",
			fields:fields{
				fieldType:fieldTypeShort,
				flags:flagUnsigned,
			},
			want:"UNSIGNED SMALLINT",
		},
		{
			name:"fieldTypeShort",
			fields:fields{
				fieldType:fieldTypeShort,
				flags:0,
			},
			want:"SMALLINT",
		},
		{
			name:"fieldTypeString",
			fields:fields{
				fieldType:fieldTypeString,
				charSet:63,
			},
			want:"BINARY",
		},
		{
			name:"fieldTypeString",
			fields:fields{
				fieldType:fieldTypeString,
				charSet:64,
			},
			want:"CHAR",
		},
		{
			name:"fieldTypeTime",
			fields:fields{
				fieldType:fieldTypeTime,
			},
			want:"TIME",
		},
		{
			name:"fieldTypeTimestamp",
			fields:fields{
				fieldType:fieldTypeTimestamp,
			},
			want:"TIMESTAMP",
		},
		{
			name:"fieldTypeTiny",
			fields:fields{
				fieldType:fieldTypeTiny,
				flags:flagUnsigned,
			},
			want:"UNSIGNED TINYINT",
		},
		{
			name:"fieldTypeTiny",
			fields:fields{
				fieldType:fieldTypeTiny,
				flags:0,
			},
			want:"TINYINT",
		},

		{
			name:"fieldTypeTinyBLOB",
			fields:fields{
				fieldType:fieldTypeTinyBLOB,
				charSet:63,
			},
			want:"TINYBLOB",
		},
		{
			name:"fieldTypeTinyBLOB",
			fields:fields{
				fieldType:fieldTypeTinyBLOB,
				charSet:64,
			},
			want:"TINYTEXT",
		},


		{
			name:"fieldTypeVarChar",
			fields:fields{
				fieldType:fieldTypeVarChar,
				charSet:63,
			},
			want:"VARBINARY",
		},
		{
			name:"fieldTypeVarChar",
			fields:fields{
				fieldType:fieldTypeVarChar,
				charSet:64,
			},
			want:"VARCHAR",
		},

		{
			name:"fieldTypeVarString",
			fields:fields{
				fieldType:fieldTypeVarString,
				charSet:63,
			},
			want:"VARBINARY",
		},
		{
			name:"fieldTypeVarString",
			fields:fields{
				fieldType:fieldTypeVarString,
				charSet:64,
			},
			want:"VARCHAR",
		},

		{
			name:"fieldTypeYear",
			fields:fields{
				fieldType:fieldTypeYear,
			},
			want:"YEAR",
		},

		{
			name:"unknow",
			fields:fields{
				fieldType:100,
			},
			want:"",
		},
	}
		for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mf := &mysqlField{
				tableName: tt.fields.tableName,
				name:      tt.fields.name,
				length:    tt.fields.length,
				flags:     tt.fields.flags,
				fieldType: tt.fields.fieldType,
				decimals:  tt.fields.decimals,
				charSet:   tt.fields.charSet,
			}
			if got := mf.typeDatabaseName(); got != tt.want {
				t.Errorf("typeDatabaseName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mysqlField_scanType(t *testing.T) {
	type fields struct {
		tableName string
		name      string
		length    uint32
		flags     fieldFlag
		fieldType fieldType
		decimals  byte
		charSet   uint8
	}
	tests := []struct {
		name   string
		fields fields
		want   reflect.Type
	}{
		{
			name:"fieldTypeTiny",
			fields:fields{
				fieldType:fieldTypeTiny,
				flags:63,
			},
			want : scanTypeUint8,
		},
		{
			name:"fieldTypeTiny",
			fields:fields{
				fieldType:fieldTypeTiny,
				flags:31,
			},
			want : scanTypeInt8,
		},
		{
			name:"fieldTypeTiny",
			fields:fields{
				fieldType:fieldTypeTiny,
				flags:0,
			},
			want : scanTypeNullInt,
		},
		{
			name:"fieldTypeShort",
			fields:fields{
				fieldType:fieldTypeShort,
				flags:63,
			},
			want : scanTypeUint16,
		},
		{
			name:"fieldTypeShort",
			fields:fields{
				fieldType:fieldTypeShort,
				flags:31,
			},
			want : scanTypeInt16,
		},
		{
			name:"fieldTypeShort",
			fields:fields{
				fieldType:fieldTypeShort,
				flags:0,
			},
			want : scanTypeNullInt,
		},

		{
			name:"fieldTypeInt24",
			fields:fields{
				fieldType:fieldTypeInt24,
				flags:63,
			},
			want : scanTypeUint32,
		},
		{
			name:"fieldTypeInt24",
			fields:fields{
				fieldType:fieldTypeInt24,
				flags:31,
			},
			want : scanTypeInt32,
		},
		{
			name:"fieldTypeInt24",
			fields:fields{
				fieldType:fieldTypeInt24,
				flags:0,
			},
			want : scanTypeNullInt,
		},


		{
			name:"fieldTypeLongLong",
			fields:fields{
				fieldType:fieldTypeLongLong,
				flags:63,
			},
			want : scanTypeUint64,
		},
		{
			name:"fieldTypeLongLong",
			fields:fields{
				fieldType:fieldTypeLongLong,
				flags:31,
			},
			want : scanTypeInt64,
		},
		{
			name:"fieldTypeLongLong",
			fields:fields{
				fieldType:fieldTypeLongLong,
				flags:0,
			},
			want : scanTypeNullInt,
		},


		{
			name:"fieldTypeFloat",
			fields:fields{
				fieldType:fieldTypeFloat,
				flags:31,
			},
			want : scanTypeFloat32,
		},
		{
			name:"fieldTypeFloat",
			fields:fields{
				fieldType:fieldTypeFloat,
				flags:0,
			},
			want : scanTypeNullFloat,
		},


		{
			name:"fieldTypeDouble",
			fields:fields{
				fieldType:fieldTypeDouble,
				flags:31,
			},
			want : scanTypeFloat64,
		},
		{
			name:"fieldTypeDouble",
			fields:fields{
				fieldType:fieldTypeDouble,
				flags:0,
			},
			want : scanTypeNullFloat,
		},

		{
			name:"fieldTypeDecimal",
			fields:fields{
				fieldType:fieldTypeDecimal,
			},
			want : scanTypeRawBytes,
		},

		{
			name:"fieldTypeDate",
			fields:fields{
				fieldType:fieldTypeDate,
			},
			want : scanTypeNullTime,
		},

		{
			name:"UNknown",
			fields:fields{
				fieldType:100,
			},
			want : scanTypeUnknown,
		},
	}
		for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mf := &mysqlField{
				tableName: tt.fields.tableName,
				name:      tt.fields.name,
				length:    tt.fields.length,
				flags:     tt.fields.flags,
				fieldType: tt.fields.fieldType,
				decimals:  tt.fields.decimals,
				charSet:   tt.fields.charSet,
			}
			if got := mf.scanType(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("scanType() = %v, want %v", got, tt.want)
			}
		})
	}
}