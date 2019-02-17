package hive

import (
	"fmt"
	"reflect"
	"time"

	"github.com/bippio/go-impala/services/cli_service"
)

type TableSchema struct {
	Columns []*ColDesc
}

type ColDesc struct {
	Name string

	DatabaseTypeName string
	ScanType         reflect.Type

	ColumnTypeNullable  bool
	ColumnTypeLength    int64
	ColumnTypePrecision int64
	ColumnTypeScale     int64
}

func typeOf(entry *cli_service.TPrimitiveTypeEntry) reflect.Type {
	switch entry.Type {
	case cli_service.TTypeId_BOOLEAN_TYPE:
		return reflect.TypeOf(true)
	case cli_service.TTypeId_TINYINT_TYPE:
		return reflect.TypeOf(int8(0))
	case cli_service.TTypeId_SMALLINT_TYPE:
		return reflect.TypeOf(int16(0))
	case cli_service.TTypeId_INT_TYPE:
		return reflect.TypeOf(int32(0))
	case cli_service.TTypeId_BIGINT_TYPE:
		return reflect.TypeOf(int64(0))
	case cli_service.TTypeId_FLOAT_TYPE:
		return reflect.TypeOf(float32(0))
	case cli_service.TTypeId_DOUBLE_TYPE:
		return reflect.TypeOf(float64(0))
	case cli_service.TTypeId_STRING_TYPE:
		return reflect.TypeOf("")
	case cli_service.TTypeId_CHAR_TYPE:
		return reflect.TypeOf("")
	case cli_service.TTypeId_VARCHAR_TYPE:
		return reflect.TypeOf("")
	case cli_service.TTypeId_TIMESTAMP_TYPE:
		return reflect.TypeOf(time.Time{})
	}

	panic(fmt.Sprintf("unknown column type: %s", entry.Type))
}
