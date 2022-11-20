/*
@author: panfengguo
@since: 2022/11/19
@desc: desc
*/
package executor

import (
	"database/sql"
	"encoding/gob"
	"fmt"
	"reflect"
	"strconv"
)

const separator = "\t|\t"

type QueryResult struct {
	Field []Filed  // 字段名
	Data  [][]Cell // 字段值
}

type Filed struct {
	Name string
	Type reflect.Kind
}

type TempType uint32

func (q *QueryResult) NewDataRow() ([]interface{}, error) {
	dataRow := make([]interface{}, len(q.Field))
	return dataRow, nil
}

func (q *QueryResult) appendDataRow(dataRow []interface{}) {
	q.Data = append(q.Data, make([]Cell, len(q.Field)))
	idx := len(q.Data) - 1
	for i := range q.Field {
		if q.Field[i].Type == reflect.Int32 {
			tmp, _ := strconv.Atoi(string(dataRow[i].([]uint8)))
			q.Data[idx][i] = CellInt(tmp)
		} else {
			q.Data[idx][i] = CellString(dataRow[i].([]uint8))
		}
	}
}

func NewQueryResult(columnTypes []*sql.ColumnType) (*QueryResult, error) {
	field := make([]Filed, 0)
	for _, columnType := range columnTypes {
		cellType := columnType.ScanType().Kind()
		field = append(field, Filed{
			Name: columnType.Name(),
			Type: cellType,
		})
	}
	return &QueryResult{
		Field: field,
	}, nil
}

func (q *QueryResult) String() string {
	if q == nil || q.Data == nil {
		return ""
	}
	// header
	ret := ""
	for i, f := range q.Field {
		if i == len(q.Field)-1 {
			ret += f.Name + "\n"
		} else {
			ret += f.Name + separator
		}
	}
	// tuple
	for i, row := range q.Data {
		ret += strconv.Itoa(i+1) + separator
		for j, col := range row {
			if j == len(row)-1 {
				ret += fmt.Sprint(col) + "\n"
			} else {
				ret += fmt.Sprint(col) + separator
			}
		}
	}
	ret += fmt.Sprintf("total count = %d\n", len(q.Data))
	return ret
}

type CellType int

const (
	CellIntType = iota
	CellStringType
)

type Cell interface {
	fmt.Stringer
	Type() CellType
}

type CellInt uint32

func (c CellInt) String() string {
	return strconv.Itoa(int(c))
}

func (c CellInt) Type() CellType {
	return CellIntType
}

type CellString string

func (c CellString) String() string {
	return string(c)
}

func (c CellString) Type() CellType {
	return CellStringType
}

func init() {
	gob.Register(CellInt(0))
	gob.Register(CellString(0))
}
