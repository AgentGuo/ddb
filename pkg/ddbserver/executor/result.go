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
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"
	"reflect"
	"strconv"
	"strings"
)

const separator = "\t|\t"

type QueryResult struct {
	Field []Field  // 字段名
	Data  [][]Cell // 字段值
}

type Field struct {
	TableName string
	FieldName string
	Type      reflect.Kind
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

func (q *QueryResult) getValueByField(field plan.Field_, row []Cell) (Cell, error) {
	for i, f := range q.Field {
		if len(field.TableName) == 0 && field.FieldName == f.FieldName {
			return row[i], nil
		} else if len(field.TableName) != 0 &&
			field.TableName == f.TableName &&
			field.FieldName == f.FieldName {
			return row[i], nil
		}
	}
	return nil, fmt.Errorf("no match field, get filed:%v, result filed:%v", field, q.Field)
}

func NewQueryResult(columnTypes []*sql.ColumnType, tableName string) (*QueryResult, error) {
	field := make([]Field, 0)
	for _, columnType := range columnTypes {
		cellType := columnType.ScanType().Kind()
		field = append(field, Field{
			TableName: tableName,
			FieldName: columnType.Name(),
			Type:      cellType,
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
	ret := separator
	for i, f := range q.Field {
		if i == len(q.Field)-1 {
			ret += f.TableName + "." + f.FieldName + "\n"
		} else {
			ret += f.TableName + "." + f.FieldName + separator
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

func Compare(cell1 Cell, cell2 Cell, comp plan.CompareType_) bool {
	var compResult int = 0
	if cell1.Type() == CellIntType && cell2.Type() == CellStringType {
		intVal, err := strconv.Atoi(cell2.String())
		if err == nil {
			cell2 = CellInt(intVal)
		}
	} else if cell2.Type() == CellIntType && cell1.Type() == CellStringType {
		intVal, err := strconv.Atoi(cell1.String())
		if err == nil {
			cell1 = CellInt(intVal)
		}
	}
	if cell1.Type() == CellIntType && cell2.Type() == CellIntType {
		compResult = int(cell1.(CellInt)) - int(cell2.(CellInt))
	} else if cell1.Type() == CellStringType && cell2.Type() == CellStringType {
		compResult = strings.Compare(string(cell1.(CellString)), string(cell2.(CellString)))
	} else {
		compResult = strings.Compare(cell1.String(), cell2.String())
	}
	switch comp {
	case plan.Lt:
		return compResult < 0
	case plan.Le:
		return compResult <= 0
	case plan.Eq:
		return compResult == 0
	case plan.Ge:
		return compResult >= 0
	case plan.Gt:
		return compResult > 0
	case plan.Neq:
		return compResult != 0
	default:
		return false
	}
}

func init() {
	gob.Register(CellInt(0))
	gob.Register(CellString(""))
}
