/*
@author: panfengguo
@since: 2022/11/20
@desc: desc
*/
package executor

import (
	"fmt"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"
)

const ScanSqlStr = "SELECT * FROM %s;"

func (e *Executor) ExecuteScan(op *plan.Operator_) (*QueryResult, error) {
	if op.OperType != plan.Scan {
		return nil, fmt.Errorf("invalid operator type, get type = %d", op.OperType)
	}
	if op.ScanOper == nil {
		return nil, fmt.Errorf("operator is nil, operator type = %d", op.OperType)
	}
	tx, err := e.Db.Begin()
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(fmt.Sprintf(ScanSqlStr, op.ScanOper.TableName))
	if err != nil {
		return nil, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	result, err := NewQueryResult(columnTypes)
	if err != nil {
		return nil, err
	}
	dataRow := make([]interface{}, len(result.Field))
	pointer := make([]interface{}, len(dataRow))
	for i, _ := range dataRow {
		pointer[i] = &dataRow[i]
	}
	for rows.Next() {
		err = rows.Scan(pointer...)
		if err != nil {
			return nil, err
		}
		result.appendDataRow(dataRow)
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Executor) ExecuteUnion(op *plan.Operator_) (*QueryResult, error) {
	if op.OperType != plan.Union {
		return nil, fmt.Errorf("invalid operator type, get type = %d", op.OperType)
	}
	if op.UnionOper == nil {
		return nil, fmt.Errorf("operator is nil, operator type = %d", op.OperType)
	}

	resultLeft, err := e.ExecuteFunc(op.Lchild)
	if err != nil {
		return nil, err
	}
	resultRight, err := e.ExecuteFunc(op.Rchild)
	if err != nil {
		return nil, err
	}
	if resultLeft == nil {
		return resultRight, nil
	} else if resultRight == nil {
		return resultRight, nil
	} else {
		resultLeft.Data = append(resultLeft.Data, resultRight.Data...)
		return resultLeft, nil
	}
}
