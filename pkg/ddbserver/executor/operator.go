/*
@author: panfengguo
@since: 2022/11/20
@desc: desc
*/
package executor

import (
	"fmt"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"
	"strconv"
	"sync"
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
	result, err := NewQueryResult(columnTypes, op.ScanOper.TableName)
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

	// 并发执行子树操作
	var (
		wg                      sync.WaitGroup
		resultLeft, resultRight *QueryResult
		leftErr, rightErr       error
	)
	wg.Add(2)
	go func() {
		resultLeft, leftErr = e.ExecuteFunc(op.Lchild)
		wg.Done()
	}()
	go func() {
		resultRight, rightErr = e.ExecuteFunc(op.Rchild)
		wg.Done()
	}()
	wg.Wait()
	if leftErr != nil {
		return nil, leftErr
	}
	if rightErr != nil {
		return nil, rightErr
	}
	if resultLeft == nil {
		return resultRight, nil
	} else if resultRight == nil {
		return resultLeft, nil
	} else {
		resultLeft.Data = append(resultLeft.Data, resultRight.Data...)
		return resultLeft, nil
	}
}

func (e *Executor) ExecutePredicate(op *plan.Operator_) (*QueryResult, error) {
	if op.OperType != plan.Predicate {
		return nil, fmt.Errorf("invalid operator type, get type = %d", op.OperType)
	}
	if op.Lchild == nil {
		return nil, nil
	}
	result, err := e.ExecuteFunc(op.Lchild)
	if err != nil {
		return nil, err
	}
	if op.PredicateOper == nil || result == nil {
		return result, nil
	} else {
		conditions := op.PredicateOper.PredConditions
		filterResult := &QueryResult{
			Field: result.Field,
			Data:  [][]Cell{},
		}
		for _, cell := range result.Data {
			for _, condition := range conditions {
				var leftVal, rightVal Cell
				if condition.Lexpression.IsField {
					leftVal, err = result.getValueByField(condition.Lexpression.Field, cell)
					if err != nil {
						break
					}
				} else {
					intVal, err := strconv.Atoi(string(condition.Lexpression.Value))
					if err != nil {
						leftVal = CellString(condition.Lexpression.Value)
					} else {
						leftVal = CellInt(intVal)
					}
				}

				if condition.Rexpression.IsField {
					rightVal, err = result.getValueByField(condition.Rexpression.Field, cell)
					if err != nil {
						break
					}
				} else {
					intVal, err := strconv.Atoi(string(condition.Rexpression.Value))
					if err != nil {
						rightVal = CellString(condition.Rexpression.Value)
					} else {
						rightVal = CellInt(intVal)
					}
				}

				if Compare(leftVal, rightVal, condition.CompOp) {
					filterResult.Data = append(filterResult.Data, cell)
				}
			}
		}
		return filterResult, nil
	}
}

func (e *Executor) ExecuteJoin(op *plan.Operator_) (*QueryResult, error) {
	if op.OperType != plan.Join {
		return nil, fmt.Errorf("invalid operator type, get type = %d", op.OperType)
	}
	if op.JoinOper.JoinConditions == nil {
		return nil, fmt.Errorf("join condition is nil")
	}
	// 并发执行子树操作
	var (
		wg                      sync.WaitGroup
		resultLeft, resultRight *QueryResult
		leftErr, rightErr       error
	)
	wg.Add(2)
	go func() {
		resultLeft, leftErr = e.ExecuteFunc(op.Lchild)
		wg.Done()
	}()
	go func() {
		resultRight, rightErr = e.ExecuteFunc(op.Rchild)
		wg.Done()
	}()
	wg.Wait()
	if leftErr != nil {
		return nil, leftErr
	}
	if rightErr != nil {
		return nil, rightErr
	}
	if resultLeft == nil {
		return resultLeft, nil
	} else if resultRight == nil {
		return resultRight, nil
	} else {
		conditions := op.JoinOper.JoinConditions
		for _, condition := range conditions {
			if !condition.Lexpression.IsField || !condition.Rexpression.IsField {
				return nil, fmt.Errorf("condition is not field")
			}
		}
		result := &QueryResult{
			Field: []Field{},
			Data:  [][]Cell{},
		}
		// 确定属性
		for _, f := range resultLeft.Field {
			result.Field = append(result.Field, f)
		}
		skipMap := map[int]int{}
		for i, f := range resultRight.Field {
			isSkip := false
			for _, condition := range conditions {
				if condition.Rexpression.Field.FieldName == f.FieldName {
					isSkip = true
					break
				}
			}
			if isSkip {
				skipMap[i] = 1
				continue
			}
			result.Field = append(result.Field, f)
		}
		// 进行join
		for _, leftCell := range resultLeft.Data {
			for _, rightCell := range resultRight.Data {
				isPass := false
				for _, condition := range conditions {
					leftVal, err := resultLeft.getValueByField(condition.Lexpression.Field, leftCell)
					if err != nil {
						return nil, err
					}
					rightVal, err := resultRight.getValueByField(condition.Rexpression.Field, rightCell)
					if err != nil {
						return nil, err
					}
					if !Compare(leftVal, rightVal, condition.CompOp) {
						isPass = true
						break
					}
				}
				if isPass {
					continue
				}
				joinCell := []Cell{}
				for _, c := range leftCell {
					joinCell = append(joinCell, c)
				}
				for i, c := range rightCell {
					if skipMap[i] == 1 {
						continue
					}
					joinCell = append(joinCell, c)
				}
				result.Data = append(result.Data, joinCell)
			}
		}
		return result, nil
	}
}

func (e *Executor) ExecuteProject(op *plan.Operator_) (*QueryResult, error) {
	if op.OperType != plan.Project {
		return nil, fmt.Errorf("invalid operator type, get type = %d", op.OperType)
	}
	result, err := e.ExecuteFunc(op.Lchild)
	if err != nil {
		return nil, err
	}
	if op.ProjectOper == nil {
		return result, nil
	} else {
		projectIdx := []int{}
		for i, f := range result.Field {
			for _, f2 := range op.ProjectOper.Fields {
				if len(f2.TableName) == 0 && f2.FieldName == f.FieldName {
					projectIdx = append(projectIdx, i)
					break
				} else if len(f2.TableName) != 0 &&
					f2.FieldName == f.FieldName &&
					f2.TableName == f.TableName {
					projectIdx = append(projectIdx, i)
					break
				}
			}
		}
		projectResult := &QueryResult{
			Field: []Field{},
			Data:  [][]Cell{},
		}
		for _, idx := range projectIdx {
			projectResult.Field = append(projectResult.Field, result.Field[idx])
		}
		for _, cell := range result.Data {
			tmp := []Cell{}
			for _, idx := range projectIdx {
				tmp = append(tmp, cell[idx])
			}
			projectResult.Data = append(projectResult.Data, tmp)
		}
		return projectResult, nil
	}
}
