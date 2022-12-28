/*
@author: panfengguo
@since: 2022/11/20
@desc: desc
*/
package executor

import (
	"fmt"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"
	"log"
	"strconv"
	"sync"
)

const (
	ScanSqlStr  = "SELECT * FROM %s;"
	CreateTbStr = `
CREATE TABLE IF NOT EXISTS %s(%s
)ENGINE=InnoDB DEFAULT CHARSET=utf8;`
	InsertSqlStr      = `INSERT INTO %s (%s) VALUES (%s);`
	DeleteSqlStr      = `DELETE FROM %s;`
	SelectCountSqlStr = `SELECT COUNT(1) FROM %s;`
)

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
	log.Printf("ExecuteScan: result row num = %d\n", len(result.Data))
	return result, nil
}

func (e *Executor) ExecuteUnion(op *plan.Operator_) (*QueryResult, error) {
	if op.OperType != plan.Union {
		return nil, fmt.Errorf("invalid operator type, get type = %d", op.OperType)
	}

	// 并发执行子树操作
	wg := sync.WaitGroup{}
	resultList := []QueryResult{}
	errList := []error{}
	resultLock := sync.Mutex{}
	for i, _ := range op.Childs {
		wg.Add(1)
		go func(index int) {
			result, err := e.ExecuteFunc(op.Childs[index])
			resultLock.Lock()
			if result != nil {
				resultList = append(resultList, *result)
			}
			errList = append(errList, err)
			resultLock.Unlock()
			wg.Done()
		}(i)
	}
	wg.Wait()
	for _, err := range errList {
		if err != nil {
			return nil, err
		}
	}
	if len(resultList) == 0 {
		return nil, nil
	} else {
		result := resultList[0]
		for i, r := range resultList {
			if i == 0 {
				continue
			}
			result.Data = append(result.Data, r.Data...)
		}
		log.Printf("ExecuteUnion: result row num = %d\n", len(result.Data))
		return &result, nil
	}
}

func (e *Executor) ExecutePredicate(op *plan.Operator_) (*QueryResult, error) {
	if op.OperType != plan.Predicate {
		return nil, fmt.Errorf("invalid operator type, get type = %d", op.OperType)
	}
	if len(op.Childs) == 0 {
		return nil, nil
	}
	result, err := e.ExecuteFunc(op.Childs[0])
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
			filterFlag := true
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

				if !Compare(leftVal, rightVal, condition.CompOp) {
					filterFlag = false
					break
				}
			}
			if filterFlag {
				filterResult.Data = append(filterResult.Data, cell)
			}
		}
		log.Printf("ExecutePredicate: result row num = %d\n", len(filterResult.Data))
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
	if len(op.Childs) != 2 {
		return nil, fmt.Errorf("join child is not 2")
	}
	// 并发执行子树操作
	var (
		wg                      sync.WaitGroup
		resultLeft, resultRight *QueryResult
		leftErr, rightErr       error
	)
	wg.Add(2)
	go func() {
		resultLeft, leftErr = e.ExecuteFunc(op.Childs[0])
		wg.Done()
	}()
	go func() {
		resultRight, rightErr = e.ExecuteFunc(op.Childs[1])
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
		return nil, nil
	} else if resultRight == nil {
		return nil, nil
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
		for _, f := range resultRight.Field {
			result.Field = append(result.Field, f)
		}
		// 进行join
		hashJoinFlag := true
		for _, condition := range conditions {
			if condition.CompOp != plan.Eq {
				hashJoinFlag = false
			}
		}
		if hashJoinFlag { // 只包含eq，使用hash join
			hashTb := map[string][]int{}
			for i, leftCell := range resultLeft.Data {
				key := ""
				for _, condition := range conditions {
					leftVal, err := resultLeft.getValueByField(condition.Lexpression.Field, leftCell)
					if err != nil {
						leftVal, err = resultLeft.getValueByField(condition.Rexpression.Field, leftCell)
						if err != nil {
							return nil, err
						}
					}
					key += leftVal.String()
				}
				if _, ok := hashTb[key]; ok {
					hashTb[key] = append(hashTb[key], i)
				} else {
					hashTb[key] = []int{i}
				}
			}
			for _, rightCell := range resultRight.Data {
				key := ""
				for _, condition := range conditions {
					rightVal, err := resultRight.getValueByField(condition.Rexpression.Field, rightCell)
					if err != nil {
						rightVal, err = resultRight.getValueByField(condition.Lexpression.Field, rightCell)
						if err != nil {
							return nil, err
						}
						return nil, err
					}
					key += rightVal.String()
				}
				if idxList, ok := hashTb[key]; ok {
					for _, idx := range idxList {
						joinCell := []Cell{}
						for _, c := range resultLeft.Data[idx] {
							joinCell = append(joinCell, c)
						}
						for _, c := range rightCell {
							joinCell = append(joinCell, c)
						}
						result.Data = append(result.Data, joinCell)
					}
				}
			}
			log.Printf("ExecuteHashJoin: result row num = %d\n", len(result.Data))
			return result, nil
		} else { // 包含其他运算符，使用loop join
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
					for _, c := range rightCell {
						joinCell = append(joinCell, c)
					}
					result.Data = append(result.Data, joinCell)
				}
			}
			log.Printf("ExecuteLoopJoin: result row num = %d\n", len(result.Data))
			return result, nil
		}
	}
}

func (e *Executor) ExecuteProject(op *plan.Operator_) (*QueryResult, error) {
	if op.OperType != plan.Project {
		return nil, fmt.Errorf("invalid operator type, get type = %d", op.OperType)
	}
	if len(op.Childs) == 0 {
		return nil, fmt.Errorf("project child is empty")
	}
	result, err := e.ExecuteFunc(op.Childs[0])
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
		log.Printf("ExecuteProject: result row num = %d\n", len(projectResult.Data))
		return projectResult, nil
	}
}

func (e *Executor) ExecuteCreateFrag(op *plan.Operator_) (*QueryResult, error) {
	if op.OperType != plan.CreateFrag {
		return nil, fmt.Errorf("invalid operator type, get type = %d", op.OperType)
	}
	if op.CreateFragOper == nil {
		return nil, fmt.Errorf("create frag operation is nil")
	}
	tx, err := e.Db.Begin()
	if err != nil {
		return nil, err
	}
	filedStr := ""
	for i, f := range op.CreateFragOper.Fields {
		if f.Type == "CHAR" {
			filedStr += fmt.Sprintf("%s %s(%d)", f.FieldName, f.Type, f.Size)
		} else {
			filedStr += fmt.Sprintf("%s %s", f.FieldName, f.Type)
		}
		if i != len(op.CreateFragOper.Fields)-1 {
			filedStr += ", "
		}
	}
	createPbTbStmt, err := tx.Prepare(fmt.Sprintf(CreateTbStr, op.CreateFragOper.TableName, filedStr))
	if err != nil {
		return nil, err
	}
	_, err = createPbTbStmt.Exec()
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (e *Executor) ExecuteInsert(op *plan.Operator_) (*QueryResult, error) {
	if op.OperType != plan.Insert {
		return nil, fmt.Errorf("invalid operator type, get type = %d", op.OperType)
	}
	if op.InsertOper == nil {
		return nil, fmt.Errorf("insert operation is nil")
	}
	wg := sync.WaitGroup{}
	resultLock := sync.Mutex{}
	errList := []error{}
	for i, _ := range op.Childs {
		wg.Add(1)
		go func(index int) {
			_, err := e.ExecuteFunc(op.Childs[index])
			resultLock.Lock()
			errList = append(errList, err)
			resultLock.Unlock()
			wg.Done()
		}(i)
	}
	wg.Wait()
	for _, err := range errList {
		if err != nil {
			return nil, err
		}
	}
	fieldStr, valueStr := "", ""
	for i, f := range op.InsertOper.Fields {
		fieldStr += f
		if i != len(op.InsertOper.Fields)-1 {
			fieldStr += ","
		}
	}
	for i, v := range op.InsertOper.Values {
		valueStr += fmt.Sprintf("'%s'", string(v))
		if i != len(op.InsertOper.Values)-1 {
			valueStr += ","
		}
	}
	tx, err := e.Db.Begin()
	if err != nil {
		return nil, err
	}
	insertStmt, err := tx.Prepare(fmt.Sprintf(InsertSqlStr, op.InsertOper.TableName, fieldStr, valueStr))
	if err != nil {
		return nil, err
	}
	_, err = insertStmt.Exec()
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (e *Executor) ExecuteDelete(op *plan.Operator_) (*QueryResult, error) {
	if op.OperType != plan.Delete {
		return nil, fmt.Errorf("invalid operator type, get type = %d", op.OperType)
	}
	if op.DeleteOper == nil {
		return nil, fmt.Errorf("delete operation is nil")
	}
	wg := sync.WaitGroup{}
	resultLock := sync.Mutex{}
	errList := []error{}
	for i, _ := range op.Childs {
		wg.Add(1)
		go func(index int) {
			_, err := e.ExecuteFunc(op.Childs[index])
			resultLock.Lock()
			errList = append(errList, err)
			resultLock.Unlock()
			wg.Done()
		}(i)
	}
	wg.Wait()
	for _, err := range errList {
		if err != nil {
			return nil, err
		}
	}
	tx, err := e.Db.Begin()
	if err != nil {
		return nil, err
	}
	deleteStmt, err := tx.Prepare(fmt.Sprintf(DeleteSqlStr, op.DeleteOper.TableName))
	if err != nil {
		return nil, err
	}
	_, err = deleteStmt.Exec()
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (e *Executor) ExecuteSelectCount(table string) (int, error) {
	tx, err := e.Db.Begin()
	if err != nil {
		return 0, err
	}
	rows, err := tx.Query(fmt.Sprintf(SelectCountSqlStr, table))
	if err != nil {
		return 0, err
	}
	dataRow := []int{0}
	pointer := make([]interface{}, 1)
	for i, _ := range dataRow {
		pointer[i] = &dataRow[i]
	}
	for rows.Next() {
		err = rows.Scan(pointer...)
		if err != nil {
			return 0, err
		} else {
			break
		}
	}
	err = tx.Commit()
	if err != nil {
		return 0, err
	}
	return dataRow[0], nil
}
