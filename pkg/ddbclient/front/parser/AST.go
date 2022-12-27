package parser

import "github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"

type StmtType_ int64

const (
	Select = iota
	CreateFrag
	Insert
	Delete
)

type SelectStmt_ struct {
	Fields         []plan.Field_
	Tables         []string
	ConditionUnits []plan.ConditionUnit_
}

type FieldWithInfo struct {
	FieldName string
	Size      int64
	Type      string
}

type CreateFragStmt_ struct {
	TableName string
	SiteName  string
	Fields    []FieldWithInfo
}

type InsertStmt_ struct {
	TableName string
	Fields    []string
	Values    []plan.Value_
}

type DeleteStmt_ struct {
	TableName string
}

type Stmt_ struct {
	Type           StmtType_
	SelectStmt     *SelectStmt_
	CreateFragStmt *CreateFragStmt_
	InsertStmt     *InsertStmt_
	DeleteStmt     *DeleteStmt_
}
