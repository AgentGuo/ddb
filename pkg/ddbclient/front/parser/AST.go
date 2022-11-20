package parser

type StmtType_ int64

const (
	Select = iota
	Insert
	Delete
	CreateTable
	CreateDb
	DefineSite
	DefineFragment
)

// Compare operation
type CompareType_ int

const (
	Lt = iota
	Le
	Eq
	Ge
	Gt
	Neq
)

type Field_ struct {
	TableName string
	FieldName string
}

type Value_ string

type Expression_ struct {
	IsField bool
	Field   Field_
	Value   Value_
}

type ConditionUnit_ struct {
	Lexpression Expression_
	Rexpression Expression_
	CompOp      CompareType_
}

type SelectStmt_ struct {
	Fields         []Field_
	Tables         []string
	ConditionUnits []ConditionUnit_
}

type Stmt_ struct {
	Type       StmtType_
	SelectStmt *SelectStmt_
}
