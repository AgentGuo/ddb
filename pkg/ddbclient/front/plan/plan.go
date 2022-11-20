package plan

// OperatorType
type OperatorType_ int

const (
	Scan = iota
	Predicate
	Join
	Union
	Project
	// Insert
	// Delete
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

// Some other types
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

// Operators
type ScanOper_ struct {
	TableName string
}

type PredicateOper_ struct {
	PredConditions []ConditionUnit_
}

type JoinOper_ struct {
	LtableName     string
	RtableName     string
	JoinConditions []ConditionUnit_
}

type UnionOper_ struct {
	LtableName string
	RtableName string
}

type ProjectOper_ struct {
	Fields []Field_
}

// type InsertOper_ struct {
// 	Fields []Field_
// 	Values []Value_
// }

// type DeleteOper_ struct {
// 	TableName        string
// 	DeleteConditions []ConditionUnit_
// }

type Operator_ struct {
	TmpTableName  string
	Parent        *Operator_
	Lchild        *Operator_
	Rchild        *Operator_
	Site          string
	NeedTransfer  bool   // 算子是否需要传输数据
	DestSite      string // 数据传输地址
	OperType      OperatorType_
	ScanOper      *ScanOper_
	PredicateOper *PredicateOper_
	JoinOper      *JoinOper_
	UnionOper     *UnionOper_
	ProjectOper   *ProjectOper_
	// InsertOper    InsertOper_
	// DeleteOper    DeleteOper_
}

type Plantree struct {
	Root        *Operator_
	OperatorNum int64
}
