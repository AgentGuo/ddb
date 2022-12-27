package plan

// OperatorType
type OperatorType_ int

const (
	Scan = iota
	Predicate
	Join
	Union
	Project

	CreateDb
	CreateFrag
	Insert
	Delete
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

type Frag_ struct {
	Name       string
	SiteName   string
	IsVertical bool
	Cols       []string
	Condition  []ConditionUnit_
}

// Operators
type ScanOper_ struct {
	TableName string
	Frag      Frag_ //仅优化时使用，执行时不使用
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

type CreateDbOper_ struct {
	DbName string
}

type FieldWithInfo struct {
	FieldName string
	Size      int64
	Type      string
}

type CreateFragOper_ struct {
	TableName string
	Fields    []FieldWithInfo
}

type InsertOper_ struct {
	TableName string
	Fields    []string
	Values    []Value_
}

type DeleteOper_ struct {
	TableName string
	// DeleteConditions []ConditionUnit_
}

type Operator_ struct {
	FragName       string
	Unused         bool
	Parent         *Operator_
	Childs         []*Operator_
	Site           string
	NeedTransfer   bool   // 算子是否需要传输数据
	DestSite       string // 数据传输地址
	OperType       OperatorType_
	ScanOper       *ScanOper_
	PredicateOper  *PredicateOper_
	JoinOper       *JoinOper_
	UnionOper      *UnionOper_
	ProjectOper    *ProjectOper_
	CreateDbOper   *CreateDbOper_
	CreateFragOper *CreateFragOper_
	InsertOper     *InsertOper_
	DeleteOper     *DeleteOper_
}

type Plantree struct {
	Root        *Operator_
	OperatorNum int64
}
