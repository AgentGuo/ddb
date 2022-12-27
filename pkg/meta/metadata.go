package meta

import "github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"

// physical meta
type SiteMeta_ struct {
	Name string
	Ip   string
	Port string
}

type FragmentMeta_ struct {
	Name      string
	SiteName  string
	TableName string
	Info      string
}

// logical meta
type FieldMeta_ struct {
	Name string
	Type string
	Size int64
	IsPK bool
}

type RouterMeta_ struct {
	IsVertical    bool
	VerticalMap   map[string]string //map[col]sitename
	HorizontalMap map[string]string //map[condition]sitename
	//目前认为一个site上不会有一个table的两个fragment
}

type TableMeta_ struct {
	Name       string
	FieldMetas []FieldMeta_
	RouterMeta RouterMeta_
	Frags      []plan.Frag_
}

type DbMeta_ struct {
	Name string
}

// meta type
type MetaType int64

const (
	SiteMetaType = iota
	FragmentMetaType
	FieldMetaType
	RouterMetaType
	TableMetaType
	DbMetaType
)
