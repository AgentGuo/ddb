package meta

// physical meta
type SiteMeta_ struct {
	Name string
	Ip   string
	Port string
}

type FragmentMeta_ struct {
	Name          string
	SiteName      string
	TableName     string
	IsVertical    bool //horizontal时为false
	FragCondition string
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
	VerticalMap   map[string]string
	HorizontalMap map[string]string
}

type TableMeta_ struct {
	Name       string
	FieldMetas []FieldMeta_
	RouterMeta RouterMeta_
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
