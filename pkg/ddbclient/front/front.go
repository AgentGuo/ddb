package front

import (
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"
	// "github.com/AgentGuo/ddb/pkg/meta"
	"fmt"
	"github.com/AgentGuo/ddb/cmd/ddbclient/config"
)

func RunParser(config *config.ClientConfig) {
	sql := "select"
	var ss plan.Plantree
	fmt.Print(sql)
	fmt.Printf("ss.OperatorNum: %v\n", ss.OperatorNum)
	// fmt.Printf("ss.Root.ProjectOper.Fields[0].FieldName: %v\n", ss.Root.ProjectOper.Fields[0].FieldName)
	// s1 := meta.DbMeta_{}
}
