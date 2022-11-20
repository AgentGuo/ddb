/*
@author: panfengguo
@since: 2022/11/19
@desc: desc
*/
package executor

import (
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"
	"net/rpc"
)

const (
	ExecutorSvc = "ExecutorService"
	ExecutorQT  = "ExecutorQT"
)

type ExecutorQTArgs struct {
	QT plan.Plantree
}

type ExecutorQTReply struct {
	QueryResult *QueryResult
}

type ExecutorService interface {
	ExecutorQT(args ExecutorQTArgs, reply *ExecutorQTReply) error
}

func RegisterService(service ExecutorService) error {
	return rpc.RegisterName(ExecutorSvc, service)
}
