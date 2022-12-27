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
	ExecuteQT   = "ExecuteQT"
)

type ExecuteQTArgs struct {
	QT plan.Plantree
}

type ExecuteQTReply struct {
	QueryResult *QueryResult
}

type GetDataNumArgs struct {
	Table string
}

type GetDataNumReply struct {
	DataNum int
}

type ExecutorService interface {
	ExecuteQT(args ExecuteQTArgs, reply *ExecuteQTReply) error
	GetDataNum(args GetDataNumArgs, reply *GetDataNumReply) error
}

func RegisterService(service ExecutorService) error {
	return rpc.RegisterName(ExecutorSvc, service)
}
