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

func RemoteExecuteQT(host string, planT *plan.Plantree) (*QueryResult, error) {
	reply := &ExecuteQTReply{}
	err := call(host, ExecuteQT, ExecuteQTArgs{QT: *planT}, reply)
	if err != nil {
		return nil, err
	}
	return reply.QueryResult, nil
}

func RemoteGetDataNum(site, table string) (int, error) {
	reply := &GetDataNumReply{}
	err := call(site, GetDataNum, GetDataNumArgs{Table: table}, reply)
	if err != nil {
		return 0, err
	}
	return reply.DataNum, nil
}

func call(host string, rpcName string, args interface{}, reply interface{}) error {
	client, err := rpc.Dial("tcp", host)
	if err != nil {
		return err
	}
	err = client.Call(ExecutorSvc+"."+rpcName, args, reply)
	if err != nil {
		return err
	}
	client.Close()
	return nil
}
