/*
@author: panfengguo
@since: 2022/11/20
@desc: desc
*/
package executor

import (
	"database/sql"
	"fmt"
	"github.com/AgentGuo/ddb/cmd/ddbserver/config"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"
	"testing"
)

func TestExecutor_ExecutorQT(t *testing.T) {
	type fields struct {
		Ip   string
		Port int
		Db   *sql.DB
	}
	type args struct {
		args  ExecutorQTArgs
		reply *ExecutorQTReply
	}
	planT1 := plan.Plantree{
		Root: &plan.Operator_{
			TmpTableName:  "",
			Parent:        nil,
			Lchild:        nil,
			Rchild:        nil,
			Site:          "10.77.50.214:13306",
			NeedTransfer:  false,
			DestSite:      "",
			OperType:      plan.Scan,
			ScanOper:      &plan.ScanOper_{TableName: "Publisher"},
			PredicateOper: nil,
			JoinOper:      nil,
			UnionOper:     nil,
			ProjectOper:   nil,
		},
		OperatorNum: -1,
	}

	planT2 := plan.Plantree{
		Root: &plan.Operator_{
			TmpTableName: "",
			Parent:       nil,
			Lchild: &plan.Operator_{
				TmpTableName:  "",
				Parent:        nil,
				Lchild:        nil,
				Rchild:        nil,
				Site:          "10.77.50.214:13306",
				NeedTransfer:  false,
				DestSite:      "",
				OperType:      plan.Scan,
				ScanOper:      &plan.ScanOper_{TableName: "Publisher"},
				PredicateOper: nil,
				JoinOper:      nil,
				UnionOper:     nil,
				ProjectOper:   nil,
			},
			Rchild: &plan.Operator_{
				TmpTableName:  "",
				Parent:        nil,
				Lchild:        nil,
				Rchild:        nil,
				Site:          "10.77.50.214:13306",
				NeedTransfer:  false,
				DestSite:      "",
				OperType:      plan.Scan,
				ScanOper:      &plan.ScanOper_{TableName: "Publisher"},
				PredicateOper: nil,
				JoinOper:      nil,
				UnionOper:     nil,
				ProjectOper:   nil,
			},
			Site:          "10.77.50.214:13306",
			NeedTransfer:  false,
			DestSite:      "",
			OperType:      plan.Union,
			ScanOper:      nil,
			PredicateOper: nil,
			JoinOper:      nil,
			UnionOper: &plan.UnionOper_{
				LtableName: "",
				RtableName: "",
			},
			ProjectOper: nil,
		},
		OperatorNum: -1,
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"test#1", args{
			args: ExecutorQTArgs{
				QT: planT1,
			},
			reply: &ExecutorQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#2", args{
			args: ExecutorQTArgs{
				QT: planT2,
			},
			reply: &ExecutorQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
	}
	e := NewExecutor(&config.ServerConfig{
		ServerPort: 13306,
		ETCDPort:   -1,
		MysqlConfig: config.MysqlConfig{
			Ip:     "10.77.50.214",
			Port:   "23306",
			User:   "root",
			Passwd: "foobar",
		},
	})
	e.Ip = "10.77.50.214"
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := e.ExecutorQT(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("ExecutorQT() error = %v, wantErr %v", err, tt.wantErr)
			}
			fmt.Println("get result:\n", tt.args.reply.QueryResult)
		})
	}
}
