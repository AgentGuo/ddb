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
			Parent:        nil,
			Childs:        nil,
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
			Parent: nil,
			Childs: []*plan.Operator_{
				&plan.Operator_{
					Parent:        nil,
					Childs:        nil,
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
				&plan.Operator_{
					Parent:        nil,
					Childs:        nil,
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

	planT3 := plan.Plantree{
		Root: &plan.Operator_{
			Parent: nil,
			Childs: []*plan.Operator_{
				&plan.Operator_{
					Parent:        nil,
					Childs:        nil,
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
			},
			Site:         "10.77.50.214:13306",
			NeedTransfer: false,
			DestSite:     "",
			OperType:     plan.Predicate,
			ScanOper:     nil,
			PredicateOper: &plan.PredicateOper_{
				PredConditions: []plan.ConditionUnit_{
					plan.ConditionUnit_{
						Lexpression: plan.Expression_{
							IsField: true,
							Field: plan.Field_{
								TableName: "",
								FieldName: "id",
							},
							Value: "",
						},
						Rexpression: plan.Expression_{
							IsField: false,
							Field:   plan.Field_{},
							Value:   "100008",
						},
						CompOp: plan.Lt,
					},
				},
			},
			JoinOper:    nil,
			UnionOper:   nil,
			ProjectOper: nil,
		},
		OperatorNum: -1,
	}

	planT4 := plan.Plantree{
		Root: &plan.Operator_{
			Parent: nil,
			Childs: []*plan.Operator_{
				&plan.Operator_{
					Parent:        nil,
					Childs:        nil,
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
			},
			Site:         "10.77.50.214:13306",
			NeedTransfer: false,
			DestSite:     "",
			OperType:     plan.Predicate,
			ScanOper:     nil,
			PredicateOper: &plan.PredicateOper_{
				PredConditions: []plan.ConditionUnit_{
					plan.ConditionUnit_{
						Lexpression: plan.Expression_{
							IsField: true,
							Field: plan.Field_{
								TableName: "",
								FieldName: "name",
							},
							Value: "",
						},
						Rexpression: plan.Expression_{
							IsField: false,
							Field:   plan.Field_{},
							Value:   "Publisher #100009",
						},
						CompOp: plan.Le,
					},
				},
			},
			JoinOper:    nil,
			UnionOper:   nil,
			ProjectOper: nil,
		},
		OperatorNum: -1,
	}

	planT7 := plan.Plantree{
		Root: &plan.Operator_{
			Parent: nil,
			Childs: []*plan.Operator_{
				&plan.Operator_{
					Parent:        nil,
					Childs:        nil,
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
			},

			Site:          "10.77.50.214:13306",
			NeedTransfer:  false,
			DestSite:      "",
			OperType:      plan.Project,
			ScanOper:      nil,
			PredicateOper: nil,
			JoinOper:      nil,
			UnionOper:     nil,
			ProjectOper: &plan.ProjectOper_{Fields: []plan.Field_{
				plan.Field_{
					TableName: "Publisher",
					FieldName: "name",
				},
				plan.Field_{
					TableName: "Publisher",
					FieldName: "nation",
				},
			}},
		},
		OperatorNum: -1,
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"test#1 单个scan", args{
			args: ExecutorQTArgs{
				QT: planT1,
			},
			reply: &ExecutorQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#2 两个scan再union", args{
			args: ExecutorQTArgs{
				QT: planT2,
			},
			reply: &ExecutorQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#3 scan+小于predicate", args{
			args: ExecutorQTArgs{
				QT: planT3,
			},
			reply: &ExecutorQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#4 scan+小于等于predicate", args{
			args: ExecutorQTArgs{
				QT: planT4,
			},
			reply: &ExecutorQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#5 scan+project", args{
			args: ExecutorQTArgs{
				QT: planT7,
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

func TestExecutor_ExecutorQT1(t *testing.T) {
	type fields struct {
		Ip   string
		Port int
		Db   *sql.DB
	}
	type args struct {
		args  ExecutorQTArgs
		reply *ExecutorQTReply
	}
	planT5 := plan.Plantree{
		Root: &plan.Operator_{
			Parent: nil,
			Childs: []*plan.Operator_{
				&plan.Operator_{
					Parent:        nil,
					Childs:        nil,
					Site:          "127.0.0.1:13306",
					NeedTransfer:  false,
					DestSite:      "",
					OperType:      plan.Scan,
					ScanOper:      &plan.ScanOper_{TableName: "Customer"},
					PredicateOper: nil,
					JoinOper:      nil,
					UnionOper:     nil,
					ProjectOper:   nil,
				},
				&plan.Operator_{
					Parent:        nil,
					Childs:        nil,
					Site:          "127.0.0.1:13306",
					NeedTransfer:  false,
					DestSite:      "",
					OperType:      plan.Scan,
					ScanOper:      &plan.ScanOper_{TableName: "Customer"},
					PredicateOper: nil,
					JoinOper:      nil,
					UnionOper:     nil,
					ProjectOper:   nil,
				},
			},
			Site:          "127.0.0.1:13306",
			NeedTransfer:  false,
			DestSite:      "",
			OperType:      plan.Join,
			ScanOper:      nil,
			PredicateOper: nil,
			JoinOper: &plan.JoinOper_{
				LtableName: "",
				RtableName: "",
				JoinConditions: []plan.ConditionUnit_{
					plan.ConditionUnit_{
						Lexpression: plan.Expression_{
							IsField: true,
							Field: plan.Field_{
								TableName: "Customer",
								FieldName: "id",
							},
							Value: "",
						},
						Rexpression: plan.Expression_{
							IsField: true,
							Field: plan.Field_{
								TableName: "Customer",
								FieldName: "id",
							},
							Value: "",
						},
						CompOp: plan.Eq,
					},
				},
			},
			UnionOper:   nil,
			ProjectOper: nil,
		},
		OperatorNum: -1,
	}

	planT6 := plan.Plantree{
		Root: &plan.Operator_{
			Parent: nil,
			Childs: []*plan.Operator_{
				&plan.Operator_{
					Parent:        nil,
					Childs:        nil,
					Site:          "10.77.50.214:22306",
					NeedTransfer:  false,
					DestSite:      "",
					OperType:      plan.Scan,
					ScanOper:      &plan.ScanOper_{TableName: "Customer"},
					PredicateOper: nil,
					JoinOper:      nil,
					UnionOper:     nil,
					ProjectOper:   nil,
				},
				&plan.Operator_{
					Parent:        nil,
					Childs:        nil,
					Site:          "10.77.50.214:32306",
					NeedTransfer:  false,
					DestSite:      "",
					OperType:      plan.Scan,
					ScanOper:      &plan.ScanOper_{TableName: "Customer"},
					PredicateOper: nil,
					JoinOper:      nil,
					UnionOper:     nil,
					ProjectOper:   nil,
				},
			},
			Site:          "127.0.0.1:13306",
			NeedTransfer:  false,
			DestSite:      "",
			OperType:      plan.Join,
			ScanOper:      nil,
			PredicateOper: nil,
			JoinOper: &plan.JoinOper_{
				LtableName: "",
				RtableName: "",
				JoinConditions: []plan.ConditionUnit_{
					plan.ConditionUnit_{
						Lexpression: plan.Expression_{
							IsField: true,
							Field: plan.Field_{
								TableName: "Customer",
								FieldName: "id",
							},
							Value: "",
						},
						Rexpression: plan.Expression_{
							IsField: true,
							Field: plan.Field_{
								TableName: "Customer",
								FieldName: "id",
							},
							Value: "",
						},
						CompOp: plan.Eq,
					},
				},
			},
			UnionOper:   nil,
			ProjectOper: nil,
		},
		OperatorNum: -1,
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"test#1 ", args{
			args: ExecutorQTArgs{
				QT: planT5,
			},
			reply: &ExecutorQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#2", args{
			args: ExecutorQTArgs{
				QT: planT6,
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
	e.Ip = "127.0.0.1"
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := e.ExecutorQT(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("ExecutorQT() error = %v, wantErr %v", err, tt.wantErr)
			}
			fmt.Println("get result:\n", tt.args.reply.QueryResult)
		})
	}
}
