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
		args  ExecuteQTArgs
		reply *ExecuteQTReply
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
					ScanOper:      &plan.ScanOper_{TableName: "Book"},
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
								FieldName: "copies",
							},
							Value: "",
						},
						Rexpression: plan.Expression_{
							IsField: false,
							Field:   plan.Field_{},
							Value:   "7000",
						},
						CompOp: plan.Gt,
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
			args: ExecuteQTArgs{
				QT: planT1,
			},
			reply: &ExecuteQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#2 两个scan再union", args{
			args: ExecuteQTArgs{
				QT: planT2,
			},
			reply: &ExecuteQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#3 scan+小于predicate", args{
			args: ExecuteQTArgs{
				QT: planT3,
			},
			reply: &ExecuteQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#4 scan+大于predicate", args{
			args: ExecuteQTArgs{
				QT: planT4,
			},
			reply: &ExecuteQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#5 scan+project", args{
			args: ExecuteQTArgs{
				QT: planT7,
			},
			reply: &ExecuteQTReply{
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
			if err := e.ExecuteQT(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("ExecuteQT() error = %v, wantErr %v", err, tt.wantErr)
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
		args  ExecuteQTArgs
		reply *ExecuteQTReply
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

	planT8 := plan.Plantree{
		Root: &plan.Operator_{
			FragName:      "",
			Unused:        false,
			Parent:        nil,
			Childs:        nil,
			Site:          "127.0.0.1:13306",
			NeedTransfer:  false,
			DestSite:      "",
			OperType:      plan.CreateFrag,
			ScanOper:      nil,
			PredicateOper: nil,
			JoinOper:      nil,
			UnionOper:     nil,
			ProjectOper:   nil,
			CreateDbOper:  nil,
			CreateFragOper: &plan.CreateFragOper_{
				TableName: "testTb",
				Fields: []plan.FieldWithInfo{
					plan.FieldWithInfo{
						FieldName: "a",
						Size:      0,
						Type:      "INT",
					},
					plan.FieldWithInfo{
						FieldName: "b",
						Size:      0,
						Type:      "DOUBLE",
					},
					plan.FieldWithInfo{
						FieldName: "c",
						Size:      20,
						Type:      "CHAR",
					},
				},
			},
			InsertOper: nil,
			DeleteOper: nil,
		},
		OperatorNum: 0,
	}

	planT9 := plan.Plantree{
		Root: &plan.Operator_{
			FragName:       "",
			Unused:         false,
			Parent:         nil,
			Childs:         nil,
			Site:           "127.0.0.1:13306",
			NeedTransfer:   false,
			DestSite:       "",
			OperType:       plan.Insert,
			ScanOper:       nil,
			PredicateOper:  nil,
			JoinOper:       nil,
			UnionOper:      nil,
			ProjectOper:    nil,
			CreateDbOper:   nil,
			CreateFragOper: nil,
			InsertOper: &plan.InsertOper_{
				TableName: "Publisher",
				Fields:    []string{"id", "name", "nation"},
				Values:    []plan.Value_{"111", "panfeng", "CN"},
			},
			DeleteOper: nil,
		},
		OperatorNum: 0,
	}

	planT10 := plan.Plantree{
		Root: &plan.Operator_{
			FragName:       "",
			Unused:         false,
			Parent:         nil,
			Childs:         nil,
			Site:           "127.0.0.1:13306",
			NeedTransfer:   false,
			DestSite:       "",
			OperType:       plan.Delete,
			ScanOper:       nil,
			PredicateOper:  nil,
			JoinOper:       nil,
			UnionOper:      nil,
			ProjectOper:    nil,
			CreateDbOper:   nil,
			CreateFragOper: nil,
			InsertOper:     nil,
			DeleteOper:     &plan.DeleteOper_{TableName: "Publisher"},
		},
		OperatorNum: 0,
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"test#1 join测试", args{
			args: ExecuteQTArgs{
				QT: planT5,
			},
			reply: &ExecuteQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#2 join测试", args{
			args: ExecuteQTArgs{
				QT: planT6,
			},
			reply: &ExecuteQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#3 createFrag测试", args{
			args: ExecuteQTArgs{
				QT: planT8,
			},
			reply: &ExecuteQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#4 insert测试", args{
			args: ExecuteQTArgs{
				QT: planT9,
			},
			reply: &ExecuteQTReply{
				QueryResult: &QueryResult{},
			},
		}, false},
		{"test#5 delete测试", args{
			args: ExecuteQTArgs{
				QT: planT10,
			},
			reply: &ExecuteQTReply{
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
			if err := e.ExecuteQT(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("ExecuteQT() error = %v, wantErr %v", err, tt.wantErr)
			}
			fmt.Println("get result:\n", tt.args.reply.QueryResult)
		})
	}
}

func TestExecutor_GetDataNum(t *testing.T) {
	type args struct {
		args  GetDataNumArgs
		reply *GetDataNumReply
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"test#1", args{
			args:  GetDataNumArgs{Table: "Publisher"},
			reply: &GetDataNumReply{DataNum: 0},
		}, false},
		{"test#2", args{
			args:  GetDataNumArgs{Table: "Customer"},
			reply: &GetDataNumReply{DataNum: 0},
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
			if err := e.GetDataNum(tt.args.args, tt.args.reply); (err != nil) != tt.wantErr {
				t.Errorf("GetDataNum() error = %v, wantErr %v", err, tt.wantErr)
			}
			fmt.Println("get result:", tt.args.reply.DataNum)
		})
	}
}
