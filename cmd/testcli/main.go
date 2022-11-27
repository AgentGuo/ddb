/*
@author: panfengguo
@since: 2022/11/19
@desc: desc
*/
package main

import (
	"fmt"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"
	"github.com/AgentGuo/ddb/pkg/ddbserver/executor"
	"os"
)

func main() {
	host := ""
	args := os.Args
	if len(args) <= 1 {
		panic("args lens should >= 0")
	}
	host = args[1]
	//planT1 := &plan.Plantree{
	//	Root: &plan.Operator_{
	//		TmpTableName: "",
	//		Parent:       nil,
	//		Lchild: &plan.Operator_{
	//			TmpTableName:  "",
	//			Parent:        nil,
	//			Lchild:        nil,
	//			Rchild:        nil,
	//			Site:          "10.77.50.214:22306",
	//			NeedTransfer:  false,
	//			DestSite:      "",
	//			OperType:      plan.Scan,
	//			ScanOper:      &plan.ScanOper_{TableName: "Publisher"},
	//			PredicateOper: nil,
	//			JoinOper:      nil,
	//			UnionOper:     nil,
	//			ProjectOper:   nil,
	//		},
	//		Rchild: &plan.Operator_{
	//			TmpTableName:  "",
	//			Parent:        nil,
	//			Lchild:        nil,
	//			Rchild:        nil,
	//			Site:          "10.77.50.214:32306",
	//			NeedTransfer:  false,
	//			DestSite:      "",
	//			OperType:      plan.Scan,
	//			ScanOper:      &plan.ScanOper_{TableName: "Publisher"},
	//			PredicateOper: nil,
	//			JoinOper:      nil,
	//			UnionOper:     nil,
	//			ProjectOper:   nil,
	//		},
	//		Site:          "10.77.50.214:22306",
	//		NeedTransfer:  false,
	//		DestSite:      "",
	//		OperType:      plan.Union,
	//		ScanOper:      nil,
	//		PredicateOper: nil,
	//		JoinOper:      nil,
	//		UnionOper: &plan.UnionOper_{
	//			LtableName: "",
	//			RtableName: "",
	//		},
	//		ProjectOper: nil,
	//	},
	//	OperatorNum: -1,
	//}

	//planT2 := &plan.Plantree{
	//	Root: &plan.Operator_{
	//		TmpTableName:  "",
	//		Parent:        nil,
	//		Lchild:        nil,
	//		Rchild:        nil,
	//		Site:          "10.77.50.214:22306",
	//		NeedTransfer:  false,
	//		DestSite:      "",
	//		OperType:      plan.Scan,
	//		ScanOper:      &plan.ScanOper_{TableName: "Publisher"},
	//		PredicateOper: nil,
	//		JoinOper:      nil,
	//		UnionOper:     nil,
	//		ProjectOper:   nil,
	//	},
	//	OperatorNum: -1,
	//}

	planT1 := &plan.Plantree{
		Root: &plan.Operator_{
			TmpTableName: "",
			Parent:       nil,
			Lchild: &plan.Operator_{
				TmpTableName:  "",
				Parent:        nil,
				Lchild:        nil,
				Rchild:        nil,
				Site:          "10.77.50.214:22306",
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
				TmpTableName: "",
				Parent:       nil,
				Lchild: &plan.Operator_{
					TmpTableName:  "",
					Parent:        nil,
					Lchild:        nil,
					Rchild:        nil,
					Site:          "10.77.50.214:32306",
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
					TmpTableName: "",
					Parent:       nil,
					Lchild: &plan.Operator_{
						TmpTableName:  "",
						Parent:        nil,
						Lchild:        nil,
						Rchild:        nil,
						Site:          "10.77.110.228:22306",
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
						Site:          "10.77.110.158:22306",
						NeedTransfer:  false,
						DestSite:      "",
						OperType:      plan.Scan,
						ScanOper:      &plan.ScanOper_{TableName: "Publisher"},
						PredicateOper: nil,
						JoinOper:      nil,
						UnionOper:     nil,
						ProjectOper:   nil,
					},
					Site:          "10.77.110.228:22306",
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
				Site:          "10.77.50.214:32306",
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
			Site:          "10.77.50.214:22306",
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
	result, err := executor.RemoteExecuteQT(host, planT1)
	if err != nil {
		panic(err)
	} else {
		fmt.Println(result)
	}
}
