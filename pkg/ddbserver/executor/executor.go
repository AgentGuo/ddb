/*
@author: panfengguo
@since: 2022/11/19
@desc: desc
*/
package executor

import (
	"database/sql"
	"fmt"
	"github.com/AgentGuo/ddb/cmd/ddbserver/config"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"
	"github.com/AgentGuo/ddb/utils"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net"
	"net/rpc"
)

const (
	driverType = "mysql"
	dbFmtAddr  = "root:foobar@tcp(%s:%s)/ddb?charset=utf8&multiStatements=true"
)

type Executor struct {
	Ip   string  // executor服务ip
	Port int     // executor服务端口
	Db   *sql.DB // executor本地db
}

func (e *Executor) ExecuteQT(args ExecuteQTArgs, reply *ExecuteQTReply) error {
	result, err := e.ExecuteFunc(args.QT.Root)
	if err != nil {
		log.Println(err)
		return err
	}
	(*reply).QueryResult = result
	if (*reply).QueryResult != nil {
		log.Printf("ExecuteQT: result row num = %d\n", len((*reply).QueryResult.Data))
	}
	return nil
}

func (e *Executor) GetDataNum(args GetDataNumArgs, reply *GetDataNumReply) error {
	result, err := e.ExecuteSelectCount(args.Table)
	if err != nil {
		log.Println(err)
		return err
	}
	(*reply).DataNum = result
	return nil
}

func (e *Executor) getSite() string {
	return fmt.Sprintf("%s:%d", e.Ip, e.Port)
}

func (e *Executor) ExecuteFunc(op *plan.Operator_) (*QueryResult, error) {
	if op == nil {
		return nil, nil
	} else if op.Site != e.getSite() {
		planT := &plan.Plantree{
			Root:        op,
			OperatorNum: -1,
		}
		return RemoteExecuteQT(op.Site, planT)
	} else {
		switch op.OperType {
		case plan.Scan:
			return e.ExecuteScan(op)
		case plan.Union:
			return e.ExecuteUnion(op)
		case plan.Predicate:
			return e.ExecutePredicate(op)
		case plan.Join:
			return e.ExecuteJoin(op)
		case plan.Project:
			return e.ExecuteProject(op)
		case plan.CreateFrag:
			return e.ExecuteCreateFrag(op)
		case plan.Insert:
			return e.ExecuteInsert(op)
		case plan.Delete:
			return e.ExecuteDelete(op)
		default:
			return nil, fmt.Errorf("op = %d not implemented", op.OperType)
		}
	}
}

func (e *Executor) server() {
	err := RegisterService(e)
	if err != nil {
		return
	}
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", e.Ip, e.Port))
	if err != nil {
		log.Fatal("listen error:", err)
		return
	} else {
		log.Printf("listening: %d", e.Port)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Fatal("listen accept error:", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()
}

func NewExecutor(config *config.ServerConfig) *Executor {
	ip, err := utils.GetOutBoundIP()
	log.Printf("executor server at: %s:%d", ip, config.ServerPort)
	if err != nil {
		panic(err)
	}
	db, err := sql.Open(driverType, fmt.Sprintf(dbFmtAddr, config.MysqlConfig.Ip, config.MysqlConfig.Port))
	if err != nil {
		panic(err)
	}
	db.SetConnMaxLifetime(-1)
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	return &Executor{
		Ip:   ip,
		Port: config.ServerPort,
		Db:   db,
	}
}
