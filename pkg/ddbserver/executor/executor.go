package executor

import (
	"fmt"
	"github.com/AgentGuo/ddb/cmd/ddbserver/config"
	_ "github.com/go-sql-driver/mysql"
)

func RunExecutor(config *config.ServerConfig) {
	fmt.Println("i am executor hello world")
	//db, err := sql.Open("mysql", "root:foobar@tcp(127.0.0.1:23306)/ddb")
	//if err != nil {
	//	panic(err)
	//}
	//// See "Important settings" section.
	//db.SetConnMaxLifetime(time.Minute * 3)
	//db.SetMaxOpenConns(10)
	//db.SetMaxIdleConns(10)
}
