package executor

import (
	"fmt"
	"github.com/AgentGuo/ddb/cmd/ddbserver/config"
	_ "github.com/go-sql-driver/mysql"
)

func RunExecutor(config *config.ServerConfig) {
	fmt.Println("i am executor hello world")
	executor := NewExecutor(config)
	executor.server()
	fmt.Println("[Enter \"q\" to stop]")
	quit := ""
	for quit != "q" {
		fmt.Scanf("%s", &quit)
	}
}
