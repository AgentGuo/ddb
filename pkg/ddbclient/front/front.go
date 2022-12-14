package front

import (

	// "github.com/AgentGuo/ddb/pkg/meta"

	"fmt"
	"github.com/AgentGuo/ddb/pkg/ddbserver/executor"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/AgentGuo/ddb/cmd/ddbclient/config"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/optimizer"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/parser"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plangenerator"
	"github.com/AgentGuo/ddb/pkg/meta"

	"golang.org/x/crypto/ssh/terminal"
)

func RunFront(config *config.ClientConfig) {
	oldTermState, err := terminal.MakeRaw(syscall.Stdin)
	if err != nil {
		fmt.Println(err)
		return
	}

	term := terminal.NewTerminal(os.Stdin, ">")
	rawState, err := terminal.GetState(syscall.Stdin)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		cmdline, err := term.ReadLine()
		if err != nil {
			break
		}
		cmdline = strings.TrimSpace(cmdline)
		if cmdline == "" {
			continue
		}
		if cmdline == "exit" {
			break
		}
		terminal.Restore(syscall.Stdin, oldTermState)
		frontfunc(cmdline)
		terminal.Restore(syscall.Stdin, rawState)
	}

	terminal.Restore(syscall.Stdin, oldTermState)
	fmt.Println("")

}

func frontfunc(input string) {
	switch input {
	case "show sites":
		{
			ShowSites()
		}
	case "delete all etcd":
		{
			client := meta.Connect()
			meta.DeleteAll(client)
			client.Close()

		}
	case "show tables":
		{
			ShowTables()
		}
	case "show fragments":
		{
			ShowFragments()
		}
	default:
		{
			ast := parser.Parse(input)
			null := parser.Stmt_{}
			if ast == null {
				// fmt.Println("DDL")
			} else if ast.Type == parser.Select {
				// fmt.Printf("ast: %v\n", ast)
				ppt := plangenerator.Plangenerate(ast)
				// fmt.Printf("ppt: %v\n", ppt)
				ShowTree(&ppt)

				opt := optimizer.Optimize(ppt)
				// opt.Root = opt.Root.Childs[0].Childs[0].Childs[0].Childs[0].Childs[0]
				// for i := range opt.Root.Childs[0].Childs[0].ProjectOper.Fields {
				// 	fmt.Printf("opt.Root.Childs[0].Childs[0].ProjectOper.Fields[i].TableName: %v\n", opt.Root.Childs[0].Childs[0].ProjectOper.Fields[i].TableName)
				// 	fmt.Printf("opt.Root.Childs[0].Childs[0].ProjectOper.Fields[i].FieldName: %v\n", opt.Root.Childs[0].Childs[0].ProjectOper.Fields[i].FieldName)
				// }
				ShowTree(&opt)
				// fmt.Printf("opt: %v\n", opt)
				// fmt.Printf("opt.Root.OperType: %v\n", opt.Root.OperType)
				// fmt.Printf("opt.Root.Childs[0].OperType: %v\n", opt.Root.Childs[0].OperType)
				// fmt.Printf("opt.Root.Childs[0].Childs[0].OperType: %v\n", opt.Root.Childs[0].Childs[0].OperType)
				// fmt.Printf("opt.Root.Childs[0].Childs[0].Childs[0].OperType: %v\n", opt.Root.Childs[0].Childs[0].Childs[0].OperType)
				// now := opt.Root
				// cnt := 1
				// for {
				// 	fmt.Printf("now.OperType: %v\n", now.OperType)
				// 	if len(now.Childs) == 0 {
				// 		break
				// 	} else {
				// 		now = now.Childs[0]
				// 	}
				// 	fmt.Printf("cnt: %v\n", cnt)
				// 	cnt += 1

				// }
				// fmt.Printf("ppt.Root.Lchild.Lchild.Lchild: %v\n", ppt.Root.Lchild.Lchild.Lchild)
				// fmt.Println()
				// fmt.Printf("ppt.Root.Lchild.Lchild: %v\n", ppt.Root.Lchild.Lchild)
				// fmt.Println()
				// fmt.Printf("ppt.Root.Lchild: %v\n", ppt.Root.Lchild)
				// fmt.Println()
				//fmt.Printf("ppt.Root: %v\n", ppt.Root)

				// ????????????????????????
				//dataNum, err := executor.RemoteGetDataNum("10.77.50.214:22306", "Publisher")
				//if err != nil {
				//	panic(err)
				//} else {
				//	fmt.Println(dataNum)
				//}
				// host??????executor
				start := time.Now()
				result, err := executor.RemoteExecuteQT(opt.Root.Site, &opt)
				if err != nil {
					fmt.Printf("Query failed, err = %s\n", err)
				} else {
					elapsed := time.Since(start)
					result.PrintResult()
					fmt.Printf("Query ok, %s\n", elapsed)
				}
			} else {
				ppt := plangenerator.Plangenerate(ast)
				//ShowTree(&ppt)
				start := time.Now()
				if ppt.Root != nil {
					result, err := executor.RemoteExecuteQT(ppt.Root.Site, &ppt)
					if err != nil {
						fmt.Printf("Query failed, err = %s\n", err)
					} else {
						elapsed := time.Since(start)
						result.PrintResult()
						fmt.Printf("Query ok, %s\n", elapsed)
					}
				}
			}
		}
	}
}
