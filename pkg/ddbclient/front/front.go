package front

import (

	// "github.com/AgentGuo/ddb/pkg/meta"

	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/AgentGuo/ddb/cmd/ddbclient/config"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/optimizer"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/parser"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plangenerator"

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
		// ShowTree(&opt)
		fmt.Printf("opt: %v\n", opt)
		fmt.Printf("opt.Root.OperType: %v\n", opt.Root.OperType)
		fmt.Printf("opt.Root.Childs[0].OperType: %v\n", opt.Root.Childs[0].OperType)
		fmt.Printf("opt.Root.Childs[0].Childs[0].OperType: %v\n", opt.Root.Childs[0].Childs[0].OperType)
		// fmt.Printf("ppt.Root.Lchild.Lchild.Lchild: %v\n", ppt.Root.Lchild.Lchild.Lchild)
		// fmt.Println()
		// fmt.Printf("ppt.Root.Lchild.Lchild: %v\n", ppt.Root.Lchild.Lchild)
		// fmt.Println()
		// fmt.Printf("ppt.Root.Lchild: %v\n", ppt.Root.Lchild)
		// fmt.Println()
		//fmt.Printf("ppt.Root: %v\n", ppt.Root)

		// host是主executor
		// result, err := executor.RemoteExecuteQT(opt.Root.Site, &opt)
		// if err != nil {
		// 	panic(err)
		// } else {
		// 	fmt.Println(result)
		// }
	}
}
