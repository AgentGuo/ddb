package parser

import (
	// "github.com/jiunx/xsqlparser"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	// "github.com/xwb1989/sqlparser"
	"github.com/AgentGuo/ddb/pkg/meta"
	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

func Parse(input string) Stmt_ {
	stmt, err := sqlparser.Parse(input)
	// fmt.Printf("stmt.(*sqlparser.Select).SelectExprs: %v\n", stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Qualifier.Name.String())
	// fmt.Println(stmt.(*sqlparser.Select).Where.Expr.(*sqlparser.ComparisonExpr).Right.(*sqlparser.ColName).Name.String())
	// fmt.Println(stmt.(*sqlparser.Select).Where.Expr.(*sqlparser.AndExpr).Left.(*sqlparser.ComparisonExpr).Operator)
	if err != nil {
		// fmt.Println("sqlparser error")
		return DDL(input)
	} else {
		switch stmt := stmt.(type) {
		case *sqlparser.Select:
			{
				return genSelectStmt(stmt)
			}
		case *sqlparser.CreateTable:
			{
				return genCreateTableStmt(stmt)
			}
		case *sqlparser.Insert:
			{
				fmt.Println("To do")
			}
		case *sqlparser.Delete:
			{
				fmt.Println("To do")
			}
		default:
			{
				fmt.Println("To do")
			}
		}
	}
	return Stmt_{}
}

func genSelectStmt(stmt *sqlparser.Select) Stmt_ {
	astSelect := SelectStmt_{}
	for i := range stmt.SelectExprs {
		switch stmtIn := stmt.SelectExprs[i].(type) {
		case *sqlparser.AliasedExpr:
			{
				astSelect.Fields = append(astSelect.Fields, Field_{stmtIn.Expr.(*sqlparser.ColName).Qualifier.Name.String(), stmtIn.Expr.(*sqlparser.ColName).Name.String()})
			}
		case *sqlparser.StarExpr:
			{
				astSelect.Fields = append(astSelect.Fields, Field_{"", "*"})
			}
		}
	}
	for i := range stmt.From {
		astSelect.Tables = append(astSelect.Tables, stmt.From[i].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String())
	}
	ast := Stmt_{
		Select,
		&astSelect,
	}
	a, _ := json.Marshal(ast)
	fmt.Println("\n" + string(a) + "\n")
	return ast
}
func DDL(input string) Stmt_ {

	//create database d;
	//create site a(10.77.50.214:23306)
	//create fragment a(s,t,vp/hp,cols/condition)
	//create fragment f0(s0,t0,hp,customer_id<307000);
	//create fragment f0(s0,t0,vp,id,io);
	str := strings.Split(input, " ")
	switch strings.ToLower(strings.Trim(str[0], " ")) {
	case "create":
		{
			switch strings.ToLower(strings.Trim(str[1], " ")) {
			case "database":
				{
					meta.DbMeta.Name = str[2][:len(str[2])-1]
					// fmt.Printf("meta.DbMeta: %v\n", meta.DbMeta)
					client := meta.Connect()
					// defer client.Close()
					meta.Write(client, meta.DbMetaType)
					client.Close()
				}
			case "fragment": //暂定//table与frag的顺序问题，空格问题，大小写问题
				{
					defList := strings.Split(str[2][strings.Index(str[2], "(")+1:len(str[2])-2], ",")
					if strings.ToLower(strings.Trim(defList[2], " ")) == "vp" {
						meta.TableMeta.RouterMeta.IsVertical = true
						for i := 3; i < len(defList); i += 1 {
							meta.TableMeta.RouterMeta.VerticalMap[defList[i]] = defList[0]
						}
					} else {
						meta.TableMeta.RouterMeta.IsVertical = false
						meta.TableMeta.RouterMeta.HorizontalMap[defList[3]] = defList[0]
					}
					client := meta.Connect()
					meta.Write(client, meta.TableMetaType)
					client.Close()
				}
			case "site":
				{
					defList := strings.Split(str[2][strings.Index(str[2], "(")+1:len(str[2])-2], ":")
					meta.SiteMeta.Name = str[2][0:strings.Index(str[2], "(")]
					meta.SiteMeta.Ip = defList[0]
					meta.SiteMeta.Port = defList[1]
					// fmt.Printf("meta.SiteMeta: %v\n", meta.SiteMeta)
					client := meta.Connect()
					meta.Write(client, meta.SiteMetaType)
					client.Close()
				}
			default:
				{
					fmt.Println("other unknown self-define create input")
				}
			}
		}
	default:
		{
			fmt.Println("other unknown self-define input")
		}
	}
	return Stmt_{}
}

func genCreateTableStmt(stmt *sqlparser.CreateTable) Stmt_ {
	// fmt.Printf("stmt.Columns: %v\n", stmt.Columns[0].Type)
	// fmt.Printf("stmt.DDL: %v\n", stmt.DDL.NewName.Name)
	meta.TableMeta.Name = stmt.DDL.NewName.Name.String()
	for i, j := range stmt.Columns {
		size, _ := strconv.Atoi((j.Type[strings.Index(j.Type, "(")+1 : strings.Index(j.Type, ")")]))
		meta.TableMeta.FieldMetas = append(meta.TableMeta.FieldMetas, meta.FieldMeta_{Name: j.Name, Type: string(j.Type[:strings.Index(j.Type, "(")]), Size: int64(size), IsPK: i == 0})
	}
	fmt.Printf("meta.TableMeta: %v\n", meta.TableMeta)
	client := meta.Connect()
	meta.Write(client, meta.TableMetaType)
	client.Close()
	//先把router的map的初始化放在这里，之后再改
	meta.TableMeta.RouterMeta.VerticalMap = make(map[string]string)
	meta.TableMeta.RouterMeta.HorizontalMap = make(map[string]string)
	return Stmt_{}
}
