package parser

import (
	// "github.com/jiunx/xsqlparser"

	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	// "github.com/xwb1989/sqlparser"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"
	"github.com/AgentGuo/ddb/pkg/meta"
	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

// 目前查询where没有实现or
func Parse(input string) Stmt_ {
	stmt, err := sqlparser.Parse(input)
	// fmt.Printf("stmt.(*sqlparser.Select).SelectExprs: %v\n", stmt.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName).Qualifier.FieldName.String())
	// fmt.Println(stmt.(*sqlparser.Select).Where.Expr.(*sqlparser.ComparisonExpr).Right.(*sqlparser.ColName).FieldName.String())
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
				return genInsertStmt(stmt)
			}
		case *sqlparser.Delete:
			{
				return genDeleteStmt(stmt)
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

	for i := range stmt.From {
		astSelect.Tables = append(astSelect.Tables, stmt.From[i].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String())
	}
	for i := range stmt.SelectExprs {
		switch stmtIn := stmt.SelectExprs[i].(type) {
		case *sqlparser.AliasedExpr:
			{
				//假设多表时的条件一定加上表名
				tableName := stmtIn.Expr.(*sqlparser.ColName).Qualifier.Name.String()
				if tableName == "" {
					tableName = astSelect.Tables[0]
				}

				astSelect.Fields = append(astSelect.Fields, plan.Field_{TableName: tableName, FieldName: stmtIn.Expr.(*sqlparser.ColName).Name.String()})
			}
		case *sqlparser.StarExpr:
			{
				//假设一定是单表
				astSelect.Fields = append(astSelect.Fields, plan.Field_{TableName: astSelect.Tables[0], FieldName: "*"})
			}
		}
	}
	// fmt.Printf("astSelect.Tables: %v\n", astSelect.Tables)

	if stmt.Where != nil {
		queue := []sqlparser.Expr{}
		queue_end := []sqlparser.Expr{}
		queue = append(queue, stmt.Where.Expr)
		i := 0
		j := 0
		for {
			switch s := queue[i].(type) {
			case *sqlparser.AndExpr:
				{
					queue = append(queue, s.Left)
					queue = append(queue, s.Right)
					j += 2
				}
			case *sqlparser.OrExpr:
				{
					queue = append(queue, s.Left)
					queue = append(queue, s.Right)
					j += 2
				}
			case *sqlparser.ComparisonExpr:
				{
					queue_end = append(queue_end, s)
				}
			default:
				{
					fmt.Println("unknown conditon")
				}
			}
			if i == j {
				break
			}
			i += 1
		}

		for i := range queue_end {
			condi := plan.ConditionUnit_{}
			switch queue_end[i].(*sqlparser.ComparisonExpr).Operator {
			case "<":
				{
					condi.CompOp = plan.Lt
				}
			case "<=":
				{
					condi.CompOp = plan.Le
				}
			case ">":
				{
					condi.CompOp = plan.Gt
				}
			case ">=":
				{
					condi.CompOp = plan.Ge
				}
			case "=":
				{
					condi.CompOp = plan.Eq
				}
			default:
				{
					fmt.Println("unknown comparison operator")
				}
			}
			switch s := queue_end[i].(*sqlparser.ComparisonExpr).Left.(type) {
			case *sqlparser.SQLVal:
				{
					condi.Lexpression = plan.Expression_{IsField: false, Value: plan.Value_(s.Val)}
				}
			case *sqlparser.ColName:
				{
					//假设多表时的条件一定加上表名
					tableName := s.Qualifier.Name.String()
					if tableName == "" {
						tableName = astSelect.Tables[0]
					}

					condi.Lexpression = plan.Expression_{IsField: true, Field: plan.Field_{TableName: tableName, FieldName: s.Name.String()}}
				}
			}

			switch s := queue_end[i].(*sqlparser.ComparisonExpr).Right.(type) {
			case *sqlparser.SQLVal:
				{
					condi.Rexpression = plan.Expression_{IsField: false, Value: plan.Value_(s.Val)}
				}
			case *sqlparser.ColName:
				{
					//假设多表时的条件一定加上表名
					tableName := s.Qualifier.Name.String()
					if tableName == "" {
						tableName = astSelect.Tables[0]
					}
					condi.Rexpression = plan.Expression_{IsField: true, Field: plan.Field_{TableName: tableName, FieldName: s.Name.String()}}
				}
			}

			astSelect.ConditionUnits = append(astSelect.ConditionUnits, condi)
		}

	}

	ast := Stmt_{
		Type:       Select,
		SelectStmt: &astSelect,
	}
	// a, _ := json.MarshalIndent(ast, "", "\t")
	// fmt.Println("\n" + string(a) + "\n")
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
					client := meta.Connect()
					meta.Write(client, meta.DbMetaType)
					client.Close()
				}
			case "fragment": //table与frag的顺序问题，空格问题(解决)，大小写问题(解决)
				//这里有一个问题，PRC这种外面的引号没有加，我省掉了(解决)
				{
					reg_list := regexp.MustCompile(`[(,][\.a-zA-Z0-9_!<>= \']+`)
					reg_fragname := regexp.MustCompile(`[A-Za-z\.0-9]+[ ]*[(]`)

					flist := reg_list.FindAllString(input, -1)
					for i := range flist {
						flist[i] = strings.Replace(flist[i], "(", "", -1)
						flist[i] = strings.Replace(flist[i], ",", "", -1)
						flist[i] = strings.Trim(flist[i], " ")
					}
					fragname := strings.Trim(strings.Replace(reg_fragname.FindString(input), "(", "", -1), " ")

					client := meta.Connect()
					data := meta.ReadLogi(client, meta.DefaultDbName, flist[1], meta.TableMetaType)
					var table meta.TableMeta_
					json.Unmarshal(data, &table)
					//router的map的初始化放在这里
					if table.RouterMeta.VerticalMap == nil {
						table.RouterMeta.VerticalMap = make(map[string]string)
					}
					if table.RouterMeta.HorizontalMap == nil {
						table.RouterMeta.HorizontalMap = make(map[string]string)
					}

					frag := plan.Frag_{Name: fragname, SiteName: flist[0]}
					if strings.ToLower(flist[2]) == "vp" {
						frag.IsVertical = true //new
						table.RouterMeta.IsVertical = true
						for i := 3; i < len(flist); i += 1 {
							table.RouterMeta.VerticalMap[flist[i]] = flist[0]
							frag.Cols = append(frag.Cols, flist[i]) //new
						}
					} else {
						frag.IsVertical = false                     //new
						frag.Condition = AnalyseCondition(flist[3]) //new
						table.RouterMeta.IsVertical = false
						table.RouterMeta.HorizontalMap[flist[3]] = flist[0]
					}

					table.Frags = append(table.Frags, frag)
					meta.TableMeta = table

					// a, _ := json.MarshalIndent(meta.TableMeta, "", "  ")
					// fmt.Println("\n" + string(a) + "\n")
					// fmt.Printf("meta.TableMeta: %v\n", meta.TableMeta)
					meta.Write(client, meta.TableMetaType)

					//physi
					meta.FragmentMeta.Name = fragname
					meta.FragmentMeta.SiteName = flist[0]
					meta.FragmentMeta.TableName = flist[1]
					//info?
					// fmt.Printf("meta.FragmentMeta: %v\n", meta.FragmentMeta)
					meta.Write(client, meta.FragmentMetaType) //要不要写一个删除的操作？
					client.Close()

					CreateFragStmt := CreateFragStmt_{}
					CreateFragStmt.TableName = flist[1]
					CreateFragStmt.SiteName = flist[0]
					for i := range table.FieldMetas {
						if strings.ToLower(flist[2]) == "vp" {
							for _, j := range flist[3:] {
								if table.FieldMetas[i].Name == j {
									CreateFragStmt.Fields = append(CreateFragStmt.Fields, plan.FieldWithInfo{FieldName: table.FieldMetas[i].Name, Size: table.FieldMetas[i].Size, Type: table.FieldMetas[i].Type})
								}
							}
						} else {
							CreateFragStmt.Fields = append(CreateFragStmt.Fields, plan.FieldWithInfo{FieldName: table.FieldMetas[i].Name, Size: table.FieldMetas[i].Size, Type: table.FieldMetas[i].Type})
						}
					}

					ast := Stmt_{
						Type:           CreateFrag,
						CreateFragStmt: &CreateFragStmt,
					}
					return ast
					//之前的代码
					// if strings.ToLower(strings.Trim(defList[2], " ")) == "vp" {
					// 	meta.TableMeta.RouterMeta.IsVertical = true
					// 	for i := 3; i < len(defList); i += 1 {
					// 		meta.TableMeta.RouterMeta.VerticalMap[defList[i]] = defList[0]
					// 	}
					// } else {
					// 	meta.TableMeta.RouterMeta.IsVertical = false
					// 	meta.TableMeta.RouterMeta.HorizontalMap[defList[3]] = defList[0]
					// }
					// client := meta.Connect()
					// meta.ReadLogi(client,meta.DefaultDbName,,meta.TableMetaType)
					// meta.Write(client, meta.TableMetaType)
					// client.Close()
				}
			case "site":
				{
					reg_name := regexp.MustCompile(`[A-Za-z\.0-9]+[ ]*[(]`)
					reg_ip := regexp.MustCompile(`[\d]{2,3}\.[\d]{2,3}\.[\d]{2,3}\.[\d]{2,3}`)
					reg_port := regexp.MustCompile(`:[ ]*[\d]{4,5}`)

					meta.SiteMeta.Name = strings.Trim(strings.Replace(reg_name.FindString(input), "(", "", -1), " ")
					meta.SiteMeta.Ip = reg_ip.FindString(input)
					meta.SiteMeta.Port = strings.Trim(reg_port.FindString(input), " :")
					// fmt.Printf("meta.SiteMeta.Port: %vdf\n", meta.SiteMeta.Port)
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
	//清空全局table
	meta.TableMeta = meta.TableMeta_{}

	meta.TableMeta.Name = stmt.DDL.NewName.Name.String()
	for i, j := range stmt.Columns {
		size, _ := strconv.Atoi((j.Type[strings.Index(j.Type, "(")+1 : strings.Index(j.Type, ")")]))
		meta.TableMeta.FieldMetas = append(meta.TableMeta.FieldMetas, meta.FieldMeta_{Name: j.Name, Type: string(j.Type[:strings.Index(j.Type, "(")]), Size: int64(size), IsPK: i == 0})
	}
	fmt.Printf("meta.TableMeta: %v\n", meta.TableMeta)
	client := meta.Connect()
	meta.Write(client, meta.TableMetaType)
	client.Close()
	// //先把router的map的初始化放在这里，之后再改
	// meta.TableMeta.RouterMeta.VerticalMap = make(map[string]string)
	// meta.TableMeta.RouterMeta.HorizontalMap = make(map[string]string)
	return Stmt_{}
}
func genInsertStmt(stmt *sqlparser.Insert) Stmt_ {
	InsertStmt := InsertStmt_{}
	InsertStmt.TableName = stmt.Table.Name.String()
	for i := range stmt.Columns {
		InsertStmt.Fields = append(InsertStmt.Fields, stmt.Columns[i].String())
	}
	buf := sqlparser.NewTrackedBuffer(nil)
	// buf.Myprintf("%v",
	// 	stmt.Rows)
	stmt.Rows.Format(buf)
	// fmt.Printf("buf.String(): %v\n", buf.String())
	reg_insert_pre := regexp.MustCompile(`\(.*\)`)
	numlist_pre := reg_insert_pre.FindString(buf.String())
	// fmt.Printf("numlist_pre: %v\n", numlist_pre)

	reg_insert := regexp.MustCompile(`[0-9a-zA-Z\' ]+`)
	numlist := reg_insert.FindAllString(numlist_pre, -1)
	for i := range numlist {
		numlist[i] = strings.Replace(numlist[i], "'", "", -1)
		numlist[i] = strings.Trim(numlist[i], " ")
	}
	// fmt.Printf("numlist: %v\n", numlist)
	for i := range numlist {
		InsertStmt.Values = append(InsertStmt.Values, plan.Value_(numlist[i]))
	}
	ast := Stmt_{
		Type:       Insert,
		InsertStmt: &InsertStmt,
	}
	return ast
}
func genDeleteStmt(stmt *sqlparser.Delete) Stmt_ {
	DeleteStmt := DeleteStmt_{}
	DeleteStmt.TableName = stmt.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()
	ast := Stmt_{
		Type:       Delete,
		DeleteStmt: &DeleteStmt,
	}
	return ast
}

// 这里有一个问题，PRC这种外面的引号没有加，我省掉了(解决)
// 水平分片条件没有实现or
func AnalyseCondition(con string) []plan.ConditionUnit_ {
	cons := []plan.ConditionUnit_{}
	tree, _ := sqlparser.Parse("select * from t where " + con)
	expr := tree.(*sqlparser.Select).Where.Expr

	stmt := &sqlparser.Select{}
	stmt.AddWhere(expr)
	if stmt.Where != nil {
		queue := []sqlparser.Expr{}
		queue_end := []sqlparser.Expr{}
		queue = append(queue, stmt.Where.Expr)
		i := 0
		j := 0
		for {
			switch s := queue[i].(type) {
			case *sqlparser.AndExpr:
				{
					queue = append(queue, s.Left)
					queue = append(queue, s.Right)
					j += 2
				}
			case *sqlparser.OrExpr:
				{
					queue = append(queue, s.Left)
					queue = append(queue, s.Right)
					j += 2
				}
			case *sqlparser.ComparisonExpr:
				{
					queue_end = append(queue_end, s)
				}
			default:
				{
					fmt.Println("unknown conditon")
				}
			}
			if i == j {
				break
			}
			i += 1
		}

		for i := range queue_end {
			condi := plan.ConditionUnit_{}
			switch queue_end[i].(*sqlparser.ComparisonExpr).Operator {
			case "<":
				{
					condi.CompOp = plan.Lt
				}
			case "<=":
				{
					condi.CompOp = plan.Le
				}
			case ">":
				{
					condi.CompOp = plan.Gt
				}
			case ">=":
				{
					condi.CompOp = plan.Ge
				}
			case "=":
				{
					condi.CompOp = plan.Eq
				}
			default:
				{
					fmt.Println("unknown comparison operator")
				}
			}
			switch s := queue_end[i].(*sqlparser.ComparisonExpr).Left.(type) {
			case *sqlparser.SQLVal:
				{
					condi.Lexpression = plan.Expression_{IsField: false, Value: plan.Value_(s.Val)}
				}
			case *sqlparser.ColName:
				{
					condi.Lexpression = plan.Expression_{IsField: true, Field: plan.Field_{TableName: s.Qualifier.Name.String(), FieldName: s.Name.String()}}
				}
			}

			switch s := queue_end[i].(*sqlparser.ComparisonExpr).Right.(type) {
			case *sqlparser.SQLVal:
				{
					condi.Rexpression = plan.Expression_{IsField: false, Value: plan.Value_(s.Val)}
				}
			case *sqlparser.ColName:
				{
					condi.Rexpression = plan.Expression_{IsField: true, Field: plan.Field_{TableName: s.Qualifier.Name.String(), FieldName: s.Name.String()}}
				}
			}
			cons = append(cons, condi)
		}

	}
	return cons
}
