package plangenerator

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/AgentGuo/ddb/pkg/ddbclient/front/parser"
	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"
	"github.com/AgentGuo/ddb/pkg/meta"
)

func Plangenerate(ast parser.Stmt_) plan.Plantree {
	tree := plan.Plantree{}
	switch ast.Type {
	case parser.Select:
		{
			sitemap := map[string]string{}
			client := meta.Connect()
			for i := 1; i < 5; i += 1 {
				site_bi := meta.ReadPhys(client, "s"+strconv.Itoa(i), "", meta.SiteMetaType)
				var site meta.SiteMeta_
				json.Unmarshal(site_bi, &site)
				sitemap[site.Name] = site.Ip + ":" + site.Port
			}
			client.Close()
			// fmt.Printf("sitemap: %v\n", sitemap)

			join_groups := [][]plan.Operator_{}
			for _, t := range ast.SelectStmt.Tables {
				client := meta.Connect()
				data := meta.ReadLogi(client, meta.DefaultDbName, t, meta.TableMetaType)
				var table meta.TableMeta_
				json.Unmarshal(data, &table)
				client.Close()

				// a, _ := json.MarshalIndent(table.Frags, "", "  ")
				// fmt.Println("\n" + string(a) + "\n")
				if table.RouterMeta.IsVertical {
					for i := range table.Frags {
						need := false
						//假设只有单表才会用*
						if ast.SelectStmt.Fields[0].FieldName != "*" {
							for j := range ast.SelectStmt.Fields {
								if ast.SelectStmt.Fields[j].TableName == table.Name {
									for k := range table.Frags[i].Cols {
										if ast.SelectStmt.Fields[j].FieldName == table.Frags[i].Cols[k] {
											need = true
										}
									}
								}
							}
							for j := range ast.SelectStmt.ConditionUnits {
								if ast.SelectStmt.ConditionUnits[j].Lexpression.IsField && ast.SelectStmt.ConditionUnits[j].Lexpression.Field.TableName == table.Name {
									for k := range table.Frags[i].Cols {
										if ast.SelectStmt.ConditionUnits[j].Lexpression.Field.FieldName == table.Frags[i].Cols[k] {
											need = true
										}
									}
								} else if ast.SelectStmt.ConditionUnits[j].Rexpression.IsField && ast.SelectStmt.ConditionUnits[j].Rexpression.Field.TableName == table.Name {
									for k := range table.Frags[i].Cols {
										if ast.SelectStmt.ConditionUnits[j].Rexpression.Field.FieldName == table.Frags[i].Cols[k] {
											need = true
										}
									}
								}
							}
						} else {
							need = true
						}
						if need {
							join_group := []plan.Operator_{}
							temp := plan.Operator_{}
							temp.OperType = plan.Scan
							temp.Site = sitemap[table.Frags[i].SiteName]
							temp.ScanOper = &plan.ScanOper_{TableName: table.Name, Frag: table.Frags[i]}
							join_group = append(join_group, temp)
							join_groups = append(join_groups, join_group)
						}
					}
				} else {
					join_group := []plan.Operator_{}
					for i := range table.Frags {
						temp := plan.Operator_{}
						temp.OperType = plan.Scan
						temp.Site = sitemap[table.Frags[i].SiteName]
						temp.ScanOper = &plan.ScanOper_{TableName: table.Name, Frag: table.Frags[i]}
						join_group = append(join_group, temp)
					}
					join_groups = append(join_groups, join_group)
				}
			}

			project := plan.Operator_{}
			// project.Parent = tree.Root
			project.OperType = plan.Project
			project.ProjectOper = &plan.ProjectOper_{}
			predicate := plan.Operator_{}

			// predicate.Parent = &project
			predicate.OperType = plan.Predicate
			predicate.PredicateOper = &plan.PredicateOper_{}
			predicate.PredicateOper.PredConditions = append(predicate.PredicateOper.PredConditions, ast.SelectStmt.ConditionUnits...)
			if ast.SelectStmt.Fields[0].FieldName != "*" {
				project.ProjectOper.Fields = append(project.ProjectOper.Fields, ast.SelectStmt.Fields...)

				tree.Root = &project
				project.Childs = append(project.Childs, &predicate)
			} else {
				tree.Root = &predicate
			}
			genJoin(&join_groups, &predicate, ast.SelectStmt)

			if len(ast.SelectStmt.ConditionUnits) == 0 {
				if tree.Root.OperType == plan.Project {
					tree.Root.Childs[0] = predicate.Childs[0]
				} else if tree.Root.OperType == plan.Predicate {
					tree.Root = predicate.Childs[0]
				}
			}

			return tree
		}
	case parser.CreateFrag:
		{

		}
	case parser.Insert:
		{

		}
	case parser.Delete:
		{

		}
	default:
		{
			fmt.Println("To do")
		}
	}
	return plan.Plantree{}
}

func genJoin(groups *[][]plan.Operator_, root *plan.Operator_, ast *parser.SelectStmt_) {
	groups_ := *groups
	for i := range groups_ {
		if len(groups_[i]) == 1 {
			genTree(root, &groups_[i][0], ast)
		} else if len(groups_[i]) > 1 {
			union := plan.Operator_{}
			union.OperType = plan.Union
			for j := range groups_[i] {
				// groups_[i][j].Parent = &union
				union.Childs = append(union.Childs, &groups_[i][j])
			}
			genTree(root, &union, ast)
		}
	}

}

func genTree(root *plan.Operator_, new *plan.Operator_, ast *parser.SelectStmt_) {
	if len(root.Childs) == 0 {
		if root.OperType == plan.Predicate {
			root.Childs = append(root.Childs, new)
			// new.Parent = root
		} else {
			fmt.Println("error when len of children = 0")
		}
	} else if len(root.Childs) == 1 { //自己是Scan或Union或Join,new可能是Scan或Union
		if root.Childs[0].OperType == plan.Scan {
			join := plan.Operator_{}
			join.OperType = plan.Join

			left := root.Childs[0]
			right := new
			if new.OperType == plan.Union {
				right = new.Childs[0]
			}
			genJoinOperCondition(ast, left, right, &join)

			join.Childs = append(join.Childs, root.Childs[0])
			join.Childs = append(join.Childs, new)
			// root.Childs[0].Parent = &join
			// new.Parent = &join

			root.Childs[0] = &join
			// join.Parent = root
		} else if root.Childs[0].OperType == plan.Join {
			genTree(root.Childs[0], new, ast)
		} else if root.Childs[0].OperType == plan.Union {
			join := plan.Operator_{}
			join.OperType = plan.Join

			left := root.Childs[0]
			if root.Childs[0].OperType == plan.Union {
				left = root.Childs[0].Childs[0]
			}
			right := new
			if new.OperType == plan.Union {
				right = new.Childs[0]
			}
			genJoinOperCondition(ast, left, right, &join)

			join.Childs = append(join.Childs, root.Childs[0])
			join.Childs = append(join.Childs, new)
			// root.Childs[0].Parent = &join
			// new.Parent = &join

			root.Childs[0] = &join
			// join.Parent = root
		} else {
			fmt.Println("union has other one-child child")
		}
	} else if len(root.Childs) == 2 {
		if root.OperType == plan.Join {
			if root.Childs[0].OperType == plan.Scan || root.Childs[0].OperType == plan.Union {
				join := plan.Operator_{}
				join.OperType = plan.Join

				left := root.Childs[0]
				if root.Childs[0].OperType == plan.Union {
					left = root.Childs[0].Childs[0]
				}
				right := new
				if new.OperType == plan.Union {
					right = new.Childs[0]
				}
				genJoinOperCondition(ast, left, right, &join)

				join.Childs = append(join.Childs, root.Childs[0])
				join.Childs = append(join.Childs, new)
				// root.Childs[0].Parent = &join
				// new.Parent = &join

				root.Childs[0] = &join
				// join.Parent = root

			} else if root.Childs[0].OperType == plan.Join && root.Childs[1].OperType == plan.Scan || root.Childs[1].OperType == plan.Union {
				join := plan.Operator_{}
				join.OperType = plan.Join

				left := root.Childs[1]
				if root.Childs[1].OperType == plan.Union {
					left = root.Childs[1].Childs[0]
				}
				right := new
				if new.OperType == plan.Union {
					right = new.Childs[0]
				}

				genJoinOperCondition(ast, left, right, &join)

				join.Childs = append(join.Childs, root.Childs[1])
				join.Childs = append(join.Childs, new)
				// root.Childs[1].Parent = &join
				// new.Parent = &join

				root.Childs[1] = &join
				// join.Parent = root

			} else if root.Childs[0].OperType == plan.Join && root.Childs[1].OperType == plan.Join {
				if getDepth(root.Childs[0]) > getDepth(root.Childs[1]) {
					genTree(root.Childs[1], new, ast)
				} else {
					genTree(root.Childs[0], new, ast)
				}
			} else {
				fmt.Println("error when in join oper")
			}
		} else {
			fmt.Println("no-join oper has two children")
		}
	} else {
		fmt.Println("Tree has more than two children")
	}
}

func getDepth(root *plan.Operator_) int {
	if root.Childs[1].OperType == plan.Scan || root.Childs[1].OperType == plan.Union {
		return 1
	} else if root.Childs[1].OperType == plan.Join {
		return getDepth(root.Childs[1]) + 1
	} else {
		fmt.Println("error in getDepth")
		return -1
	}
}

func genJoinOperCondition(ast *parser.SelectStmt_, left *plan.Operator_, right *plan.Operator_, join *plan.Operator_) {
	join.JoinOper = &plan.JoinOper_{}
	for _, j := range ast.ConditionUnits {
		if j.Lexpression.IsField && j.Rexpression.IsField {
			if left.ScanOper.Frag.IsVertical && right.ScanOper.Frag.IsVertical {
				//默认第一个键是主键
				//这样太慢了
				client := meta.Connect()
				data := meta.ReadLogi(client, meta.DefaultDbName, left.ScanOper.TableName, meta.TableMetaType)
				var table meta.TableMeta_
				json.Unmarshal(data, &table)
				client.Close()
				Lexpression := plan.Expression_{IsField: true, Field: plan.Field_{TableName: left.ScanOper.TableName, FieldName: table.FieldMetas[0].Name}}
				Rexpression := plan.Expression_{IsField: true, Field: plan.Field_{TableName: right.ScanOper.TableName, FieldName: table.FieldMetas[0].Name}}
				join.JoinOper.JoinConditions = append(join.JoinOper.JoinConditions, plan.ConditionUnit_{Lexpression: Lexpression, Rexpression: Rexpression, CompOp: plan.Eq})
			} else {
				if j.Lexpression.Field.TableName == left.ScanOper.TableName && j.Rexpression.Field.TableName == right.ScanOper.TableName {
					Lexpression := plan.Expression_{IsField: true, Field: plan.Field_{TableName: j.Lexpression.Field.TableName, FieldName: j.Lexpression.Field.FieldName}}
					Rexpression := plan.Expression_{IsField: true, Field: plan.Field_{TableName: j.Rexpression.Field.TableName, FieldName: j.Rexpression.Field.FieldName}}
					//这里的j.CompOp默认是plan.Eq
					join.JoinOper.JoinConditions = append(join.JoinOper.JoinConditions, plan.ConditionUnit_{Lexpression: Lexpression, Rexpression: Rexpression, CompOp: j.CompOp})

				} else if j.Lexpression.Field.TableName == right.ScanOper.TableName && j.Rexpression.Field.TableName == left.ScanOper.TableName {
					Lexpression := plan.Expression_{IsField: true, Field: plan.Field_{TableName: j.Rexpression.Field.TableName, FieldName: j.Rexpression.Field.FieldName}}
					Rexpression := plan.Expression_{IsField: true, Field: plan.Field_{TableName: j.Lexpression.Field.TableName, FieldName: j.Lexpression.Field.FieldName}}
					//这里的j.CompOp默认是plan.Eq
					join.JoinOper.JoinConditions = append(join.JoinOper.JoinConditions, plan.ConditionUnit_{Lexpression: Lexpression, Rexpression: Rexpression, CompOp: j.CompOp})

				}

			}
		}
	}
}
