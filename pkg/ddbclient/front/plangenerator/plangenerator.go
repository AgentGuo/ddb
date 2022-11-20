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

			for _, t := range ast.SelectStmt.Tables {
				client := meta.Connect()
				data := meta.ReadLogi(client, meta.DefaultDbName, t, meta.TableMetaType)
				var table meta.TableMeta_
				json.Unmarshal(data, &table)
				client.Close()
				// fmt.Printf("table.RouterMeta: %v\n", table.RouterMeta)
				if table.RouterMeta.IsVertical {
					fmt.Println("To do for join")
				} else {
					var unions []plan.Operator_
					for i := 0; i < len(table.RouterMeta.HorizontalMap)-1; i += 1 {
						temp := plan.Operator_{}
						temp.OperType = plan.Union
						unions = append(unions, temp)
					}
					tree.Root = &unions[len(table.RouterMeta.HorizontalMap)-2]
					tree.OperatorNum = -1

					var scans []plan.Operator_
					//hp时scan oper不需要关注condition
					for _, j := range table.RouterMeta.HorizontalMap {
						temp := plan.Operator_{}
						temp.Site = sitemap[j]
						temp.OperType = plan.Scan
						temp.ScanOper = &plan.ScanOper_{TableName: table.Name}
						scans = append(scans, temp)
					}
					unions[0].Lchild = &scans[0]
					unions[0].Rchild = &scans[1]
					unions[0].Site = scans[1].Site

					scans[0].Parent = &unions[0]
					scans[0].NeedTransfer = true
					scans[0].DestSite = scans[1].Site

					scans[1].Parent = &unions[0]

					for i := 1; i < len(table.RouterMeta.HorizontalMap)-1; i += 1 {
						unions[i].Lchild = &unions[i-1]
						unions[i].Rchild = &scans[i+1]
						unions[i].Site = scans[i+1].Site

						unions[i-1].Parent = &unions[i]
						unions[i-1].NeedTransfer = true
						unions[i-1].DestSite = scans[i+1].Site

						scans[i+1].Parent = &unions[i]
					}
				}

			}
			// fmt.Printf("tree.Root: %v\n", tree.Root)
			return tree
		}
	default:
		{
			fmt.Println("To do")
		}
	}
	return plan.Plantree{}
}
