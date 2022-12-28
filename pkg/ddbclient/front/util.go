package front

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/AgentGuo/ddb/pkg/ddbclient/front/plan"
	"github.com/AgentGuo/ddb/pkg/meta"
	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
	"go.etcd.io/etcd/clientv3"
)

var cnt int = 0

func ShowTree(tree *plan.Plantree) {
	color := map[string]string{
		"10.77.50.214:22306":  "blue",
		"10.77.50.214:32306":  "yellow",
		"10.77.110.228:22306": "red",
		"10.77.110.158:22306": "black",
	}

	operType := map[int]string{
		plan.Project:    "project",
		plan.Predicate:  "predicate",
		plan.Union:      "union",
		plan.Join:       "join",
		plan.Scan:       "scan",
		plan.CreateFrag: "createfrag",
		plan.Insert:     "insert",
		plan.Delete:     "delete",
	}
	g := graphviz.New()
	graph, err := g.Graph()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := graph.Close(); err != nil {
			log.Fatal(err)
		}
		g.Close()
	}()

	// fmt.Println(strconv.Atoi("PRC"))
	cnt = 0
	root, _ := graph.CreateNode(strconv.Itoa(cnt) + ": " + operType[int(tree.Root.OperType)])
	cnt += 1
	root.SetColor(color[tree.Root.Site])

	if tree.Root.OperType == plan.Project {
		for j := range tree.Root.ProjectOper.Fields {
			root.SetComment(tree.Root.ProjectOper.Fields[j].TableName + ":" + tree.Root.ProjectOper.Fields[j].FieldName)
		}
	} else if tree.Root.OperType == plan.Predicate {

	} else if tree.Root.OperType == plan.Union {

	} else if tree.Root.OperType == plan.Join {

	} else if tree.Root.OperType == plan.Scan {

	}

	travelTreeInShow(root, tree.Root, graph, color, operType)
	// var buf bytes.Buffer
	// if err := g.Render(graph, "dot", &buf); err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println(buf.String())
	_, err1 := os.Stat("./graph.png")
	if err1 != nil {
		//不存在
		fmt.Println(err1)
	} else {
		//存在
		os.Remove("./graph.png")
	}
	if err := g.RenderFilename(graph, graphviz.PNG, "./graph.png"); err != nil {
		log.Fatal(err)
	}
}

func travelTreeInShow(parentNode *cgraph.Node, oper *plan.Operator_, graph *cgraph.Graph, color map[string]string, operType map[int]string) {
	if len(oper.Childs) > 0 {
		for id := range oper.Childs {
			str := strconv.Itoa(cnt) + ": " + operType[int(oper.Childs[id].OperType)]
			if oper.Childs[id].OperType == plan.Scan {
				str = strconv.Itoa(cnt) + ": " + oper.Childs[id].ScanOper.Frag.Name
			} else {

			}
			node, _ := graph.CreateNode(str)
			cnt += 1

			if oper.Childs[id].OperType == plan.Project {
				for j := range oper.Childs[id].ProjectOper.Fields {
					node.SetComment(oper.Childs[id].ProjectOper.Fields[j].TableName + ":" + oper.Childs[id].ProjectOper.Fields[j].FieldName)
				}
			} else if oper.Childs[id].OperType == plan.Predicate {

			} else if oper.Childs[id].OperType == plan.Union {

			} else if oper.Childs[id].OperType == plan.Join {

			} else if oper.Childs[id].OperType == plan.Scan {

			}

			node.SetColor(color[oper.Childs[id].Site])
			graph.CreateEdge("", parentNode, node)
			travelTreeInShow(node, oper.Childs[id], graph, color, operType)
		}
	}
}

func ShowSites() {
	sitemap := map[string]meta.SiteMeta_{}
	client := meta.Connect()
	for i := 1; i < 5; i += 1 {
		site_bi := meta.ReadPhys(client, "s"+strconv.Itoa(i), "", meta.SiteMetaType)
		var site meta.SiteMeta_
		json.Unmarshal(site_bi, &site)
		sitemap[site.Name] = site
	}
	client.Close()

	cnt := 1
	for i := range sitemap {
		fmt.Println("----------------------------------------------------------------------------")
		fmt.Printf("site[%d].Name: %v\n", cnt, sitemap[i].Name)
		fmt.Printf("site[%d].Ip: %v\n", cnt, sitemap[i].Ip)
		fmt.Printf("site[%d].Port: %v\n\n", cnt, sitemap[i].Port)
		cnt += 1
	}
}

func ShowTables() {
	tables := []meta.TableMeta_{}

	client := meta.Connect()
	kv := clientv3.NewKV(client)
	// ctx, cancel := context.WithTimeout(rootContext, time.Duration(5)*time.Second)
	ctx := context.TODO()
	response, err := kv.Get(ctx, "db/ddb/", clientv3.WithPrefix())
	if err != nil {
		fmt.Println(err)
	}

	for _, resp := range response.Kvs {
		// fmt.Printf("key: %s, value:%s\n", string(resp.Key), string(resp.Value))
		if strings.Contains(string(resp.Key), "meta") {
			var table meta.TableMeta_
			json.Unmarshal(resp.Value, &table)
			tables = append(tables, table)
		}
	}

	client.Close()

	cnt := 1
	for i := range tables {
		if tables[i].Name != "ddb" {
			fmt.Println("----------------------------------------------------------------------------")
			fmt.Printf("TableName: %v\n", tables[i].Name)
			fmt.Println("Attributes:")
			for j := range tables[i].FieldMetas {
				fmt.Printf("Name: %v\n", tables[i].FieldMetas[j].Name)
				fmt.Printf("Type: %v\n", tables[i].FieldMetas[j].Type)
				fmt.Printf("Size: %v\n\n", tables[i].FieldMetas[j].Size)
			}
			cnt += 1
		}

	}
}
func ShowFragments() {
	tables := []meta.TableMeta_{}

	client := meta.Connect()
	kv := clientv3.NewKV(client)
	// ctx, cancel := context.WithTimeout(rootContext, time.Duration(5)*time.Second)
	ctx := context.TODO()
	response, err := kv.Get(ctx, "db/ddb/", clientv3.WithPrefix())
	if err != nil {
		fmt.Println(err)
	}

	for _, resp := range response.Kvs {
		// fmt.Printf("key: %s, value:%s\n", string(resp.Key), string(resp.Value))
		if strings.Contains(string(resp.Key), "meta") {
			var table meta.TableMeta_
			json.Unmarshal(resp.Value, &table)
			tables = append(tables, table)
		}
	}

	client.Close()

	cnt := 1
	for i := range tables {
		if tables[i].Name != "ddb" {
			fmt.Println("----------------------------------------------------------------------------")
			fmt.Printf("TableName: %v\n", tables[i].Name)
			fmt.Println("Fragments:")
			for j := range tables[i].Frags {
				fmt.Printf("Name: %v\n", tables[i].Frags[j].Name)
				fmt.Printf("Site: %v\n", tables[i].Frags[j].SiteName)
				if tables[i].Frags[j].IsVertical {
					fmt.Printf("Vertical\n")
					fmt.Printf("Cols: %v\n", tables[i].Frags[j].Cols)
				} else {
					fmt.Printf("Horizontal\n")
					// fmt.Printf("Condition: %v\n", tables[i].Frags[j].Condition)
				}
				fmt.Println()
			}
			cnt += 1
		}
	}
}
