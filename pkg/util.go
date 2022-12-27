package main

import (
	"log"

	"github.com/goccy/go-graphviz"
)

func main() {
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
	n, _ := graph.CreateNode("n")

	m, _ := graph.CreateNode("m")

	m1, _ := graph.CreateNode("m1")

	o1, _ := graph.CreateNode("o1")
	// o1.SetColor("blue")
	// o1.SetColorScheme("blue")
	o1.SetFillColor("red")
	o2, _ := graph.CreateNode("o2")
	graph.CreateEdge("e", m, o1)
	graph.CreateEdge("e", m, o2)

	e, err := graph.CreateEdge("e", n, m)
	if err != nil {
		log.Fatal(err)
	}
	e.SetLabel("e")

	e1, err := graph.CreateEdge("e1", n, m1)
	if err != nil {
		log.Fatal(err)
	}
	e1.SetLabel("e1")
	// var buf bytes.Buffer
	// if err := g.Render(graph, "dot", &buf); err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println(buf.String())
	if err := g.RenderFilename(graph, graphviz.PNG, "./graph.png"); err != nil {
		log.Fatal(err)
	}
}
