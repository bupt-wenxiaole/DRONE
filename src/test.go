package main

import (
	"fmt"
	"graph"
	"os"
)

func main() {
	fmt.Println("run")
	/*
	patternFile, err := os.Open("test_data\\pattern.txt.tmp")
	if err != nil {
		fmt.Println(err)
	}
	g, _ := graph.NewPatternGraph(patternFile)

	for id := range g.GetNodes() {
		source, _ := g.GetSources(id)
		for srcId := range source {
			fmt.Printf("%v is source of %v\n", srcId, id)
		}
	}*/
	G, _ := os.Open("test_data\\G.0")
	FI, _ := os.Open("test_data\\F0.I")
	FO, _ := os.Open("test_data\\F0.O")

	g, _ := graph.NewGraphFromTXT(G, FI, FO, "0")

	for id, msg := range g.GetFOs() {
		fmt.Println("---------------")
		fmt.Println(id)
		for _, m := range msg {
			fmt.Println(m.RelatedId(), m.RelatedWgt(), m.RoutePartition())
		}
	}
}
