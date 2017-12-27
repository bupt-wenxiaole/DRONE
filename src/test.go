package main

import (
	"fmt"
	"graph"
	"os"
)

func main() {
	fmt.Println("run")
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
	}
}
