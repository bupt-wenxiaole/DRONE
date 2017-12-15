package main

import (
	//"algorithm"
	"fmt"
	"graph"
	//"os"
	"math"
	//"strconv"
	//"unicode"
	//"tools"
	"os"
	"log"
	"tools"
)

func Generate(g graph.Graph) (map[graph.ID]int64, map[graph.ID]int64) {
	distance := make(map[graph.ID]int64)
	exchangeMsg := make(map[graph.ID]int64)

	for id := range g.GetNodes() {
		distance[id] = math.MaxInt64
	}

	for id := range g.GetFOs() {
		exchangeMsg[id] = math.MaxInt64
	}
	return distance, exchangeMsg
}

// This example creates a PriorityQueue with some items, adds and manipulates an item,
// and then removes the items in priority order.
func main() {
	graphPath := "/GRAPE/data/G0.json"
	partitionPath := "/GRAPE/data/P0.json"

	fs := tools.GenerateAlluxioClient("10.2.152.24")

	fmt.Println("start")
	f0, _ := tools.ReadFromAlluxio(fs, graphPath)
	pf0, _ := tools.ReadFromAlluxio(fs, partitionPath)
	g, err := graph.NewGraphFromJSON(f0, pf0, "0")
	if err != nil {
		log.Fatal(err)
	}
    fmt.Println("finish")

	for fo, _ := range g.GetFOs() {
		fmt.Printf("fo: %v \n", fo)
	}

	f0.Close()
	pf0.Close()

}
