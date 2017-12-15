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
	graphPath := "/home/zpltys/G1.json"
	partitionPath := "/home/zpltys/P1.json"

	//fs := tools.GenerateAlluxioClient("10.2.152.24")

	fmt.Println("start")
	f0, _ := os.Open(graphPath)
	pf0, _ := os.Open(partitionPath)
	g, err := graph.NewGraphFromJSON(f0, pf0, "0")
	if err != nil {
		log.Fatal(err)
	}

	for fo, _ := range g.GetFOs() {
		fmt.Printf("fo: %v \n", fo)
	}

	f0.Close()
	pf0.Close()

}
