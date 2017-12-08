package main

import (
	"fmt"
	. "graph"
	"os"
	"strconv"
)

func main() {

	subGraphDir := "C:\\Users\\zpltys\\code\\GRAPE\\test_data\\subgraph.json"
	partitionDir := "C:\\Users\\zpltys\\code\\GRAPE\\test_data\\partition.json"

	f, err := os.Open(subGraphDir)
	//println(err)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	pf, err := os.Open(partitionDir)
	g, err := NewGraphFromJSON(f, pf, "0")
	if err != nil {
		panic(err)
	}
	fmt.Println(g.String())

	for srcId, msg := range g.GetFIs() {
		fmt.Println("FI: " + srcId.String())
		for i := 0; i < len(msg); i++ {
			fmt.Println("nodeId:" + msg[i].RelatedId().String() + "  partitionId:" + strconv.Itoa(msg[i].RoutePartition()))
		}
	}

	for srcId, msg := range g.GetFOs() {
		fmt.Println("FO: " + srcId.String())
		for i := 0; i < len(msg); i++ {
			fmt.Println("nodeId:" + msg[i].RelatedId().String() + "  partitionId:" + strconv.Itoa(msg[i].RoutePartition()))
		}
	}

}
