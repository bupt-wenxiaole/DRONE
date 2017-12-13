package main

import (
	"algorithm"
	"fmt"
	"graph"
	//"os"
	"math"
	//"strconv"
	//"unicode"
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
	graphPath := "/zpltys/graphData/subgraph.json"
	partitionPath := "/zpltys/graphData/partition.json"

	fs := tools.GenerateAlluxioClient("10.2.152.24")

	fmt.Println("start")
	f0, _ := tools.ReadFromAlluxio(fs, graphPath)
	pf0, _ := tools.ReadFromAlluxio(fs, partitionPath)
	g0, _ := graph.NewGraphFromJSON(f0, pf0, "0")
	f0.Close()
	pf0.Close()

	f1, _ := tools.ReadFromAlluxio(fs, graphPath)
	pf1, _ := tools.ReadFromAlluxio(fs, partitionPath)
	g1, _ := graph.NewGraphFromJSON(f1, pf1, "1")
	f1.Close()
	pf1.Close()

	dis0, exc0 := Generate(g0)
	dis1, exc1 := Generate(g1)

	routeTable0 := algorithm.GenerateRouteTable(g0.GetFOs())
	routeTable1 := algorithm.GenerateRouteTable(g1.GetFOs())
	/*
		for id, msgs := range routeTable1 {
			for _, msg := range msgs {
				fmt.Printf("routeTable1 : id:%v, disId:%v, routeLen:%v\n", id, msg.DstId, msg.RouteLen)
			}
		}
	*/

	continue0, messageMap0 := algorithm.SSSP_PEVal(g0, dis0, exc0, routeTable0, graph.StringID("1"))
	continue1, messageMap1 := algorithm.SSSP_PEVal(g1, dis1, exc1, routeTable1, graph.StringID("1"))

	fmt.Printf("continue1:%v\n", continue1)
	var nc0, nc1 bool
	var nmsg0, nmsg1 map[int][]*algorithm.Pair

	for continue0 || continue1 {
		fmt.Println("interation")
		for i := 0; i <= 1; i++ {
			message := make([]*algorithm.Pair, 0)
			if m0, ok := messageMap0[i]; ok {
				message = append(message, m0...)
			}
			if m1, ok := messageMap1[i]; ok {
				message = append(message, m1...)
			}

			if i == 0 {
				nc0, nmsg0 = algorithm.SSSP_IncEval(g0, dis0, exc0, routeTable0, message)
			} else {
				nc1, nmsg1 = algorithm.SSSP_IncEval(g1, dis1, exc1, routeTable1, message)
			}
		}
		continue0 = nc0
		continue1 = nc1
		messageMap0 = nmsg0
		messageMap1 = nmsg1
	}

	for id, dis := range dis0 {
		fmt.Printf("g0:  %v : %v\n", id, dis)
	}
	for id, dis := range dis1 {
		fmt.Printf("g1:  %v : %v\n", id, dis)
	}

	writePath := "/zpltys/testWrite"
	data := make([]string, 0)
	data = append(data, "ni hao")
	data = append(data, "ok")
	//fmt.Println(data[0] + data[1])
	tools.WriteToAlluxio(fs, writePath, data)
}
