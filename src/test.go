package main

import (
	"fmt"
	"graph"
	"math"
	//"log"
	"os"
	"tools"
	//"io"
	//"google.golang.org/grpc/test"
	"strings"
	//"github.com/json-iterator/go"
	"io/ioutil"
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
	//graphPath := "/GRAPE/data/G1.json"
//	partitionPath := "/GRAPE/data/P0.json"
	//graphPath := "/zpltys/graphData/text.txt"
       fs := tools.GenerateAlluxioClient("10.2.152.24")

	fmt.Println("start")
	//f0, _ := tools.ReadFromAlluxio(fs, graphPath)
  	f0, _ := os.Open("/home/xwen/G0.json")
//	pf0, _ := tools.ReadFromAlluxio(fs, partitionPath)
   // pf0, _ := os.Open("/home/zpltys/G1.json")

    data, err := ioutil.ReadAll(f0)
    if err != nil {
    	log.Fatal(err)
	}

	fmt.Printf("byte len:%v\n", len(data))
	str := string(data)
	fmt.Printf("str len:%v\n", len(str))
	s := strings.Split(str, "\n")
	fmt.Printf("slice len:%v\n", len(s))

	tools.WriteToAlluxio(fs, "/zpltys/test1.json", s)

	//fmt.Println(str)


}
