package algorithm

import (
	"fmt"
	"graph"
	"os"
)

func main()  {
	f, err := os.Open("C:\\Users\\zpltys\\code\\GRAPE\\test_data\\subgraph.json")
	//println(err)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	g, err := graph.NewGraphFromJSON(f, "Graph0")
}



