package demo

//import "fmt"

type edge struct{
	v int       // the id of the node correspond to this edge
	weight int  // the weight or length of this edge
	nextEdge int // the index of next edge
}

type Graph struct {
	head []int     // head[i] means the index of first edge correspond to node i in edges
	edges []edge   // store all edge data
}

func GenerateGraph(n int) *Graph {
	graph := Graph{}
	graph.head = make([]int, n)
	// initial head to -1, which means there is not any edge for nodes
	for i := 0; i < n; i++ {
		graph.head[i] = -1
	}

	graph.edges = make([]edge, 0, 1)

	return &graph
}

// insert a directed edge u -> v with weight
func (g *Graph) Insert(u, v, weight int) {
	e := edge{}
	e.v = v
	e.weight = weight
	e.nextEdge = g.head[u]
	g.edges = append(g.edges, e)
	g.head[u] = len(g.edges) - 1
}

// generate all edges from node u
func (g *Graph) Query(u int) []edge {
	result := make([]edge, 0)
	for index := g.head[u]; index != -1; index = g.edges[index].nextEdge {
		result = append(result, g.edges[index])
	}

	return result
}

