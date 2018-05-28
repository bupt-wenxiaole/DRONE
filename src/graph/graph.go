package graph

import (
	"bytes"
	//"github.com/json-iterator/go"
	"fmt"
	"io"
	"sync"
	"encoding/json"
	"log"
	"bufio"
	"strings"
	"strconv"
	"tools"
)

// ID is unique identifier.
type ID interface {
	// String returns the string ID.
	String() string
	IntVal() int64
}

type StringID int64

func (s StringID) String() string {
	return strconv.FormatInt(int64(s), 10)
}

func (s StringID) IntVal() int64 {
	return int64(s)
}

// Node is vertex. The ID must be unique within the graph.
type Node interface {
	// ID returns the ID.
	ID() ID
	String() string
	Attr() int64
}

type node struct {
	id   int64
	attr int64
}

//var nodeCnt uint64

func NewNode(id int64, attr int64) Node {
	return &node{
		id:   id,
		attr: attr,
	}
}

func (n *node) ID() ID {
	return StringID(n.id)
}

func (n *node) String() string {
	return strconv.FormatInt(n.id, 10)
}

func (n *node) Attr() int64 {
	return n.attr
}

// Graph describes the methods of graph operations.
// It assumes that the identifier of a Node is unique.
// And weight values is float64.
type Graph interface {
	// Init initializes a Graph.
	Init()

	// GetNodeCount returns the total number of nodes.
	GetNodeCount() int

	// GetNode finds the Node. It returns nil if the Node
	// does not exist in the graph.
	GetNode(id ID) Node

	// GetNodes returns a map from node ID to
	// empty struct value. Graph does not allow duplicate
	// node ID or name.
	GetNodes() map[ID]Node

	// AddNode adds a node to a graph, and returns false
	// if the node already existed in the graph.
	AddNode(nd Node) bool

	// DeleteNode deletes a node from a graph.
	// It returns true if it got deleted.
	// And false if it didn't get deleted.
	DeleteNode(id ID) bool

	// AddEdge adds an edge from nd1 to nd2 with the weight.
	// It returns error if a node does not exist.
	AddEdge(id1, id2 ID, weight float64) error

	// ReplaceEdge replaces an edge from id1 to id2 with the weight.
	ReplaceEdge(id1, id2 ID, weight float64) error

	// DeleteEdge deletes an edge from id1 to id2.
	DeleteEdge(id1, id2 ID) error

	// GetWeight returns the weight from id1 to id2.
	GetWeight(id1, id2 ID) (float64, error)

	// GetSources returns the map of parent Nodes.
	// (Nodes that come towards the argument vertex.)
	GetSources(id ID) (map[ID]Node, error)

	// GetTargets returns the map of child Nodes.
	// (Nodes that go out of the argument vertex.)
	GetTargets(id ID) (map[ID]Node, error)

	// get all Fi.I message
	GetFIs() map[ID][]RouteMsg

	// get all Fi.O message
	GetFOs() map[ID][]RouteMsg

	// String describes the Graph.
	String() string
}

// graph is an internal default graph type that
// implements all methods in Graph interface.
type graph struct {
	mu sync.RWMutex // guards the following

	// idToNodes stores all nodes.
	idToNodes map[ID]Node

	// nodeToSources maps a Node identifer to sources(parents) with edge weights.
	nodeToSources map[ID]map[ID]float64

	// nodeToTargets maps a Node identifer to targets(children) with edge weights.
	nodeToTargets map[ID]map[ID]float64

	// store Fi.I, Fi.O of graph i
	graphFI map[ID][]RouteMsg
	graphFO map[ID][]RouteMsg
}

// newGraph returns a new graph.
func newGraph() *graph {
	return &graph{
		idToNodes:     make(map[ID]Node),
		nodeToSources: make(map[ID]map[ID]float64),
		nodeToTargets: make(map[ID]map[ID]float64),
		graphFI:       make(map[ID][]RouteMsg),
		graphFO:       make(map[ID][]RouteMsg),
		//
		// without this
		// panic: assignment to entry in nil map
	}
}

// NewGraph returns a new graph.
func NewGraph() Graph {
	return newGraph()
}

func (g *graph) Init() {
	// (X) g = newGraph()
	// this only updates the pointer
	//
	//
	// (X) *g = *newGraph()
	// assignment copies lock value

	g.idToNodes = make(map[ID]Node)
	g.nodeToSources = make(map[ID]map[ID]float64)
	g.nodeToTargets = make(map[ID]map[ID]float64)
	g.graphFI = make(map[ID][]RouteMsg)
	g.graphFO = make(map[ID][]RouteMsg)
}

func (g *graph) GetNodeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return len(g.idToNodes)
}

func (g *graph) GetNode(id ID) Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.idToNodes[id]
}

func (g *graph) GetNodes() map[ID]Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.idToNodes
}

func (g *graph) unsafeExistID(id ID) bool {
	_, ok := g.idToNodes[id]
	return ok
}

func (g *graph) AddNode(nd Node) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.unsafeExistID(nd.ID()) {
		return false
	}

	id := nd.ID()
	g.idToNodes[id] = nd
	return true
}

//TODO this function's implement needs profit
func (g *graph) DeleteNode(id ID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.unsafeExistID(id) {
		return false
	}

	delete(g.idToNodes, id)

	delete(g.nodeToTargets, id)
	for _, smap := range g.nodeToTargets {
		delete(smap, id)
	}

	delete(g.nodeToSources, id)
	for _, smap := range g.nodeToSources {
		delete(smap, id)
	}

	return true
}

func (g *graph) AddEdge(id1, id2 ID, weight float64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.nodeToTargets[id1]; ok {
		if v, ok2 := g.nodeToTargets[id1][id2]; ok2 {
			g.nodeToTargets[id1][id2] = v + weight
		} else {
			g.nodeToTargets[id1][id2] = weight
		}
	} else {
		tmap := make(map[ID]float64)
		tmap[id2] = weight
		g.nodeToTargets[id1] = tmap
	}
	if _, ok := g.nodeToSources[id2]; ok {
		if v, ok2 := g.nodeToSources[id2][id1]; ok2 {
			g.nodeToSources[id2][id1] = v + weight
		} else {
			g.nodeToSources[id2][id1] = weight
		}
	} else {
		tmap := make(map[ID]float64)
		tmap[id1] = weight
		g.nodeToSources[id2] = tmap
	}

	return nil
}

func (g *graph) ReplaceEdge(id1, id2 ID, weight float64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.unsafeExistID(id1) {
		return fmt.Errorf("%s does not exist in the graph.", id1)
	}
	if !g.unsafeExistID(id2) {
		return fmt.Errorf("%s does not exist in the graph.", id2)
	}

	if _, ok := g.nodeToTargets[id1]; ok {
		g.nodeToTargets[id1][id2] = weight
	} else {
		tmap := make(map[ID]float64)
		tmap[id2] = weight
		g.nodeToTargets[id1] = tmap
	}
	if _, ok := g.nodeToSources[id2]; ok {
		g.nodeToSources[id2][id1] = weight
	} else {
		tmap := make(map[ID]float64)
		tmap[id1] = weight
		g.nodeToSources[id2] = tmap
	}
	return nil
}

func (g *graph) DeleteEdge(id1, id2 ID) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.unsafeExistID(id1) {
		return fmt.Errorf("%s does not exist in the graph.", id1)
	}
	if !g.unsafeExistID(id2) {
		return fmt.Errorf("%s does not exist in the graph.", id2)
	}

	if _, ok := g.nodeToTargets[id1]; ok {
		if _, ok := g.nodeToTargets[id1][id2]; ok {
			delete(g.nodeToTargets[id1], id2)
		}
	}
	if _, ok := g.nodeToSources[id2]; ok {
		if _, ok := g.nodeToSources[id2][id1]; ok {
			delete(g.nodeToSources[id2], id1)
		}
	}
	return nil
}

func (g *graph) GetWeight(id1, id2 ID) (float64, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if !g.unsafeExistID(id1) {
		return 0, fmt.Errorf("%s does not exist in the graph.", id1)
	}
	if !g.unsafeExistID(id2) {
		return 0, fmt.Errorf("%s does not exist in the graph.", id2)
	}

	if _, ok := g.nodeToTargets[id1]; ok {
		if v, ok := g.nodeToTargets[id1][id2]; ok {
			return v, nil
		}
	}
	return 0, fmt.Errorf("there is no edge from %s to %s", id1, id2)
}

func (g *graph) GetSources(id ID) (map[ID]Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if !g.unsafeExistID(id) {
		return nil, fmt.Errorf("%s does not exist in the graph.", id)
	}

	rs := make(map[ID]Node)
	if _, ok := g.nodeToSources[id]; ok {
		for n := range g.nodeToSources[id] {
			rs[n] = g.idToNodes[n]
		}
	}
	return rs, nil
}

func (g *graph) GetTargets(id ID) (map[ID]Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if !g.unsafeExistID(id) {
		return nil, fmt.Errorf("%s does not exist in the graph.", id)
	}

	rs := make(map[ID]Node)
	if _, ok := g.nodeToTargets[id]; ok {
		for n := range g.nodeToTargets[id] {
			rs[n] = g.idToNodes[n]
		}
	}
	return rs, nil
}

func (g *graph) String() string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	buf := new(bytes.Buffer)
	for id1, nd1 := range g.idToNodes {
		nmap, _ := g.GetTargets(id1)
		for id2, nd2 := range nmap {
			weight, _ := g.GetWeight(id1, id2)
			fmt.Fprintf(buf, "%s -- %d -→ %s\n", nd1, weight, nd2)
		}
	}
	return buf.String()
}

func (g *graph) GetFIs() map[ID][]RouteMsg {
	return g.graphFI
}

func (g *graph) GetFOs() map[ID][]RouteMsg {
	return g.graphFO
}

// NewGraphFromJSON returns a new Graph from a JSON file.
// Here's the sample JSON data:
//
//	{
//	    "graph_00": {
//	        "S": {
//	            "A": 100,
//	            "B": 14,
//	            "C": 200
//	        },
//	        "A": {
//	            "S": 15,
//	            "B": 5,
//	            "D": 20,
//	            "T": 44
//	        },
//	        "B": {
//	            "S": 14,
//	            "A": 5,
//	            "D": 30,
//	            "E": 18
//	        },
//	        "C": {
//	            "S": 9,
//	            "E": 24
//	        },
//	        "D": {
//	            "A": 20,
//	            "B": 30,
//	            "E": 2,
//	            "F": 11,
//	            "T": 16
//	        },
//	        "E": {
//	            "B": 18,
//	            "C": 24,
//	            "D": 2,
//	            "F": 6,
//	            "T": 19
//	        },
//	        "F": {
//	            "D": 11,
//	            "E": 6,
//	            "T": 6
//	        },
//	        "T": {
//	            "A": 44,
//	            "D": 16,
//	            "F": 6,
//	            "E": 19
//	        }
//	    },
//	}
//

// pattern graph should be constructed as the format
// NodeId type numberOfSuffixNodes id1 id2 id3 ...

func NewPatternGraph(rd io.Reader) (Graph, error) {
	buffer := bufio.NewReader(rd)

	g := newGraph()
	for {
		line, err := buffer.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}

		line = line[0 : len(line) - 1]
		msg := strings.Split(line, " ")
		nodeId, _ := strconv.Atoi(msg[0])
		attr, _ := strconv.Atoi(msg[1])
		node := NewNode(int64(nodeId), int64(attr))
		g.AddNode(node)

		num, _ := strconv.Atoi(msg[2])
		for i := 3; i < num + 3; i += 1 {
			v, _ := strconv.Atoi(msg[i])
			g.AddEdge(StringID(nodeId), StringID(v), 1)
		}

	}

	return g, nil
}

func NewGraphFromJSON(rd io.Reader, partitonReader io.Reader, graphID string) (Graph, error) {
	js := make(map[string]map[StringID]map[string]float64)

	//var json = jsoniter.ConfigCompatibleWithStandardLibrary

	dec := json.NewDecoder(rd)
	for {
		if err := dec.Decode(&js); err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("load graph error")
			log.Fatal(err)
			return nil, err
		}
	}
	if _, ok := js["Graph"+graphID]; !ok {
		return nil, fmt.Errorf("%s does not exist", graphID)
	}
	gmap := js["Graph"+graphID]

	g := newGraph()
	for id1, mm := range gmap {

		nd1 := g.GetNode(StringID(id1))
		if nd1 == nil {
			intId := id1
			nd1 = NewNode(int64(id1), int64(intId % tools.GraphSimulationTypeModel))
			if ok := g.AddNode(nd1); !ok {
				return nil, fmt.Errorf("%s already exists", nd1)
			}
		}
		for id2, weight := range mm {
			if id2 == "attr" {
				g.idToNodes[StringID(id1)] = NewNode(int64(id1), int64(weight))
			} else {
				id2Int, _ := strconv.Atoi(id2)
				nd2 := g.GetNode(StringID(id2Int))
				if nd2 == nil {
					intId, _ := strconv.Atoi(id2)
					nd2 = NewNode(int64(id2Int), int64(intId % tools.GraphSimulationTypeModel))
					if ok := g.AddNode(nd2); !ok {
						return nil, fmt.Errorf("%s already exists", nd2)
					}
				}
				g.ReplaceEdge(nd1.ID(), nd2.ID(), weight)
			}
		}
	}

	graphFI, graphFO, err := LoadRouteMsgFromJson(partitonReader, graphID)
	if err != nil {
		return nil, err
	}
	g.graphFI = graphFI
	g.graphFO = graphFO

	return g, nil
}

func NewGraphFromTXT(rd io.Reader, fxird io.Reader, fxord io.Reader, graphID string) (Graph, error) {
	g := newGraph()
	reader := bufio.NewReader(rd)
	for {
		line, err := reader.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		paras := strings.Split(strings.Split(line, "\n")[0], " ")

		parseSrc, err := strconv.ParseInt(paras[0], 10, 64)
		if err != nil {
			log.Fatal("parse src node id error")
		}
		parseDst, err := strconv.ParseInt(paras[1], 10, 64)
		if err != nil {
			log.Fatal("parse dst node id error")
		}

		srcId := StringID(parseSrc)
		dstId := StringID(parseDst)

		weight, err := strconv.ParseFloat(paras[3], 64)
		if err != nil {
			fmt.Println("zs-log: " + paras[3])
			log.Fatal("parse weight error")
		}
		//weight := 0.0

		nd1 := g.GetNode(srcId)
		if nd1 == nil {
			intId := srcId.IntVal()
			nd1 = NewNode(intId, int64(intId%tools.GraphSimulationTypeModel))
			if ok := g.AddNode(nd1); !ok {
				return nil, fmt.Errorf("%s already exists", nd1)
			}
		}
		nd2 := g.GetNode(dstId)
		if nd2 == nil {
			nd2 = NewNode(dstId.IntVal(), int64(dstId.IntVal()%tools.GraphSimulationTypeModel))
			if ok := g.AddNode(nd2); !ok {
				return nil, fmt.Errorf("%s already exists", nd2)
			}
		}
		g.ReplaceEdge(nd1.ID(), nd2.ID(), weight)
	}


	graphFI, err1 := LoadRouteMsgFromTxt(fxird, false, g)
	if err1 != nil {
		return nil, err1
	}
	graphFO, err2 := LoadRouteMsgFromTxt(fxord, true, g)
	if err2 != nil {
		return nil, err2
	}
	g.graphFI = graphFI
	g.graphFO = graphFO

	return g, nil

}
