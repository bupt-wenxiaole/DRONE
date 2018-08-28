package graph

import (
	//"github.com/json-iterator/go"
	"fmt"
	"io"
	"sync"
	"log"
	"bufio"
	"strings"
	"strconv"
	"tools"
)

// Node is vertex. The int64 must be unique within the graph.
type Node interface {
	// int64 returns the int64.
	int64() int64
	String() string
	Attr() int64
}

type node struct {
	id   int64
	attr int64
}

func NewNode(id int64, attr int64) Node {
	return &node{
		id:   id,
		attr: attr,
	}
}

func (n *node) int64() int64 {
	return n.id
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
	GetNode(id int64) Node

	// GetNodes returns a map from node int64 to
	// empty struct value. Graph does not allow duplicate
	// node int64 or name.
	GetNodes() map[int64]Node

	// AddNode adds a node to a graph, and returns false
	// if the node already existed in the graph.
	AddNode(nd Node) bool

	DeleteNode(id int64)

	// AddEdge adds an edge from nd1 to nd2 with the weight.
	// It returns error if a node does not exist.
	AddEdge(id1, id2 int64, weight float64) error

    IsMaster(id int64) bool
    IsMirror(id int64) bool

	AddMirror(id int64, masterWR int)

	GetMirrors() map[int64]int

	AddMaster(id int64, routeMsg []int)

	GetMasters() map[int64][]int

    //GetWeight returns the weight from id1 to id2.
	GetWeight(id1, id2 int64) (float64, error)

	// GetSources returns the map of parent Nodes.
	// (Nodes that come towards the argument vertex.)
	GetSources(id int64) map[int64]float64

	// GetTargets returns the map of child Nodes.
	// (Nodes that go out of the argument vertex.)
	GetTargets(id int64) map[int64]float64

}

// graph is an internal default graph type that
// implements all methods in Graph interface.
type graph struct {
	mu sync.RWMutex // guards the following

	// idToNodes stores all nodes.
	idToNodes map[int64]Node

	// master vertices
	masterWorkers map[int64][]int

	// mirror vertices
	mirrorWorker map[int64]int

	// nodeToSources maps a Node identifer to sources(parents) with edge weights.
	nodeToSources map[int64]map[int64]float64

	// nodeToTargets maps a Node identifer to targets(children) with edge weights.
	nodeToTargets map[int64]map[int64]float64

	useTargets bool
}

// newGraph returns a new graph.
func newGraph() *graph {
	return &graph{
		idToNodes:     make(map[int64]Node),
		nodeToSources: make(map[int64]map[int64]float64),
		nodeToTargets: make(map[int64]map[int64]float64),
		masterWorkers: make(map[int64][]int),
		mirrorWorker:  make(map[int64]int),
	}
}


func (g *graph) Init() {
	g.idToNodes = make(map[int64]Node)
	g.nodeToSources = make(map[int64]map[int64]float64)
	g.nodeToTargets = make(map[int64]map[int64]float64)
	g.masterWorkers = make(map[int64][]int)
	g.mirrorWorker = make(map[int64]int)
}

func (g *graph) GetNodeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return len(g.idToNodes)
}

func (g *graph) GetNode(id int64) Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.idToNodes[id]
}

func (g *graph) DeleteNode(id int64) {
	delete(g.idToNodes, id)

	for an := range g.nodeToSources[id] {
		delete(g.nodeToTargets[an], id)
	}
	for an := range g.nodeToTargets[id] {
		delete(g.nodeToSources[an], id)
	}
	delete(g.nodeToSources, id)
	delete(g.nodeToTargets, id)

	delete(g.mirrorWorker, id)
	delete(g.masterWorkers, id)
}

func (g *graph) GetNodes() map[int64]Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.idToNodes
}

func (g *graph) unsafeExistint64(id int64) bool {
	_, ok := g.idToNodes[id]
	return ok
}

func (g *graph) AddNode(nd Node) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.unsafeExistint64(nd.int64()) {
		return false
	}

	id := nd.int64()
	g.idToNodes[id] = nd
	return true
}

func (g *graph) AddMaster(id int64, routeMsg []int) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.masterWorkers[id] = routeMsg
}

func (g *graph) AddMirror(id int64, masterWR int) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.mirrorWorker[id] = masterWR
}

func (g *graph) AddEdge(id1, id2 int64, weight float64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.useTargets {
		if _, ok := g.nodeToTargets[id1]; ok {
			if _, ok2 := g.nodeToTargets[id1][id2]; ok2 {
				g.nodeToTargets[id1][id2] = weight
			} else {
				g.nodeToTargets[id1][id2] = weight
			}
		} else {
			tmap := make(map[int64]float64)
			tmap[id2] = weight
			g.nodeToTargets[id1] = tmap
		}
	} else {
		if _, ok := g.nodeToSources[id2]; ok {
			if _, ok2 := g.nodeToSources[id2][id1]; ok2 {
				g.nodeToSources[id2][id1] = weight
			} else {
				g.nodeToSources[id2][id1] = weight
			}
		} else {
			tmap := make(map[int64]float64)
			tmap[id1] = weight
			g.nodeToSources[id2] = tmap
		}
	}
	return nil
}

func (g *graph) GetWeight(id1, id2 int64) (float64, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if _, ok := g.nodeToTargets[id1]; ok {
		if v, ok := g.nodeToTargets[id1][id2]; ok {
			return v, nil
		}
	}
	return 0, fmt.Errorf("there is no edge from %s to %s", id1, id2)
}

func (g *graph) GetSources(id int64) map[int64]float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if g.useTargets {
		log.Fatal("get sources error")
		return nil
	}

	return g.nodeToSources[id]
}

func (g *graph) GetTargets(id int64) map[int64]float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if !g.useTargets {
		log.Fatal("get targets error")
		return nil
	}

	return g.nodeToTargets[id]
}

func (g *graph) GetMasters() map[int64][]int {
	return g.masterWorkers
}

func (g *graph) GetMirrors() map[int64]int {
	return g.mirrorWorker
}

func (g *graph) IsMaster(id int64) bool {
	_, ok := g.masterWorkers[id]
	return ok
}

func (g *graph) IsMirror(id int64) bool {
	_, ok := g.mirrorWorker[id]
	return ok
}


// pattern graph should be constructed as the format
// NodeId type numberOfSuffixNodes id1 id2 id3 ...

func NewPatternGraph(rd io.Reader) (Graph, error) {
	buffer := bufio.NewReader(rd)

	g := newGraph()
	g.useTargets = true
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
			g.AddEdge(int64(nodeId), int64(v), 1)
		}
	}

	return g, nil
}


func NewGraphFromTXT(G io.Reader, Master io.Reader, Mirror io.Reader, Isolated io.Reader, useTargets bool, useIsolated bool) (Graph, error) {
	g := newGraph()
	g.useTargets = useTargets
	reader := bufio.NewReader(G)
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

		srcId := int64(parseSrc)
		dstId := int64(parseDst)

		//log.Printf("src: %v dst:%v\n", srcId, dstId)

		/*weight, err := strconv.ParseFloat(paras[2], 64)
		if err != nil {
			//fmt.Println("zs-log: " + paras[3])
			log.Fatal("parse weight error")
		}*/
		weight := 0.0

		nd1 := g.GetNode(srcId)
		if nd1 == nil {
			intId := srcId
			nd1 = NewNode(intId, int64(intId%tools.GraphSimulationTypeModel))
			if ok := g.AddNode(nd1); !ok {
				return nil, fmt.Errorf("%s already exists", nd1)
			}
		}
		nd2 := g.GetNode(dstId)
		if nd2 == nil {
			nd2 = NewNode(dstId, int64(dstId%tools.GraphSimulationTypeModel))
			if ok := g.AddNode(nd2); !ok {
				return nil, fmt.Errorf("%s already exists", nd2)
			}
		}
		g.AddEdge(nd1.int64(), nd2.int64(), weight)
	}

	master := bufio.NewReader(Master)
	for {
		line, err := master.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}

		paras := strings.Split(strings.Split(line, "\n")[0], " ")

		parseMaster, err := strconv.ParseInt(paras[0], 10, 64)
		if err != nil {
			log.Fatal("parse master node id error")
		}

		masterId := int64(parseMaster)

		masterNode := g.GetNode(masterId)
		if masterNode == nil {
			intId := masterId
			masterNode = NewNode(intId, int64(intId%tools.GraphSimulationTypeModel))
			if ok := g.AddNode(masterNode); !ok {
				return nil, fmt.Errorf("%s already exists", intId)
			}
		}

		mirrorWorkers := make([]int, 0)
		for i := 1; i < len(paras); i++ {
			parseWorker, err := strconv.ParseInt(paras[i], 10, 64)
			if err != nil {
				log.Fatal("parse worker id error")
			}
			mirrorWorkers = append(mirrorWorkers, int(parseWorker))
		}

		g.AddMaster(masterId, mirrorWorkers)
	}

	mirror := bufio.NewReader(Mirror)
	for {
		line, err := mirror.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		paras := strings.Split(strings.Split(line, "\n")[0], " ")

		parseMirror, err := strconv.ParseInt(paras[0], 10, 64)
		if err != nil {
			log.Fatal("parse mirror node id error")
		}
		mirrorId := int64(parseMirror)

		MasterWorker, err := strconv.ParseInt(paras[1], 10, 64)
		if err != nil {
			log.Fatal("parse master worker id error")
		}

		//log.Printf("mirrorId: %v MasterWorker:%v\n", mirrorId, MasterWorker)

		g.AddMirror(mirrorId, int(MasterWorker))
	}

	if useIsolated {
		isolated := bufio.NewReader(Isolated)
		for {
			line, err := isolated.ReadString('\n')
			if err != nil || io.EOF == err {
				break
			}
			paras := strings.Split(strings.Split(line, "\n")[0], " ")
			parseIso, err := strconv.ParseInt(paras[0], 10, 64)
			isoId := int64(parseIso)

			nd := NewNode(isoId, int64(parseIso%tools.GraphSimulationTypeModel))
			g.AddNode(nd)
		}
	}

	return g, nil
}

func GetTargetsNum(targetsFile io.Reader) map[int64]int {
	targets := bufio.NewReader(targetsFile)
	ans := make(map[int64]int)
	for {
		line, err := targets.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		paras := strings.Split(strings.Split(line, "\n")[0], " ")

		vertexId, err := strconv.ParseInt(paras[0], 10, 64)
		if err != nil {
			log.Fatal("parse target id error")
		}

		targetN, err := strconv.ParseInt(paras[1], 10, 64)
		if err != nil {
			log.Fatal("parse target num error")
		}

		ans[vertexId] = int(targetN)
	}
	return ans
}