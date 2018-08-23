package graph

import (
	"bytes"
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

type ID int64

func (s ID) String() string {
	return strconv.FormatInt(int64(s), 10)
}

func (s ID) IntVal() int64 {
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

func NewNode(id int64, attr int64) Node {
	return &node{
		id:   id,
		attr: attr,
	}
}

func (n *node) ID() ID {
	return ID(n.id)
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

	DeleteNode(id ID)

	// AddEdge adds an edge from nd1 to nd2 with the weight.
	// It returns error if a node does not exist.
	AddEdge(id1, id2 ID, weight float64) error

    IsMaster(id ID) bool
    IsMirror(id ID) bool

	AddMirror(id ID, masterWR int)

	GetMirrors() map[ID]int

	AddMaster(id ID, routeMsg []int)

	GetMasters() map[ID][]int

// GetWeight returns the weight from id1 to id2.
	GetWeight(id1, id2 ID) (float64, error)

	// GetSources returns the map of parent Nodes.
	// (Nodes that come towards the argument vertex.)
	GetSources(id ID) (map[ID]Node, error)

	// GetTargets returns the map of child Nodes.
	// (Nodes that go out of the argument vertex.)
	GetTargets(id ID) (map[ID]Node, error)

	// String describes the Graph.
	String() string
}

// graph is an internal default graph type that
// implements all methods in Graph interface.
type graph struct {
	mu sync.RWMutex // guards the following

	// idToNodes stores all nodes.
	idToNodes map[ID]Node

	// master vertices
	masterWorkers map[ID][]int

	// mirror vertices
	mirrorWorker map[ID]int

	// nodeToSources maps a Node identifer to sources(parents) with edge weights.
	nodeToSources map[ID]map[ID]float64

	// nodeToTargets maps a Node identifer to targets(children) with edge weights.
	nodeToTargets map[ID]map[ID]float64
}

// newGraph returns a new graph.
func newGraph() *graph {
	return &graph{
		idToNodes:     make(map[ID]Node),
		nodeToSources: make(map[ID]map[ID]float64),
		nodeToTargets: make(map[ID]map[ID]float64),
		masterWorkers: make(map[ID][]int),
		mirrorWorker:  make(map[ID]int),
	}
}


func (g *graph) Init() {
	g.idToNodes = make(map[ID]Node)
	g.nodeToSources = make(map[ID]map[ID]float64)
	g.nodeToTargets = make(map[ID]map[ID]float64)
	g.masterWorkers = make(map[ID][]int)
	g.mirrorWorker = make(map[ID]int)
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

func (g *graph) DeleteNode(id ID) {
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

func (g *graph) AddMaster(id ID, routeMsg []int) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.masterWorkers[id] = routeMsg
}

func (g *graph) AddMirror(id ID, masterWR int) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.mirrorWorker[id] = masterWR
}

func (g *graph) AddEdge(id1, id2 ID, weight float64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.nodeToTargets[id1]; ok {
		if _, ok2 := g.nodeToTargets[id1][id2]; ok2 {
			g.nodeToTargets[id1][id2] = weight
		} else {
			g.nodeToTargets[id1][id2] = weight
		}
	} else {
		tmap := make(map[ID]float64)
		tmap[id2] = weight
		g.nodeToTargets[id1] = tmap
	}
	if _, ok := g.nodeToSources[id2]; ok {
		if _, ok2 := g.nodeToSources[id2][id1]; ok2 {
			g.nodeToSources[id2][id1] = weight
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

func (g *graph) GetWeight(id1, id2 ID) (float64, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

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

func (g *graph) GetMasters() map[ID][]int {
	return g.masterWorkers
}

func (g *graph) GetMirrors() map[ID]int {
	return g.mirrorWorker
}

func (g *graph) IsMaster(id ID) bool {
	_, ok := g.masterWorkers[id]
	return ok
}

func (g *graph) IsMirror(id ID) bool {
	_, ok := g.mirrorWorker[id]
	return ok
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
			g.AddEdge(ID(nodeId), ID(v), 1)
		}
	}

	return g, nil
}


func NewGraphFromTXT(G io.Reader, Master io.Reader, Mirror io.Reader, Isolated io.Reader) (Graph, error) {
	g := newGraph()
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

		srcId := ID(parseSrc)
		dstId := ID(parseDst)

		log.Printf("src: %v dst:%v\n", srcId, dstId)

		weight, err := strconv.ParseFloat(paras[2], 64)
		if err != nil {
			//fmt.Println("zs-log: " + paras[3])
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
		g.AddEdge(nd1.ID(), nd2.ID(), weight)
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

		masterId := ID(parseMaster)

		//log.Printf("masterId: %v", masterId)

		masterNode := g.GetNode(masterId)
		if masterNode == nil {
			intId := masterId.IntVal()
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
		mirrorId := ID(parseMirror)

		MasterWorker, err := strconv.ParseInt(paras[1], 10, 64)
		if err != nil {
			log.Fatal("parse master worker id error")
		}

		log.Printf("mirrorId: %v MasterWorker:%v\n", mirrorId, MasterWorker)

		g.AddMirror(mirrorId, int(MasterWorker))
	}

	isolated := bufio.NewReader(Isolated)
	for {
		line, err := isolated.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		paras := strings.Split(strings.Split(line, "\n")[0], " ")
		parseIso, err := strconv.ParseInt(paras[0], 10, 64)
		isoId := ID(parseIso)

		nd := NewNode(isoId.IntVal(), int64(parseIso%tools.GraphSimulationTypeModel))
		g.AddNode(nd)
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
			log.Fatal("parse master node id error")
		}

		targetN, err := strconv.ParseInt(paras[1], 10, 64)
		if err != nil {
			log.Fatal("parse master node id error")
		}

		ans[vertexId] = int(targetN)
	}
	return ans
}