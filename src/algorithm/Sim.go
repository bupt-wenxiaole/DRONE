package algorithm

import (
	"graph"
	"time"
	"log"
	"tools"
	"Set"
)

type SimPair struct {
	PatternNode graph.ID
	DataNode    graph.ID
}


// in this algorithm, we assume all node u is in pattern graph while v node is in data graph
func GraphSim_PEVal(g graph.Graph, pattern graph.Graph, sim map[graph.ID]Set.Set, postMap map[graph.ID]map[graph.ID]int) (bool, map[int]map[SimPair]int, float64, float64, int64, int32, int) {
	var iterationNum int64 = 0

	nodeMap := pattern.GetNodes()
	patternNodeSet := Set.NewSet() // a set for all pattern nodes
	for id := range nodeMap {
		patternNodeSet.Add(id)
	}

	//log.Printf("zs-log: start PEVal initial for rank:%v\n", id)

	// initial
	//log.Printf("start calculate remove set for rank:%v\n", id)
	removeInit := Set.NewSet()
	count := 0
	for u := range g.GetNodes() {
		count++

		targets, _ := g.GetTargets(u)
		if len(targets) != 0 {
			removeInit.Add(u)
		}
		iterationNum++
	}

	preSim := make(map[graph.ID]Set.Set)
	remove := make(map[graph.ID]Set.Set)
	allPatternColor := make(map[int64]bool)

	//log.Printf("zs-log: start PEval initial for Pattern Node for rank:%v \n", id)
	//log.Printf("pattern node size:%v\n", patternNodeSet.Size())
	for id := range patternNodeSet {
		for v := range g.GetNodes() {
			preSim[id].Add(v)
		}

		remove[id] = removeInit.Copy()
		sim[id] = Set.NewSet()
		allPatternColor[nodeMap[id].Attr()] = true
	}

	//log.Println("step 1")

	for v, msg := range g.GetNodes() {
		_, ok := allPatternColor[msg.Attr()]
		if ok {
			for id := range patternNodeSet {
				if msg.Attr() == nodeMap[id].Attr() {
					targets, _ := pattern.GetTargets(id)
					sources, _ := g.GetSources(v)
					if len(targets) == 0 {
						sim[id].Add(v)
						iterationNum += int64(len(sources))
						remove[id].Separate(sources)
					} else {
						vTargets, _ := g.GetTargets(v)
						if len(vTargets) != 0 || !g.IsMaster(v) {
							sim[id].Add(v)
							//Set.GetPreSet(g, v, emptySet2)
							iterationNum += int64(len(sources))
							remove[id].Separate(sources)
						}
					}

					for v_ := range sources {
						if _, ok := postMap[v_]; !ok {
							postMap[v_] = make(map[graph.ID]int)
						}
						postMap[v_][id] = postMap[v_][id] + 1
					}
				}
			}
		}
	}

	//log.Println("step 2")
	messageMap := make(map[int]map[SimPair]int)
	mirrors := g.GetMirrors()
	for v := range mirrors {
		for u := range postMap[v] {
			partitionId := mirrors[v]

			if _, ok := messageMap[partitionId]; !ok {
				messageMap[partitionId] = make(map[SimPair]int)
			}
			simPair := SimPair{DataNode:v, PatternNode:u}
			messageMap[partitionId][simPair] = messageMap[partitionId][simPair] + postMap[v][u]
		}
	}

	//combineStart := time.Now()

	return len(messageMap) != 0, messageMap, 0, 0, iterationNum, 0, len(messageMap)
}

func GraphSim_IncEval(g graph.Graph, pattern graph.Graph, sim map[graph.ID]Set.Set, messages []*SimPair, preSet map[graph.ID]Set.Set, postSet map[graph.ID]Set.Set) (bool, map[int][]*SimPair, float64, float64, int64, int32, int32, float64, int32, int32) {
	//emptySet1 := Set.NewSet()
	//emptySet2 := Set.NewSet()

	nodeMap := pattern.GetNodes()
	patternNodeSet := Set.NewSet() // a set for all pattern nodes
	for id := range nodeMap {
		patternNodeSet.Add(id)
	}

	// initial
	log.Println("start inc initial")
	preSim := make(map[graph.ID]Set.Set)
	remove := make(map[graph.ID]Set.Set)
	for u := range patternNodeSet {
		preSim[u] = sim[u].Copy()
		remove[u] = Set.NewSet()
	}
	for _, message := range messages {
		u := message.PatternNode
		v := message.DataNode

		sim[u].Remove(v)
		//Set.GetPreSet(g, v, emptySet1)
		for v_pre := range preSet[v] {
			//Set.GetPostSet(g, v_pre, emptySet2)
			if !postSet[v_pre].HasIntersection(sim[u]) {
				remove[u].Add(v_pre)
			}
		}
	}

	//calculate
	messageMap := make(map[int]map[SimPair]bool)
	iterationStartTime := time.Now()
	var iterationNum int64 = 0

	log.Println("start inc calculate")

	for {
		iterationFinish := true
		for u := range patternNodeSet {
			if remove[u].Size() == 0 {
				continue
			}
			log.Printf("u: %v,  iterationNum: %v,  removeSize: %v \n", u.String(), iterationNum, remove[u].Size())
			iterationFinish = false
			uSources, _ := pattern.GetSources(u)
			for u_pre := range uSources {
				var count int64 = 0
				for v := range remove[u] {
					iterationNum++

					if sim[u_pre].Has(v) {
						sim[u_pre].Remove(v)
						count++

						// if v belongs to FI set, we need to send message to other partition at end of this super step
						route := g.GetRoute()
						if routeMsgs, ok := route[v]; ok {
							for _, routeMsg := range routeMsgs {
								partitionId := routeMsg.RoutePartition()
								if _, ok = messageMap[partitionId]; !ok {
									messageMap[partitionId] = make(map[SimPair]bool)
								}
								messageMap[partitionId][SimPair{PatternNode: u, DataNode: v}] = true
							}
						}

						//Set.GetPreSet(g, v, emptySet1)
						for v_pre := range preSet[v] {
							//Set.GetPostSet(g, v_pre, emptySet2)
							if !sim[u_pre].HasIntersection(postSet[v_pre]) {
								remove[u_pre].Add(v_pre)
							}
						}
					}
				}
				//Set.GetPreSet(g, u_pre, emptySet1)
				iterationNum += int64(preSet[u_pre].Size()) * count
			}

			preSim[u] = sim[u].Copy()
			remove[u] = Set.NewSet()
		}
		if iterationFinish {
			break
		}
	}
	iterationTime := time.Since(iterationStartTime).Seconds()

	combineStart := time.Now()
	var updatePairNum int32 = 0
	var dstPartitionNum int32 = 0

	reducedMsg := make(map[int][]*SimPair)
	for partitionId, message := range messageMap {
		updatePairNum += int32(len(message))
		reducedMsg[partitionId] = make([]*SimPair, 0)
		for msg := range message {
			reducedMsg[partitionId] = append(reducedMsg[partitionId], &SimPair{PatternNode:msg.PatternNode, DataNode:msg.DataNode})
		}
	}
	for partitionId, msg := range reducedMsg {
		if len(msg) == 0 {
			delete(reducedMsg, partitionId)
		}
	}
	combineTime := time.Since(combineStart).Seconds()

	dstPartitionNum = int32(len(reducedMsg))

	return len(reducedMsg) != 0, reducedMsg, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum, 0, int32(len(messages)), int32(len(messages))
}