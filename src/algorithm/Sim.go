package algorithm

import (
	"graph"
	"time"
	"log"
	"fmt"
	"tools"
	"Set"
)

type SimPair struct {
	PatternNode graph.ID
	DataNode    graph.ID
}

/*
// generate post and pre set for data graph nodes(include FO nodes)
func GeneratePrePostFISet(g graph.Graph) (map[graph.ID]Set, map[graph.ID]Set) {
	preSet := make(map[graph.ID]Set)
	for v := range g.GetNodes() {
		preSet[v] = NewSet()
		sources, _ := g.GetSources(v)
		for id := range sources {
			preSet[v].Add(id)
		}
	}
	postSet := make(map[graph.ID]Set)
	for v := range g.GetNodes() {
		postSet[v] = NewSet()
		targets, _ := g.GetTargets(v)
		for id := range targets {
			postSet[v].Add(id)
		}
	}

	for v, routeMsg := range g.GetFOs() {
		preSet[v] = NewSet()
		for _, msg := range routeMsg {
			preSet[v].Add(msg.RelatedId())
		//	fmt.Println("zs-log:" + msg.RelatedId().String())
			postSet[msg.RelatedId()].Add(v)
		}
	}

	return preSet, postSet
}
*/

// in this algorithm, we assume all node u is in pattern graph while v node is in data graph
func GraphSim_PEVal(g graph.Graph, pattern graph.Graph, sim map[graph.ID]Set.Set) (bool, map[int][]*SimPair, float64, float64, int64, int32, int32) {
	emptySet1 := Set.NewSet()
	emptySet2 := Set.NewSet()
	nodeMap := pattern.GetNodes()
	patternNodeSet := Set.NewSet() // a set for all pattern nodes
	for id := range nodeMap {
		patternNodeSet.Add(id)
	}

	//log.Println("zs-log: start PEVal initial")

	// initial
	allNodeUnionFO := Set.NewSet()
	for v := range g.GetNodes() {
		allNodeUnionFO.Add(v)
	}
	for v := range g.GetFOs() {
		allNodeUnionFO.Add(v)
	}
	removeInit := Set.NewSet()
	for u := range allNodeUnionFO {
		//removeInit.Merge(preSet[u])
		Set.GetPostSet(g, u, emptySet1)
		if emptySet1.Size() != 0 {
			removeInit.Add(u)
		}
	}

	preSim := make(map[graph.ID]Set.Set)
	remove := make(map[graph.ID]Set.Set)
	allPatternColor := make(map[int64]bool)

	//log.Printf("zs-log: start PEval initial for Pattern Node \n")
	for id := range patternNodeSet {
		preSim[id] = allNodeUnionFO.Copy()
		remove[id] = removeInit.Copy()
		sim[id] = Set.NewSet()
		for v := range g.GetFOs() {
			sim[id].Add(v)
		}
		allPatternColor[nodeMap[id].Attr()] = true
	}

	for v, msg := range g.GetNodes() {
		_, ok := allPatternColor[msg.Attr()]
		if ok {
			for id := range patternNodeSet {
				if msg.Attr() == nodeMap[id].Attr() {
					targets, _ := pattern.GetTargets(id)
					if len(targets) == 0 {
						sim[id].Add(v)
						Set.GetPreSet(g, v, emptySet1)
						remove[id].Separate(emptySet1)
					} else {
						Set.GetPostSet(g, v, emptySet1)
						if emptySet1.Size() != 0 {
							sim[id].Add(v)
							Set.GetPreSet(g, v, emptySet2)
							remove[id].Separate(emptySet2)
						}
					}
				}
			}
		}
	}

	for v := range g.GetFOs() {
		_, ok := allPatternColor[v.IntVal()%tools.GraphSimulationTypeModel]
		if ok {
			for id := range patternNodeSet {
				if v.IntVal()%tools.GraphSimulationTypeModel == nodeMap[id].Attr() {
					sim[id].Add(v)
					Set.GetPreSet(g, v, emptySet1)
					remove[id].Separate(emptySet1)
				}
			}
		}
	}

	messageMap := make(map[int]map[SimPair]bool)

	for v, msgs := range g.GetFIs() {
		for u := range sim {
			if !sim[u].Has(v) {
				for _, msg := range msgs {
					partitionId := msg.RoutePartition()
					if _, ok := messageMap[partitionId]; !ok {
						messageMap[partitionId] = make(map[SimPair]bool)
					}
					messageMap[partitionId][SimPair{PatternNode: u, DataNode: v}] = true
				}
			}
		}
	}

	//log.Println("zs-log: start calculate")

	//calculate
	iterationStartTime := time.Now()
	var iterationNum int64 = 0
	for {
		iterationFinish := true
		for u := range patternNodeSet {
			if remove[u].Size() == 0 {
				continue
			}

			//log.Printf("u: %v,  iterationNum: %v,  removeSize: %v \n", u.String(), iterationNum, remove[u].Size())
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
						fiMap := g.GetFIs()
						if routeMsgs, ok := fiMap[v]; ok {
							for _, routeMsg := range routeMsgs {
								partitionId := routeMsg.RoutePartition()
								if _, ok = messageMap[partitionId]; !ok {
									messageMap[partitionId] = make(map[SimPair]bool)
								}
								messageMap[partitionId][SimPair{PatternNode: u_pre, DataNode: v}] = true
							}
						}

						Set.GetPreSet(g, v, emptySet1)
						for v_pre := range emptySet1 {
							Set.GetPostSet(g, v_pre, emptySet2)
							if !sim[u_pre].HasIntersection(emptySet2) {
								remove[u_pre].Add(v_pre)
							}
						}
					}
				}
				Set.GetPreSet(g, u_pre, emptySet1)
				iterationNum += int64(emptySet1.Size()) * count
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
			if msg.PatternNode.IntVal() == msg.DataNode.IntVal() % tools.GraphSimulationTypeModel {
				reducedMsg[partitionId] = append(reducedMsg[partitionId], &SimPair{PatternNode: msg.PatternNode, DataNode: msg.DataNode})
			}
		}
	}
	combineTime := time.Since(combineStart).Seconds()

	dstPartitionNum = int32(len(reducedMsg))

	return len(reducedMsg) != 0, reducedMsg, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum
}

func GraphSim_IncEval(g graph.Graph, pattern graph.Graph, sim map[graph.ID]Set.Set, messages []*SimPair) (bool, map[int][]*SimPair, float64, float64, int64, int32, int32, float64, int32, int32) {
	emptySet1 := Set.NewSet()
	emptySet2 := Set.NewSet()

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
		Set.GetPreSet(g, v, emptySet1)
		for v_pre := range emptySet1 {
			Set.GetPostSet(g, v_pre, emptySet2)
			if !emptySet2.HasIntersection(sim[u]) {
				remove[u].Add(v_pre)
			}
		}
	}

	//calculate
	messageMap := make(map[int]map[SimPair]bool)
	iterationStartTime := time.Now()
	var iterationNum int64 = 0

	fmt.Println("start inc calculate")

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
						fiMap := g.GetFIs()
						if routeMsgs, ok := fiMap[v]; ok {
							for _, routeMsg := range routeMsgs {
								partitionId := routeMsg.RoutePartition()
								if _, ok = messageMap[partitionId]; !ok {
									messageMap[partitionId] = make(map[SimPair]bool)
								}
								messageMap[partitionId][SimPair{PatternNode: u, DataNode: v}] = true
							}
						}

						Set.GetPreSet(g, v, emptySet1)
						for v_pre := range emptySet1 {
							Set.GetPostSet(g, v_pre, emptySet2)
							if !sim[u_pre].HasIntersection(emptySet2) {
								remove[u_pre].Add(v_pre)
							}
						}
					}
				}
				Set.GetPreSet(g, u_pre, emptySet1)
				iterationNum += int64(emptySet1.Size()) * count
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
	combineTime := time.Since(combineStart).Seconds()

	dstPartitionNum = int32(len(reducedMsg))

	return len(reducedMsg) != 0, reducedMsg, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum, 0, int32(len(messages)), int32(len(messages))
}
