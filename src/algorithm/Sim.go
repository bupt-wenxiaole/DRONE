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


// generate post and pre set for data graph nodes(include FO nodes)
func GeneratePrePostFISet(g graph.Graph) (map[graph.ID]Set.Set, map[graph.ID]Set.Set) {
	preSet := make(map[graph.ID]Set.Set)
	for v := range g.GetNodes() {
		preSet[v] = Set.NewSet()
		sources, _ := g.GetSources(v)
		for id := range sources {
			preSet[v].Add(id)
		}
	}
	postSet := make(map[graph.ID]Set.Set)
	for v := range g.GetNodes() {
		postSet[v] = Set.NewSet()
		targets, _ := g.GetTargets(v)
		for id := range targets {
			postSet[v].Add(id)
		}
	}

	for v, routeMsg := range g.GetFOs() {
		preSet[v] = Set.NewSet()
		for _, msg := range routeMsg {
			preSet[v].Add(msg.RelatedId())
		//	fmt.Println("zs-log:" + msg.RelatedId().String())
			postSet[msg.RelatedId()].Add(v)
		}
	}

	return preSet, postSet
}


// in this algorithm, we assume all node u is in pattern graph while v node is in data graph
func GraphSim_PEVal(g graph.Graph, pattern graph.Graph, sim map[graph.ID]Set.Set, id int, allNodeUnionFO Set.Set, preSet map[graph.ID]Set.Set, postSet map[graph.ID]Set.Set) (bool, map[int][]*SimPair, float64, float64, int64, int32, int32) {
	//emptySet1 := Set.NewSet()
	//emptySet2 := Set.NewSet()
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
	log.Printf("allNodeUnionFO size: %v\n", allNodeUnionFO.Size())
	count := 0
	for u := range allNodeUnionFO {
		count++
		//targets, _ := g.GetTargets(u)
		if len(postSet[u]) != 0 {
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
		preSim[id] = allNodeUnionFO.Copy()
		remove[id] = removeInit.Copy()
		sim[id] = Set.NewSet()
		for v := range g.GetFOs() {
			sim[id].Add(v)
		}
		allPatternColor[nodeMap[id].Attr()] = true
	}

	//log.Println("step 1")

	for v, msg := range g.GetNodes() {
		_, ok := allPatternColor[msg.Attr()]
		if ok {
			for id := range patternNodeSet {
				if msg.Attr() == nodeMap[id].Attr() {
					targets, _ := pattern.GetTargets(id)
					if len(targets) == 0 {
						sim[id].Add(v)
						//Set.GetPreSet(g, v, emptySet1)
						iterationNum += int64(preSet[v].Size())
						remove[id].Separate(preSet[v])
					} else {
						if postSet[v].Size() != 0 {
							sim[id].Add(v)
							//Set.GetPreSet(g, v, emptySet2)
							iterationNum += int64(preSet[v].Size())
							remove[id].Separate(preSet[v])
						}
					}
				}
			}
		}
	}

	//log.Println("step 2")

	for v := range g.GetFOs() {
		_, ok := allPatternColor[v.IntVal()%tools.GraphSimulationTypeModel]
		if ok {
			for id := range patternNodeSet {
				if v.IntVal()%tools.GraphSimulationTypeModel == nodeMap[id].Attr() {
					sim[id].Add(v)
					//Set.GetPreSet(g, v, emptySet1)
					iterationNum += int64(len(preSet[v]))
					remove[id].Separate(preSet[v])
				}
			}
		}
	}
	//log.Println("step 3")

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

	log.Println("zs-log: start calculate")

	//calculate
	iterationStartTime := time.Now()
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
				for v := range remove[u] {

					iterationNum++
					if sim[u_pre].Has(v) {
						sim[u_pre].Remove(v)
						iterationNum++

						// if v belongs to FI set, we need to send message to other partition at end of this super step
						fiMap := g.GetFIs()
						if routeMsgs, ok := fiMap[v]; ok {
							for _, routeMsg := range routeMsgs {
								iterationNum++
								partitionId := routeMsg.RoutePartition()
								if _, ok = messageMap[partitionId]; !ok {
									messageMap[partitionId] = make(map[SimPair]bool)
								}
								messageMap[partitionId][SimPair{PatternNode: u_pre, DataNode: v}] = true
							}
						}
						//Set.GetPreSet(g, v, emptySet1)
						for v_pre := range preSet[v] {
							iterationNum++
							//Set.GetPostSet(g, v_pre, emptySet2)
							if !sim[u_pre].HasIntersection(postSet[v_pre]) {
								remove[u_pre].Add(v_pre)
							}
						}
					}
				}
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
	messageMap = nil
	/*for v, msgs := range g.GetFIs() {
		for u := range sim {
			if !sim[u].Has(v) && u.IntVal() == v.IntVal() % tools.GraphSimulationTypeModel {
				for _, msg := range msgs {
					updatePairNum++
					partitionId := msg.RoutePartition()
					if _, ok := reducedMsg[partitionId]; !ok {
						reducedMsg[partitionId] = make([]*SimPair, 0)
					}
					reducedMsg[partitionId] = append(reducedMsg[partitionId], &SimPair{PatternNode: u, DataNode: v})
				}
			}
		}
	}*/


	combineTime := time.Since(combineStart).Seconds()

	dstPartitionNum = int32(len(reducedMsg))

	return len(reducedMsg) != 0, reducedMsg, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum
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
