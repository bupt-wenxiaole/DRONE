package algorithm

import (
	"graph"
	"time"
	"log"
)

type SimPair struct {
	PatternNode graph.ID
	DataNode    graph.ID
}


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
			postSet[msg.RelatedId()].Add(v)
		}
	}

	return preSet, postSet
}

// in this algorithm, we assume all node u is in pattern graph while v node is in data graph
func GraphSim_PEVal(g graph.Graph, pattern graph.Graph, sim map[graph.ID]Set, preSet map[graph.ID]Set, postSet map[graph.ID]Set) (bool, map[int][]*SimPair, float64, float64, int32, int32, int32) {
	nodeMap := pattern.GetNodes()
	patternNodeSet := NewSet() // a set for all pattern nodes
	for _, node := range nodeMap {
		patternNodeSet.Add(node)
	}

	log.Println("zs-log: start PEVal initial")

	// initial
	allNodeUnionFO := NewSet()
	for v := range g.GetNodes() {
		allNodeUnionFO.Add(v)
	}
	for v := range g.GetFOs() {
		allNodeUnionFO.Add(v)
	}

	preSim := make(map[graph.ID]Set)
	remove := make(map[graph.ID]Set)

	for id := range patternNodeSet {
		log.Printf("zs-log: start PEval initial for Pattern Node %v \n", id.String())

		patternNode := id.(graph.Node)
		preSim[patternNode.ID()] = allNodeUnionFO.Copy()

		sim[patternNode.ID()] = NewSet()
		targets, _ := pattern.GetTargets(patternNode.ID())
		if len(targets) == 0 {
			for v, msg := range g.GetNodes() {
				if msg.Attr() == patternNode.Attr() {
					sim[patternNode.ID()].Add(v)
				}
			}
		} else {
			for v, msg := range g.GetNodes() {
				if postSet[v].Size() != 0 && msg.Attr() == patternNode.Attr() {
					sim[patternNode.ID()].Add(v)
				}
			}
		}
		for v := range g.GetFOs() {
			sim[patternNode.ID()].Add(v)
		}

		remove[patternNode.ID()] = NewSet()
		for u := range preSim[patternNode.ID()] {
			remove[patternNode.ID()].Merge(preSet[u])
		}
		for u := range sim[patternNode.ID()] {
			remove[patternNode.ID()].Separate(preSet[u])
		}
	}

	messageMap := make(map[int]map[*SimPair]bool)

	for v, msgs := range g.GetFIs() {
		for u := range sim {
			if !sim[u].Has(v) {
				for _, msg := range msgs {
					partitionId := msg.RoutePartition()
					if _, ok := messageMap[partitionId]; !ok {
						messageMap[partitionId] = make(map[*SimPair]bool)
					}
					messageMap[partitionId][&SimPair{PatternNode: u, DataNode: v}] = true
				}
			}
		}
	}

	log.Println("zs-log: start calculate")
	log.Printf("zs-log: node size:%v \n", allNodeUnionFO.Size())

	//calculate
	iterationStartTime := time.Now()
	var iterationNum int32 = 0
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
				for v := range remove[u] {

					iterationNum++
					if iterationNum % 10000 == 0 {
						log.Printf("zs-log: have iteration %v times \n", iterationNum)
						sum := 0
						for uTmp := range pattern.GetNodes() {
							sum += sim[uTmp].Size()
						}
						log.Printf("zs-log: sum of sim size:%v\n", sum)
					}

					if sim[u_pre].Has(v) {
						sim[u_pre].Remove(v)

						// if v belongs to FI set, we need to send message to other partition at end of this super step
						fiMap := g.GetFIs()
						if routeMsgs, ok := fiMap[v]; ok {
							for _, routeMsg := range routeMsgs {
								partitionId := routeMsg.RoutePartition()
								if _, ok = messageMap[partitionId]; !ok {
									messageMap[partitionId] = make(map[*SimPair]bool)
								}
								messageMap[partitionId][&SimPair{PatternNode: u_pre, DataNode: v}] = true
							}
						}

						for v_pre := range preSet[v] {
							if !sim[u_pre].HasIntersection(postSet[v_pre]) {
								remove[u_pre].Add(v_pre)
							}
						}
					}
				}
			}

			preSim[u] = sim[u].Copy()
			remove[u] = NewSet()
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
			reducedMsg[partitionId] = append(reducedMsg[partitionId], msg)
		}
	}
	combineTime := time.Since(combineStart).Seconds()

	dstPartitionNum = int32(len(reducedMsg))

	return len(reducedMsg) != 0, reducedMsg, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum
}

func GraphSim_IncEval(g graph.Graph, pattern graph.Graph, sim map[graph.ID]Set, preSet map[graph.ID]Set, postSet map[graph.ID]Set, messages []*SimPair) (bool, map[int][]*SimPair, float64, float64, int32, int32, int32, float64, int32, int32) {
	nodeMap := pattern.GetNodes()
	patternNodeSet := NewSet() // a set for all pattern nodes
	for id := range nodeMap {
		patternNodeSet.Add(id)
	}

	// initial
	preSim := make(map[graph.ID]Set)
	remove := make(map[graph.ID]Set)
	for u := range patternNodeSet {
		preSim[u] = sim[u].Copy()
		remove[u] = NewSet()
	}
	for _, message := range messages {
		u := message.PatternNode
		v := message.DataNode


		sim[u].Remove(v)
		for v_pre := range preSet[v] {
			if !postSet[v_pre].HasIntersection(sim[u]) {

				remove[u].Add(v_pre)
			}
		}
	}

	//calculate
	messageMap := make(map[int]map[*SimPair]bool)
	iterationStartTime := time.Now()
	var iterationNum int32 = 0

	for {
		iterationFinish := true
		for u := range patternNodeSet {
			if remove[u].Size() == 0 {
				continue
			}

			iterationFinish = false
			uSources, _ := pattern.GetSources(u)
			for u_pre := range uSources {
				for v := range remove[u] {
					iterationNum++

					if sim[u_pre].Has(v) {
						sim[u_pre].Remove(v)

						// if v belongs to FI set, we need to send message to other partition at end of this super step
						fiMap := g.GetFIs()
						if routeMsgs, ok := fiMap[v]; ok {
							for _, routeMsg := range routeMsgs {
								partitionId := routeMsg.RoutePartition()
								if _, ok = messageMap[partitionId]; !ok {
									messageMap[partitionId] = make(map[*SimPair]bool)
								}
								messageMap[partitionId][&SimPair{PatternNode: u, DataNode: v}] = true
							}
						}


						for v_pre := range preSet[v] {
							if !sim[u_pre].HasIntersection(postSet[v_pre]) {
								remove[u_pre].Add(v_pre)
							}
						}
					}
				}
			}

			preSim[u] = sim[u].Copy()
			remove[u] = NewSet()
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
			reducedMsg[partitionId] = append(reducedMsg[partitionId], msg)
		}
	}
	combineTime := time.Since(combineStart).Seconds()

	dstPartitionNum = int32(len(reducedMsg))

	return len(reducedMsg) != 0, reducedMsg, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum, 0, int32(len(messages)), int32(len(messages))
}
