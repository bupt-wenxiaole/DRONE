package algorithm

import (
	"graph"
	// for more information, please reference https://github.com/fatih/set
	"gopkg.in/fatih/set.v0"
	"time"
	"log"
)

type SimPair struct {
	PatternNode graph.ID
	DataNode    graph.ID
}

// generate post and pre set for data graph nodes(include FO nodes)
func GeneratePrePostFISet(g graph.Graph) (map[graph.ID]set.Interface, map[graph.ID]set.Interface) {
	preSet := make(map[graph.ID]set.Interface)
	for v := range g.GetNodes() {
		preSet[v] = set.NewNonTS()
		sources, _ := g.GetSources(v)
		for id := range sources {
			preSet[v].Add(id)
		}
	}
	postSet := make(map[graph.ID]set.Interface)
	for v := range g.GetNodes() {
		postSet[v] = set.NewNonTS()
		targets, _ := g.GetTargets(v)
		for id := range targets {
			postSet[v].Add(id)
		}
	}

	for v, routeMsg := range g.GetFOs() {
		preSet[v] = set.NewNonTS()
		for _, msg := range routeMsg {
			preSet[v].Add(msg.RelatedId())
			postSet[msg.RelatedId()].Add(v)
		}
	}

	return preSet, postSet
}

// in this algorithm, we assume all node u is in pattern graph while v node is in data graph
func GraphSim_PEVal(g graph.Graph, pattern graph.Graph, sim map[graph.ID]set.Interface, preSet map[graph.ID]set.Interface, postSet map[graph.ID]set.Interface) (bool, map[int][]*SimPair, float64, float64, int32, int32, int32) {
	nodeMap := pattern.GetNodes()
	patternNodeSet := set.NewNonTS() // a set for all pattern nodes
	for _, node := range nodeMap {
		patternNodeSet.Add(node)
	}

	log.Println("zs-log: start PEVal initial")
	// initial
	allNodeUnionFO := set.NewNonTS()
	for v := range g.GetNodes() {
		allNodeUnionFO.Add(v)
	}
	for v := range g.GetFOs() {
		allNodeUnionFO.Add(v)
	}

	preSim := make(map[graph.ID]set.Interface)
	remove := make(map[graph.ID]set.Interface)

	for i, id := range patternNodeSet.List() {
		log.Printf("zs-log: start PEval initial for Pattern Node %v \n", i)

		patternNode := id.(graph.Node)
		preSim[patternNode.ID()] = allNodeUnionFO.Copy()

		sim[patternNode.ID()] = set.NewNonTS()
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

		remove[patternNode.ID()] = set.NewNonTS()
		for _, tmp := range preSim[patternNode.ID()].List() {
			u := tmp.(graph.ID)
			remove[patternNode.ID()].Merge(preSet[u])
		}
		for _, tmp := range sim[patternNode.ID()].List() {
			u := tmp.(graph.ID)
			remove[patternNode.ID()].Separate(preSet[u])
		}
	}

	messageMap := make(map[int]set.Interface)

	for v, msgs := range g.GetFIs() {
		for u := range sim {
			if !sim[u].Has(v) {
				for _, msg := range msgs {
					partitionId := msg.RoutePartition()
					if _, ok := messageMap[partitionId]; !ok {
						messageMap[partitionId] = set.NewNonTS()
					}
					messageMap[partitionId].Add(&SimPair{PatternNode: u, DataNode: v})
				}
			}
		}
	}

	log.Println("zs-log: start calculate")

	//calculate
	iterationStartTime := time.Now()
	var iterationNum int32 = 0
	for {
		iterationFinish := true
		for _, id := range patternNodeSet.List() {
			u := id.(graph.Node).ID()
			if remove[u].Size() == 0 {
				continue
			}

			iterationNum++
			if iterationNum % 1000000 == 0 {
				log.Printf("zs-log: have iteration %v times \n", iterationNum)
			}
			iterationFinish = false
			uSources, _ := pattern.GetSources(u)
			for u_pre := range uSources {
				for _, vInterface := range remove[u].List() {
					v := vInterface.(graph.ID)
					if sim[u_pre].Has(v) {
						sim[u_pre].Remove(v)

						// if v belongs to FI set, we need to send message to other partition at end of this super step
						fiMap := g.GetFIs()
						if routeMsgs, ok := fiMap[v]; ok {
							for _, routeMsg := range routeMsgs {
								partitionId := routeMsg.RoutePartition()
								if _, ok = messageMap[partitionId]; !ok {
									messageMap[partitionId] = set.NewNonTS()
								}
								messageMap[partitionId].Add(&SimPair{PatternNode: u_pre, DataNode: v})
							}
						}

						for _, tmp := range preSet[v].List() {
							v_pre := tmp.(graph.ID)
							if set.Intersection(sim[u_pre], postSet[v_pre]).Size() == 0 {
								remove[u_pre].Add(v_pre)
							}
						}
					}
				}
			}

			preSim[u] = sim[u].Copy()
			remove[u].Clear()
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
		updatePairNum += int32(message.Size())

		reducedMsg[partitionId] = make([]*SimPair, 0)
		for _, msg := range message.List() {
			reducedMsg[partitionId] = append(reducedMsg[partitionId], msg.(*SimPair))
		}
	}
	combineTime := time.Since(combineStart).Seconds()

	dstPartitionNum = int32(len(reducedMsg))

	return len(reducedMsg) != 0, reducedMsg, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum
}

func GraphSim_IncEval(g graph.Graph, pattern graph.Graph, sim map[graph.ID]set.Interface, preSet map[graph.ID]set.Interface, postSet map[graph.ID]set.Interface, messages []*SimPair) (bool, map[int][]*SimPair, float64, float64, int32, int32, int32, float64, int32, int32) {
	nodeMap := pattern.GetNodes()
	patternNodeSet := set.NewNonTS() // a set for all pattern nodes
	for _, node := range nodeMap {
		patternNodeSet.Add(node)
	}

	// initial
	preSim := make(map[graph.ID]set.Interface)
	remove := make(map[graph.ID]set.Interface)
	for _, id := range patternNodeSet.List() {
		u := id.(graph.Node).ID()
		preSim[u] = sim[u].Copy()
		remove[u] = set.NewNonTS()
	}
	for _, message := range messages {
		u := message.PatternNode
		v := message.DataNode

		sim[u].Remove(v)
		for _, tmp := range preSet[v].List() {
			v_pre := tmp.(graph.ID)
			if set.Intersection(postSet[v_pre], sim[u]).IsEmpty() {
				remove[u].Add(v_pre)
			}
		}
	}

	//calculate
	messageMap := make(map[int]set.Interface)
	iterationStartTime := time.Now()
	var iterationNum int32 = 0

	for {
		iterationFinish := true
		for _, id := range patternNodeSet.List() {
			u := id.(graph.Node).ID()
			if remove[u].Size() == 0 {
				continue
			}

			iterationNum++
			iterationFinish = false
			uSources, _ := pattern.GetSources(u)
			for u_pre := range uSources {
				for _, vInterface := range remove[u].List() {
					v := vInterface.(graph.ID)
					if sim[u_pre].Has(v) {
						sim[u_pre].Remove(v)

						// if v belongs to FI set, we need to send message to other partition at end of this super step
						fiMap := g.GetFIs()
						if routeMsgs, ok := fiMap[v]; ok {
							for _, routeMsg := range routeMsgs {
								partitionId := routeMsg.RoutePartition()
								if _, ok = messageMap[partitionId]; !ok {
									messageMap[partitionId] = set.NewNonTS()
								}
								messageMap[partitionId].Add(&SimPair{PatternNode: u_pre, DataNode: v})
							}
						}

						for _, tmp := range preSet[v].List() {
							v_pre := tmp.(graph.ID)
							if set.Intersection(sim[u_pre], postSet[v_pre]).Size() == 0 {
								remove[u_pre].Add(v_pre)
							}
						}
					}
				}
			}

			preSim[u] = sim[u].Copy()
			remove[u].Clear()
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
		updatePairNum += int32(message.Size())

		reducedMsg[partitionId] = make([]*SimPair, 0)
		for _, msg := range message.List() {
			reducedMsg[partitionId] = append(reducedMsg[partitionId], msg.(*SimPair))
		}
	}
	dstPartitionNum = int32(len(reducedMsg))
	combineTime := time.Since(combineStart).Seconds()

	return len(reducedMsg) != 0, reducedMsg, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum, 0, int32(len(messages)), int32(len(messages))
}
