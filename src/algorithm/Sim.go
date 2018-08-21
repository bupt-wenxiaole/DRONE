package algorithm

import (
	"graph"
	"Set"
	"time"
	"log"
)

type SimPair struct {
	PatternNode graph.ID
	DataNode    graph.ID
}

func TestSim(v graph.ID, u graph.ID, postMap map[graph.ID]map[graph.ID]int, pattern graph.Graph) bool {
	targets, _ := pattern.GetTargets(u)
	for t := range targets {
		if postMap[v][t] != 0 {
			return false
		}
	}
	return true
}

// in this algorithm, we assume all node u is in pattern graph while v node is in data graph
func GraphSim_PEVal(g graph.Graph, pattern graph.Graph, simSet map[graph.ID]Set.Set, postMap map[graph.ID]map[graph.ID]int, updatedMaster Set.Set, updatedMirror Set.Set) (bool, map[int]map[SimPair]int, float64, float64, int64, int32, int) {
	var iterationNum int64 = 0

	nodeMap := pattern.GetNodes()
	patternNodeSet := Set.NewSet() // a set for all pattern nodes
	for id := range nodeMap {
		patternNodeSet.Add(id)
	}

	//log.Printf("zs-log: start PEval initial for Pattern Node for rank:%v \n", id)
	//log.Printf("pattern node size:%v\n", patternNodeSet.Size())
	for id := range g.GetNodes() {
		simSet[id] = Set.NewSet()
	}

	//log.Println("step 1")

	for v, msg := range g.GetNodes() {
		for u := range patternNodeSet {
			if msg.Attr() == nodeMap[u].Attr() {
				targets, _ := pattern.GetTargets(u)
				sources, _ := g.GetSources(v)
				simSet[v].Add(u)
				if len(targets) != 0 {
					if g.IsMirror(v) {
						updatedMirror.Add(v)
					} else if g.IsMaster(v) {
						updatedMaster.Add(v)
					}
				}

				for v_ := range sources {
					if _, ok := postMap[v_]; !ok {
						postMap[v_] = make(map[graph.ID]int)
					}
					postMap[v_][u] = postMap[v_][u] + 1
				}
			}
		}
	}

	//log.Println("step 2")
	messageMap := make(map[int]map[SimPair]int)
	mirrors := g.GetMirrors()
	for v := range updatedMirror {
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

func GraphSim_IncEval(g graph.Graph, pattern graph.Graph, sim map[graph.ID]Set.Set, postMap map[graph.ID]map[graph.ID]int, updatedMaster Set.Set, updatedByMessage Set.Set, exchangeMessages map[graph.ID]map[graph.ID]int) (bool, map[int]map[SimPair]int, float64, float64, int64, int32, int32, float64, int32, int32) {
	for v, posts := range exchangeMessages {
		if len(posts) != len(postMap[v]) {
			updatedByMessage.Add(v)
		}
/*
		for u := range postMap[v] {
			if posts[u] == 0 {
				delete(postMap[v], u)
			}
			postMap[v][u] = posts[u]
		}
*/		postMap[v] = posts
	}

	for v := range postMap {
		for u, times := range postMap[v] {
			log.Printf("first level: u: %v, v: %v, times:%v\n", u.IntVal(), v.IntVal(), times)
		}
	}

	updated := Set.NewSet()
	messageMap := make(map[int]map[SimPair]int)
	mirrors := g.GetMirrors()

	for v := range updatedByMessage {
		for u := range sim[v] {
			if !TestSim(v, u, postMap, pattern) {
				log.Printf("delete %v from sim(%v)\n", u, v)
				for temp := range postMap[v] {
					log.Printf("second level: u: %v, v: %v\n", temp.IntVal(), v.IntVal())
				}

				sim[v].Remove(u)
				sources, _ := g.GetSources(v)
				for source := range sources {
					postMap[source][u] = postMap[source][u] - 1
					if postMap[source][u] == 0 {
						delete(postMap[source], u)
						updated.Add(source)
					}

					if g.IsMirror(source) {
						partition := mirrors[source]
						if _, ok := messageMap[partition]; !ok {
							messageMap[partition] = make(map[SimPair]int)
						}

						pair := SimPair{PatternNode:u, DataNode:source}
						messageMap[partition][pair] = messageMap[partition][pair] - 1
					}

					if g.IsMaster(source) {
						updatedMaster.Add(source)
					}
				}
			}
		}
	}

	iterationStartTime := time.Now()
	for len(updated) != 0 {
		v := updated.Top()
		updated.Remove(v)

		for u := range sim[v] {
			if !TestSim(v, u, postMap, pattern) {
				sim[v].Remove(u)
				sources, _ := g.GetSources(v)
				for source := range sources {
					postMap[source][u] = postMap[source][u] - 1
					if postMap[source][u] == 0 {
						delete(postMap[source], u)
						updated.Add(source)
					}

					if g.IsMirror(source) {
						partition := mirrors[source]
						if _, ok := messageMap[partition]; !ok {
							messageMap[partition] = make(map[SimPair]int)
						}

						pair := SimPair{PatternNode:u, DataNode:source}
						messageMap[partition][pair] = messageMap[partition][pair] - 1
					}

					if g.IsMaster(source) {
						updatedMaster.Add(source)
					}
				}
			}
		}
	}
	iterationTime := time.Since(iterationStartTime).Seconds()

	return len(messageMap) != 0, messageMap, iterationTime, 0, 0, 0, 0, 0, 0, 0
}