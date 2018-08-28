package algorithm

import (
	"graph"
	"math"
	"log"
	"time"
	"Set"
)

const eps = 0.01
const alpha = 0.85

type PRPair struct {
	PRValue float64
	ID int64
}

func PageRank_PEVal(g graph.Graph, prVal map[int64]float64, accVal map[int64]float64, targetsNum map[int64]int, updatedSet Set.Set, updatedMaster Set.Set, updatedMirror Set.Set) (bool, map[int][]*PRPair, float64) {
	initVal := 1.0
	for id := range g.GetNodes() {
		prVal[id] = initVal
	}

	iterationStartTime := time.Now()
	for u := range g.GetNodes() {
		temp := alpha * prVal[u] / float64(targetsNum[u])
		for v := range g.GetTargets(u) {
			accVal[v] += temp
			updatedSet.Add(v)
			if g.IsMirror(v) {
				updatedMirror.Add(v)
			}
			if g.IsMaster(v) {
				updatedMaster.Add(v)
			}
		}
	}
	iterationTime := time.Since(iterationStartTime).Seconds()

	messageMap := make(map[int][]*PRPair)
	mirrorMap := g.GetMirrors()
	for v := range updatedMirror {
		workerId := mirrorMap[v]
		if _, ok := messageMap[workerId]; !ok {
			messageMap[workerId] = make([]*PRPair, 0)
		}
		messageMap[workerId] = append(messageMap[workerId], &PRPair{ID:v,PRValue:accVal[v]})
	}

	return true, messageMap, iterationTime
}

func PageRank_IncEval(g graph.Graph, prVal map[int64]float64, accVal map[int64]float64, targetsNum map[int64]int, updatedSet Set.Set, updatedMaster Set.Set, updatedMirror Set.Set, exchangeBuffer []*PRPair) (bool, map[int][]*PRPair, float64) {
	for _, msg := range exchangeBuffer {
		accVal[msg.ID] = msg.PRValue
		updatedSet.Add(msg.ID)
	}

	nextUpdated := Set.NewSet()

	log.Printf("updated vertexnum:%v\n", updatedSet.Size())

	iterationStartTime := time.Now()
	for u := range updatedSet {
		pr := accVal[u] + 1 - alpha
		if math.Abs(prVal[u] - pr) > eps {
			for v := range g.GetTargets(u) {
				nextUpdated.Add(v)
				accVal[v] += alpha * (pr - prVal[u]) / float64(targetsNum[u])
				if g.IsMirror(v) {
					updatedMirror.Add(v)
				}
				if g.IsMaster(v) {
					updatedMaster.Add(v)
				}
			}
		}
		prVal[u] = pr
	}
	iterationTime := time.Since(iterationStartTime).Seconds()

	updatedSet.Clear()
	for u := range nextUpdated {
		updatedSet.Add(u)
	}
	nextUpdated.Clear()

	messageMap := make(map[int][]*PRPair)
	mirrorMap := g.GetMirrors()
	for v := range updatedMirror {
		workerId := mirrorMap[v]
		if _, ok := messageMap[workerId]; !ok {
			messageMap[workerId] = make([]*PRPair, 0)
		}
		messageMap[workerId] = append(messageMap[workerId], &PRPair{ID:v,PRValue:accVal[v]})
	}

	return len(messageMap) != 0, messageMap, iterationTime
}