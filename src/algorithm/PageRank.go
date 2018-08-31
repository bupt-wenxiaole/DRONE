package algorithm

import (
	"graph"
	"math"
	"log"
	"Set"
	"time"
)

const eps = 0.01
const alpha = 0.85

type PRPair struct {
	PRValue float64
	ID graph.ID
}

type PRBorder struct {
	ID graph.ID
	partitionId int
}

func GenerateTarget(g graph.Graph) map[int64]int {
	ans := make(map[int64]int)

	route := g.GetRoute()
	for u := range g.GetNodes() {
		ans[u.IntVal()] = len(g.GetTargets(u)) + len(route[u])
	}
	return ans
}

func PageRank_PEVal(g graph.Graph, targetNum map[int64]int, prVal map[int64]float64, accVal map[int64]float64, updated Set.Set) (bool, map[int][]*PRPair, float64) {

	initVal := 1.0
	for id := range g.GetNodes() {
		prVal[id.IntVal()] = initVal
		accVal[id.IntVal()] = 0
	}

	updatedBorder := make(map[PRBorder]bool)
	routes := g.GetRoute()

	iterationStartTime := time.Now()

	for u := range g.GetNodes() {
		temp := prVal[u.IntVal()] / float64(targetNum[u.IntVal()])
		for v := range g.GetTargets(u) {
			accVal[v.IntVal()] += temp
			updated.Add(v)
		}
		if route, ok := routes[u]; ok {
			for v, msg := range route {
				accVal[v.IntVal()] += temp
				updatedBorder[PRBorder{ID:v, partitionId:msg.RoutePartition()}] = true
			}
		}
	}

	iterationTime := time.Since(iterationStartTime).Seconds()

	messageMap := make(map[int][]*PRPair)
	for border := range updatedBorder {
		partitionId := border.partitionId
		id := border.ID
		if messageMap[partitionId] == nil {
			messageMap[partitionId] = make([]*PRPair, 0)
		}
		messageMap[partitionId] = append(messageMap[partitionId], &PRPair{ID:id, PRValue:accVal[id.IntVal()]})
		delete(accVal, id.IntVal())
	}

	return len(messageMap) != 0, messageMap, iterationTime
}

func PageRank_IncEval(g graph.Graph, targetNum map[int64]int, prVal map[int64]float64, accVal map[int64]float64, updatedSet Set.Set, receiveBuffer map[int64]float64) (bool, map[int][]*PRPair, float64) {
	maxerr := 0.0

	for id, msg := range receiveBuffer {
		accVal[id] += msg
		updatedSet.Add(graph.StringID(id))
	}

	tempSet := Set.NewSet()
	tempAcc := make(map[int64]float64)

	updatedBorder := make(map[PRBorder]bool)
	routes := g.GetRoute()
	iterationStartTime := time.Now()

	log.Printf("updated size:%v\n", len(updatedSet))

	for u := range updatedSet {
		log.Printf("u:%v, acc:%v, pr:%v\n", u.IntVal(), accVal[u.IntVal()], prVal[u.IntVal()])
		pr := alpha * accVal[u.IntVal()] + (1 - alpha)
		log.Printf("pr:%v\n", pr)
		err := math.Abs(pr - prVal[u.IntVal()])
		maxerr = math.Max(maxerr, err)

		if err > eps {
			diff := (pr - prVal[u.IntVal()]) / float64(targetNum[u.IntVal()])
			log.Printf("diff:%v\n", diff)
			for v := range g.GetTargets(u) {
				tempAcc[v.IntVal()] += diff
				tempSet.Add(v)
			}

			if route, ok := routes[u]; ok {
				for v, msg := range route {
					accVal[v.IntVal()] += diff
					updatedBorder[PRBorder{ID:v, partitionId:msg.RoutePartition()}] = true
				}
			}
		}
	}
	log.Printf("maxerr:%v\n", maxerr)

	updatedSet.Clear()
	for u := range tempSet {
		accVal[u.IntVal()] += tempAcc[u.IntVal()]
		updatedSet.Add(u)
		delete(tempAcc, u.IntVal())
	}
	tempSet.Clear()
	iterationTime := time.Since(iterationStartTime).Seconds()

	messageMap := make(map[int][]*PRPair)
	for border := range updatedBorder {
		partitionId := border.partitionId
		id := border.ID
		if messageMap[partitionId] == nil {
			messageMap[partitionId] = make([]*PRPair, 0)
		}
		messageMap[partitionId] = append(messageMap[partitionId], &PRPair{ID:id, PRValue:accVal[id.IntVal()]})
		log.Printf("msg: id:%v, diff:%v\n", id.IntVal(), tempAcc[id.IntVal()])
		delete(accVal, id.IntVal())
	}

	return len(messageMap) != 0, messageMap, iterationTime
}
