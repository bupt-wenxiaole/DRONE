package algorithm

import (
	"graph"
	"math"
	"log"
	"time"
)

const eps = 0.01

type PRPair struct {
	PRValue float64
	ID graph.ID
}

func PageRank_PEVal(g graph.Graph, prVal map[int64]float64, oldPr map[int64]float64, targetsNum map[int64]int) (bool, map[int][]*PRPair, float64) {
	nodeNum := len(g.GetNodes())
	initVal := 1.0
	for id := range g.GetNodes() {
		oldPr[id.IntVal()] = initVal
	}

	still := 0.0
	iterationStartTime := time.Now()

	for id := range g.GetNodes() {
		targets, _ := g.GetTargets(id)
		if len(targets) == 0 {
			still += oldPr[id.IntVal()]
		} else {
			num := float64(targetsNum[id.IntVal()])
			for dstId := range targets {
				prVal[dstId.IntVal()] += 0.85 * oldPr[id.IntVal()] / num
			}
		}
	}
	still = 0.85 * still / float64(nodeNum) + 0.15 * initVal

	for id := range g.GetNodes() {
		prVal[id.IntVal()] += still
	}
	iterationTime := time.Since(iterationStartTime).Seconds()

	messageMap := make(map[int][]*PRPair)
	for mirror, workerId := range g.GetMirrors() {
		if _, ok := messageMap[workerId]; !ok {
			messageMap[workerId] = make([]*PRPair, 0)
		}
		messageMap[workerId] = append(messageMap[workerId], &PRPair{ID:mirror,PRValue:prVal[mirror.IntVal()]})
	}

	return true, messageMap, iterationTime
}

func PageRank_IncEval(g graph.Graph, prVal map[int64]float64, oldPr map[int64]float64, targetsNum map[int64]int, exchangeBuffer []*PRPair) (bool, map[int][]*PRPair, float64) {
	maxerr := 0.0

	initVal := 1.0
	nodeNum := len(g.GetNodes())
	updated := false

	for _, msg := range exchangeBuffer {
		prVal[msg.ID.IntVal()] = msg.PRValue
	}

	for id := range g.GetNodes() {
		maxerr = math.Max(maxerr, math.Abs(prVal[id.IntVal()] - oldPr[id.IntVal()]))
	}
	log.Printf("max error:%v\n", maxerr)

	if maxerr > eps {
		updated = true
	}
	messageMap := make(map[int][]*PRPair)

	//oldPr = prVal
	//prVal = make(map[int64]float64)

	for u := range g.GetNodes() {
		uval := u.IntVal()
		oldPr[uval] = prVal[uval]
		prVal[uval] = 0
	}

	iterationStartTime := time.Now()

	still := 0.0
	for id := range g.GetNodes() {
		targets, _ := g.GetTargets(id)
		if len(targets) == 0 {
			still += oldPr[id.IntVal()]
		} else {
			num := float64(targetsNum[id.IntVal()])
			for dstId := range targets {
				prVal[dstId.IntVal()] += 0.85 * oldPr[id.IntVal()] / num
			}
		}
	}
	still = 0.85 * still / float64(nodeNum) + 0.15 * initVal

	for id := range g.GetNodes() {
		prVal[id.IntVal()] += still
	}
	iterationTime := time.Since(iterationStartTime).Seconds()

	for mirror, workerId := range g.GetMirrors() {
		if _, ok := messageMap[workerId]; !ok {
			messageMap[workerId] = make([]*PRPair, 0)
		}
		messageMap[workerId] = append(messageMap[workerId], &PRPair{ID:mirror,PRValue:prVal[mirror.IntVal()]})
	}

	return updated, messageMap, iterationTime
}