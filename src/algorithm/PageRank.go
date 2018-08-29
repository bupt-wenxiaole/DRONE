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
	}

	return len(messageMap) != 0, messageMap, iterationTime
}

func PageRank_IncEval(g graph.Graph, prVal map[int64]float64, oldPr map[int64]float64, workerNum int, partitionId int, outerMsg map[int64][]int64, messages map[int64]float64, totalVertexNum int64) (bool, map[int][]*PRMessage, map[int64]float64, map[int64]float64) {
	maxerr := 0.0

	still := 0.0
	initVal := 1.0 / float64(totalVertexNum)
	updated := false

	//var receiveSum float64 = 0
	//var sendSum float64 = 0

	for id, msg := range messages {
		//log.Printf("id:%v, val:%v\n", id, msg)
		if id != -1 {
			prVal[id] += msg
			//receiveSum += msg
		} else {
			still += msg
		}
	}

	log.Printf("threshold:%v\n", eps * initVal)

	var sum float64 = 0
	for id := range g.GetNodes() {
		prVal[id.IntVal()] += still * 0.85
		if math.Abs(prVal[id.IntVal()] - oldPr[id.IntVal()]) > eps * initVal {
			updated = true
		}
		maxerr = math.Max(maxerr, math.Abs(prVal[id.IntVal()] - oldPr[id.IntVal()]))
		sum += prVal[id.IntVal()]
	}
	//log.Printf("total vertex num:%v\n", totalVertexNum)
	//log.Printf("still receive:%v\n", still)
	log.Printf("max error:%v\n", maxerr)

	tempPr := make(map[int64]float64)
	messagePr := make(map[int64]float64)
	still = 0

	for id := range g.GetNodes() {
		targets, _ := g.GetTargets(id)
		sonNum := len(targets) + len(outerMsg[id.IntVal()])
		tempPr[id.IntVal()] += 0.15 * initVal
		if sonNum == 0 {
			still += prVal[id.IntVal()] / float64(totalVertexNum)
		} else {
			val := prVal[id.IntVal()] / float64(sonNum)
			//log.Printf("val: %v\n", val)
			for target := range targets {
				tempPr[target.IntVal()] += 0.85 * val
			}
			for _, outer := range outerMsg[id.IntVal()] {
				//log.Printf("out node:%v\n", outer)

				messagePr[outer] += 0.85 * val
				//sendSum += 0.85 * val
			}
		}
	}
	for id := range g.GetNodes() {
		tempPr[id.IntVal()] += 0.85 * still
	}

	reduceMsg := make(map[int][]*PRMessage)

	for i := 0; i < workerNum; i++ {
		if i == partitionId {
			continue
		}
		reduceMsg[i] = make([]*PRMessage, 0)
		reduceMsg[i] = append(reduceMsg[i], &PRMessage{PRValue:still,ID:graph.StringID(-1)})
	}

	//log.Printf("still send:%v\n", still)
/*
	for fo, routeMsg := range g.GetFOs() {
		partition := routeMsg[0].RoutePartition()
		//log.Printf("send id:%v, val:%v partition:%v\n", fo.IntVal(), tempPr[fo.IntVal()], partition)
		reduceMsg[partition] = append(reduceMsg[partition], &PRMessage{PRValue:messagePr[fo.IntVal()],ID:fo})
	}
*/
	//log.Printf("receive sum:%v\n", receiveSum)
	//log.Printf("send sum:%v\n", sendSum)
	return updated, reduceMsg, prVal, tempPr
}
